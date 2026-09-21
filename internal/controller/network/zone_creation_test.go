package network

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestZoneCreationServiceDerivesOwnershipAndPersistsExactResponse(t *testing.T) {
	// Rationale: callers choose the Environment and subnet, while the
	// Controller alone derives stable ownership and publishes the replay body.
	t.Parallel()
	at := time.Date(2026, time.August, 22, 14, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	projectID := ids.NewAt(ids.KindProject, at, 2)
	repository := &fakeZoneCreationRepository{
		environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{
				ID: environmentID, ProjectID: projectID, NetworkPool: "10.34.0.0/16",
				ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
			},
			Revision: 7, ReadRevision: 9,
		},
		project: testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
			Record:   testhierarchy.ProjectRecord{ID: projectID, Kind: testhierarchy.ProjectKindTenant},
			Revision: 8, ReadRevision: 9,
		},
		projection: zoneCreationProjectionForTest(t, environmentID, at),
	}
	idempotency := &fakeZoneCreationIdempotency{
		evidence:   zoneCreationEvidence{durable: networkTestProtectedIntent()},
		resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied},
	}
	service, err := newZoneCreationService(repository, idempotency)
	if err != nil {
		t.Fatalf("newZoneCreationService() error = %v", err)
	}
	now := time.Date(2026, time.August, 22, 15, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	input := apiTypes.ZoneCreate{
		EnvironmentID: environmentID, Name: "frontend.v2", Subnet: "10.34.20.0/24", Internal: true,
	}
	response, err := service.CreateZone(context.Background(), input, "zone-create-key-0001")
	if err != nil {
		t.Fatalf("CreateZone() error = %v", err)
	}
	var zone apiTypes.Zone
	if err := json.Unmarshal(response.Body, &zone); err != nil {
		t.Fatalf("CreateZone() body = %s, %v", response.Body, err)
	}
	if response.Status != http.StatusCreated || response.ContentKind != "application/json" ||
		zone.ID == "" || zone.EnvironmentID != environmentID || zone.Name != input.Name ||
		zone.Subnet != input.Subnet || !zone.Internal || zone.OwnerKind != apiTypes.ZoneOwnerEnvironment ||
		zone.OwnerID != environmentID {
		t.Fatalf("CreateZone() response/Zone = %#v/%#v", response, zone)
	}
	if repository.publication.Zone.Desired.ID != zone.ID ||
		repository.publication.Zone.Desired.OwnerID != environmentID ||
		repository.publication.Zone.Desired.Name != input.Name ||
		repository.calls != 1 {
		t.Fatalf("published Zone/calls = %#v/%d", repository.publication.Zone, repository.calls)
	}
	marker := repository.publication.Marker
	if marker.Locator.ScopeKind != testidempotency.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environmentID || marker.Locator.Method != http.MethodPost ||
		marker.Locator.Route != zoneCreationRoute || marker.Locator.Key != "zone-create-key-0001" ||
		marker.RetainUntil != now.Add(90*24*time.Hour) || !reflect.DeepEqual(marker.Response, response) {
		t.Fatalf("published marker = %#v", marker)
	}
	if repository.claim.SourceKind != testblueprints.EnvironmentBlueprintSourceMutation ||
		repository.claim.BaselineHeadRevision != repository.projection.Revision ||
		repository.stage.Mutation == nil || repository.stage.Mutation.Zone == nil ||
		repository.stage.Mutation.Zone.Action != testblueprints.EnvironmentZoneMutationCreate ||
		repository.stage.Mutation.Zone.ZoneID != zone.ID ||
		repository.stage.Mutation.Zone.Request == nil ||
		repository.stage.Mutation.Zone.Request.Name != input.Name {
		t.Fatalf("staged Zone create claim/audit = %#v / %#v", repository.claim, repository.stage.Mutation)
	}
	candidate := repository.publication.Projection
	if candidate.RevisionID != repository.claim.RevisionID || candidate.RenderGeneration != 2 ||
		len(candidate.DesiredZones) != 1 || candidate.DesiredZones[0].Desired.ID != zone.ID {
		t.Fatalf("published candidate = %#v", candidate)
	}
}

func TestZoneCreationServiceReplaysBeforeHierarchyReads(t *testing.T) {
	// Rationale: an exact retry remains replayable even if the hierarchy has
	// changed after the original synchronous mutation committed.
	t.Parallel()
	environmentID := ids.NewAt(ids.KindEnvironment, time.Date(2026, time.August, 22, 16, 0, 0, 0, time.UTC), 1)
	want := testidempotency.IdempotencyResponse{
		Status:      http.StatusCreated,
		ContentKind: "application/json",
		Body: []byte(
			`{"id":"net_01ARZ3NDEKTSV4RRFFQ69G5FAV","environment_id":"` + environmentID + `","name":"frontend","subnet":"10.34.20.0/24","internal":false,"owner_kind":"environment","owner_id":"` + environmentID + `"}`,
		),
	}
	repository := &fakeZoneCreationRepository{}
	service, err := newZoneCreationService(repository, &fakeZoneCreationIdempotency{
		evidence:   zoneCreationEvidence{durable: networkTestProtectedIntent()},
		existing:   true,
		resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionReplay, Response: want},
	})
	if err != nil {
		t.Fatalf("newZoneCreationService() error = %v", err)
	}
	got, err := service.CreateZone(context.Background(), apiTypes.ZoneCreate{
		EnvironmentID: environmentID, Name: "frontend", Subnet: "10.34.20.0/24",
	}, "zone-create-key-0002")
	if err != nil || !reflect.DeepEqual(got, want) || repository.reads != 0 || repository.calls != 0 {
		t.Fatalf("CreateZone(replay) = %#v, %v, reads/calls %d/%d", got, err, repository.reads, repository.calls)
	}
}

type fakeZoneCreationRepository struct {
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	project     testkeyvalue.Versioned[testhierarchy.ProjectRecord]
	projection  testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]
	claim       testblueprints.EnvironmentBlueprintStageClaim
	stage       testblueprints.EnvironmentBlueprintStageRequest
	publication etcd.EnvironmentZoneDesiredPublication
	result      etcd.IdempotencyTransactionResult
	reads       int
	calls       int
}

func zoneCreationProjectionForTest(
	t *testing.T,
	environmentID string,
	at time.Time,
) testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection] {
	t.Helper()
	serviceID := ids.NewAt(ids.KindService, at, 5)
	canonical := []byte("services:\n  api:\n    image: example.invalid/api:1\n")
	digest := sha256.Sum256(canonical)
	artifact, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId: ids.NewAt(ids.KindConfig, at, 3),
		OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:    environmentID, AuthorizedVolumeDir: "/var/lib/groundplane/volumes/" + environmentID,
		CanonicalYaml: canonical, YamlSha256: digest[:],
		Services: []*agentpb.ComposeService{{
			ServiceId: serviceID, ComposeName: "api",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
		Record: testenvironmentprojection.EnvironmentComposeProjection{
			EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, at, 4),
			RenderGeneration: 1, ComposeArtifact: artifact, NormalizedCompose: canonical,
			DesiredServices: []testservices.EnvironmentServiceProjection{
				{EnvironmentID: environmentID, Desired: core.Service{
					ID: serviceID, Name: "api", Image: "example.invalid/api:1",
					Strategy: core.StrategyRecreate, Replicas: 1,
				}},
			},
		},
		Revision: 9, ReadRevision: 9,
	}
}

func (repository *fakeZoneCreationRepository) GetEnvironment(
	context.Context, string,

) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	repository.reads++
	return repository.environment, nil
}

func (repository *fakeZoneCreationRepository) GetProject(
	context.Context, string,

) (testkeyvalue.Versioned[testhierarchy.ProjectRecord], error) {
	repository.reads++
	return repository.project, nil
}

func (repository *fakeZoneCreationRepository) GetEnvironmentComposeProjection(
	context.Context, string,

) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	repository.reads++
	return repository.projection, repository.projection.Record.EnvironmentID != "", nil
}

func (repository *fakeZoneCreationRepository) ClaimEnvironmentBlueprintStage(
	_ context.Context,
	request testblueprints.EnvironmentBlueprintStageClaimRequest,
) (testblueprints.EnvironmentBlueprintStageClaim, error) {
	repository.claim = testblueprints.EnvironmentBlueprintStageClaim{
		EnvironmentID: request.EnvironmentID, RevisionID: request.CandidateRevisionID,
		TaskID: request.CandidateTaskID, Locator: request.Locator, Intent: request.Intent,
		BaselineHeadRevision: request.BaselineHeadRevision, SourceKind: request.SourceKind,
		RenderGeneration: request.RenderGeneration, ProjectionSchema: request.ProjectionSchema,
		CreatedAt: request.CreatedAt,
	}
	return repository.claim, nil
}

func (repository *fakeZoneCreationRepository) StageEnvironmentBlueprintRevision(
	_ context.Context,
	request testblueprints.EnvironmentBlueprintStageRequest,
) (testblueprints.EnvironmentBlueprintSeal, error) {
	repository.stage = request
	return testblueprints.EnvironmentBlueprintSeal{}, nil
}

func (repository *fakeZoneCreationRepository) PublishEnvironmentZoneDesiredRevisionDirect(
	_ context.Context,
	publication etcd.EnvironmentZoneDesiredPublication,
) (etcd.IdempotencyTransactionResult, error) {
	repository.calls++
	repository.publication = publication
	repository.publication.Marker.Intent.Ciphertext = append([]byte(nil), publication.Marker.Intent.Ciphertext...)
	repository.publication.Marker.Response.Body = append([]byte(nil), publication.Marker.Response.Body...)
	return repository.result, nil
}

type fakeZoneCreationIdempotency struct {
	evidence   zoneCreationEvidence
	resolution idempotentintent.Resolution
	existing   bool
}

func (idempotency *fakeZoneCreationIdempotency) Prepare(
	context.Context,
	apiTypes.ZoneCreate,
) (zoneCreationEvidence, error) {
	return idempotency.evidence, nil
}

func (idempotency *fakeZoneCreationIdempotency) ResolveExisting(
	context.Context, testidempotency.IdempotencyLocator,

	zoneCreationEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotency.resolution, idempotency.existing, nil
}

func (idempotency *fakeZoneCreationIdempotency) ResolveKnown(
	context.Context,
	zoneCreationEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func (idempotency *fakeZoneCreationIdempotency) ResolveUnknown(
	context.Context, testidempotency.IdempotencyLocator,

	zoneCreationEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func (idempotency *fakeZoneCreationIdempotency) MatchesStaged(
	context.Context,
	zoneCreationEvidence, testidempotency.ProtectedIntentRecord,

) (bool, error) {
	return true, nil
}
