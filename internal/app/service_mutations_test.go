package app

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type fakeServiceMutationRepository struct {
	serviceMutationRepository
	environment         etcd.Versioned[etcd.EnvironmentRecord]
	project             etcd.Versioned[etcd.ProjectRecord]
	tenant              etcd.Versioned[etcd.TenantRecord]
	head                etcd.Versioned[etcd.EnvironmentBlueprintHead]
	projection          etcd.Versioned[etcd.EnvironmentComposeProjection]
	zones               etcd.Page[etcd.ZoneRecord]
	record              etcd.ServiceRecord
	references          etcd.ServiceMutationReferences
	marker              etcd.IdempotencyMarker
	withoutDesiredState bool
}

func (fake *fakeServiceMutationRepository) GetTenant(
	context.Context,
	string,
) (etcd.Versioned[etcd.TenantRecord], error) {
	return fake.tenant, nil
}

func (fake *fakeServiceMutationRepository) GetEnvironmentBlueprintHead(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentBlueprintHead], bool, error) {
	if fake.withoutDesiredState {
		return etcd.Versioned[etcd.EnvironmentBlueprintHead]{}, false, nil
	}
	return fake.head, true, nil
}

func (fake *fakeServiceMutationRepository) GetEnvironmentComposeProjection(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	if fake.withoutDesiredState {
		return etcd.Versioned[etcd.EnvironmentComposeProjection]{}, false, nil
	}
	return fake.projection, true, nil
}

func (fake *fakeServiceMutationRepository) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return fake.environment, nil
}

func (fake *fakeServiceMutationRepository) GetProject(
	context.Context,
	string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return fake.project, nil
}

func (fake *fakeServiceMutationRepository) ListZones(
	context.Context,
	string,
	etcd.PageRequest,
) (etcd.Page[etcd.ZoneRecord], error) {
	return fake.zones, nil
}

func (fake *fakeServiceMutationRepository) ListServices(
	context.Context,
	string,
	etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	return etcd.Page[etcd.ServiceRecord]{}, nil
}

func (fake *fakeServiceMutationRepository) ClaimEnvironmentBlueprintStage(
	_ context.Context,
	request etcd.EnvironmentBlueprintStageClaimRequest,
) (etcd.EnvironmentBlueprintStageClaim, error) {
	return etcd.EnvironmentBlueprintStageClaim{
		DescriptorID:  strings.TrimPrefix(request.CandidateRevisionID, "task_"),
		EnvironmentID: request.EnvironmentID, RevisionID: request.CandidateRevisionID,
		TaskID: request.CandidateTaskID, Locator: request.Locator, Intent: request.Intent,
		BaselineHeadRevision: request.BaselineHeadRevision, SourceKind: request.SourceKind,
		RenderGeneration: request.RenderGeneration, ProjectionSchema: request.ProjectionSchema,
		CreatedAt: request.CreatedAt,
	}, nil
}

func (fake *fakeServiceMutationRepository) StageEnvironmentBlueprintRevision(
	_ context.Context,
	request etcd.EnvironmentBlueprintStageRequest,
) (etcd.EnvironmentBlueprintSeal, error) {
	fake.projection.Record = request.Projection
	return etcd.EnvironmentBlueprintSeal{}, nil
}

func (fake *fakeServiceMutationRepository) PublishEnvironmentServiceDesiredRevisionDirect(
	_ context.Context,
	input etcd.EnvironmentServiceDesiredPublication,
) (etcd.IdempotencyTransactionResult, error) {
	fake.record = input.Change.Record
	fake.references = input.References
	fake.marker = input.Marker
	fake.marker.Intent.Ciphertext = append([]byte(nil), input.Marker.Intent.Ciphertext...)
	fake.marker.Response.Body = append([]byte(nil), input.Marker.Response.Body...)
	return etcd.IdempotencyTransactionResult{}, nil
}

type fakeServiceMutationIdempotency struct{ evidence serviceMutationEvidence }

func (fake *fakeServiceMutationIdempotency) ResolveReplayLocator(
	context.Context,
	etcd.IdempotencyReplayTarget,
	string,
	string,
	string,
) (etcd.IdempotencyLocator, bool, error) {
	return etcd.IdempotencyLocator{}, false, nil
}

func (fake *fakeServiceMutationIdempotency) MatchesStaged(
	context.Context,
	serviceMutationEvidence,
	etcd.ProtectedIntentRecord,
) (bool, error) {
	return true, nil
}

func (fake *fakeServiceMutationIdempotency) Prepare(
	context.Context,
	serviceMutationIntent,
) (serviceMutationEvidence, error) {
	return fake.evidence, nil
}

func (fake *fakeServiceMutationIdempotency) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	serviceMutationEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotentintent.Resolution{}, false, nil
}

func (fake *fakeServiceMutationIdempotency) ResolveKnown(
	context.Context,
	serviceMutationEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied}, nil
}

func (fake *fakeServiceMutationIdempotency) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	serviceMutationEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{}, nil
}

func TestServiceCreationCommitsExactResponseAndZoneFence(t *testing.T) {
	// Rationale: a direct Service create must allocate stable identity, start at
	// running intent, and atomically bind its exact response to every live Zone.
	t.Parallel()
	at := time.Date(2026, time.August, 23, 1, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	projectID := ids.NewAt(ids.KindProject, at, 2)
	zoneID := ids.NewAt(ids.KindNetwork, at, 3)
	tenantID := ids.NewAt(ids.KindTenant, at, 4)
	revisionID := ids.NewAt(ids.KindTask, at, 5)
	artifactID := ids.NewAt(ids.KindConfig, at, 6)
	canonical := []byte("networks: {}\nservices: {}\n")
	digest := sha256.Sum256(canonical)
	artifact, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: environmentID, ProjectName: "gp-" + strings.ToLower(environmentID),
		CanonicalYaml: canonical, YamlSha256: digest[:],
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/tenant/project/" + environmentID,
	})
	if err != nil {
		t.Fatal(err)
	}
	repository := &fakeServiceMutationRepository{
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record: etcd.EnvironmentRecord{
				ID:                environmentID,
				ProjectID:         projectID,
				ProvisioningState: etcd.EnvironmentProvisioningReady,
			},
			Revision:     7,
			ReadRevision: 7,
		},
		project: etcd.Versioned[etcd.ProjectRecord]{
			Record:       etcd.ProjectRecord{ID: projectID, TenantID: tenantID, Kind: etcd.ProjectKindTenant},
			Revision:     8,
			ReadRevision: 8,
		},
		tenant: etcd.Versioned[etcd.TenantRecord]{
			Record: etcd.TenantRecord{ID: tenantID}, Revision: 6, ReadRevision: 6,
		},
		head: etcd.Versioned[etcd.EnvironmentBlueprintHead]{
			Record:   etcd.EnvironmentBlueprintHead{EnvironmentID: environmentID, RevisionID: revisionID},
			Revision: 11, ReadRevision: 11,
		},
		projection: etcd.Versioned[etcd.EnvironmentComposeProjection]{
			Record: etcd.EnvironmentComposeProjection{
				EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: 1,
				ComposeArtifact: artifact,
			},
			Revision: 11, ReadRevision: 11,
		},
		zones: etcd.Page[etcd.ZoneRecord]{
			Items: []etcd.Versioned[etcd.ZoneRecord]{
				{Record: etcd.ZoneRecord{EnvironmentID: environmentID}, Revision: 9, ReadRevision: 9},
			},
		},
	}
	repository.zones.Items[0].Record.Desired.ID = zoneID
	repository.zones.Items[0].Record.Desired.Name = "backend"
	repository.zones.Items[0].Record.Desired.Subnet = "10.40.0.0/24"
	idempotency := &fakeServiceMutationIdempotency{
		evidence: serviceMutationEvidence{durable: projectCreationTestEvidence().durable},
	}
	service, err := newServiceMutationService(repository, idempotency)
	if err != nil {
		t.Fatalf("newServiceMutationService() error = %v", err)
	}
	now := at.Add(time.Hour)
	service.now = func() time.Time { return now }
	input := apiTypes.ServiceCreate{
		EnvironmentID: environmentID,
		Name:          "api",
		Image:         "app:stable",
		Zones:         []string{"backend"},
		Strategy:      "recreate",
		OnFailure:     apiTypes.OnFailureSwitchBack,
		Resources:     apiTypes.ServiceResources{Mem: "512m", CPUs: 0.5},
		Expose:        []string{"8080"},
		Restart:       "unless-stopped",
		Replicas:      1,
	}
	response, err := service.CreateService(context.Background(), input, "service-create-key-0001")
	if err != nil {
		t.Fatalf("CreateService() error = %v", err)
	}
	var created apiTypes.Service
	if err := json.Unmarshal(response.Body, &created); err != nil {
		t.Fatalf("CreateService() body = %s, %v", response.Body, err)
	}
	if response.Status != http.StatusCreated || created.ID == "" || created.EnvironmentID != environmentID ||
		created.RuntimeIntent != apiTypes.ServiceRuntimeIntentRunning ||
		created.Name != input.Name ||
		len(repository.references.Zones) != 1 ||
		repository.references.Zones[0].Record.Desired.ID != zoneID {
		t.Fatalf("created Service/references = %#v/%#v", created, repository.references)
	}
	if repository.record.Desired.ID != created.ID || repository.marker.Locator.Route != serviceCreationRoute ||
		repository.marker.Locator.Key != "service-create-key-0001" ||
		repository.marker.RetainUntil != now.Add(90*24*time.Hour) ||
		!reflect.DeepEqual(repository.marker.Response, response) {
		t.Fatalf("persisted Service/marker = %#v/%#v", repository.record, repository.marker)
	}
}

func TestServiceCreationBootstrapsMissingEnvironmentDesiredState(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.August, 29, 8, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	projectID := ids.NewAt(ids.KindProject, at, 2)
	tenantID := ids.NewAt(ids.KindTenant, at, 3)
	volumeDir := "/var/lib/groundplane/vol/tenant/project/" + environmentID
	repository := &fakeServiceMutationRepository{
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record: etcd.EnvironmentRecord{
				ID: environmentID, ProjectID: projectID, VolumeDir: volumeDir,
				ProvisioningState: etcd.EnvironmentProvisioningReady,
			},
			Revision: 7, ReadRevision: 7,
		},
		project: etcd.Versioned[etcd.ProjectRecord]{
			Record:   etcd.ProjectRecord{ID: projectID, TenantID: tenantID, Kind: etcd.ProjectKindTenant},
			Revision: 8, ReadRevision: 8,
		},
		tenant: etcd.Versioned[etcd.TenantRecord]{
			Record: etcd.TenantRecord{ID: tenantID}, Revision: 6, ReadRevision: 6,
		},
		withoutDesiredState: true,
	}
	idempotency := &fakeServiceMutationIdempotency{
		evidence: serviceMutationEvidence{durable: projectCreationTestEvidence().durable},
	}
	service, err := newServiceMutationService(repository, idempotency)
	if err != nil {
		t.Fatalf("newServiceMutationService() error = %v", err)
	}
	service.now = func() time.Time { return at.Add(time.Hour) }
	response, err := service.CreateService(context.Background(), apiTypes.ServiceCreate{
		EnvironmentID: environmentID, Name: "web", Image: "nginx:1.27-alpine",
		Strategy: string(core.StrategyRecreate), OnFailure: apiTypes.OnFailureSwitchBack,
		Restart: "unless-stopped", Replicas: 1,
	}, "service-create-bootstrap-key-0001")
	if err != nil {
		t.Fatalf("CreateService() error = %v", err)
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(repository.projection.Record.ComposeArtifact, artifact); err != nil {
		t.Fatalf("bootstrap Compose artifact error = %v", err)
	}
	if response.Status != http.StatusCreated || repository.projection.Record.EnvironmentID != environmentID ||
		repository.projection.Record.RenderGeneration != 1 || len(repository.projection.Record.Services) != 1 ||
		artifact.GetOwnerId() != environmentID || artifact.GetAuthorizedVolumeDir() != volumeDir {
		t.Fatalf("bootstrap response/projection/artifact = %#v/%#v/%#v", response, repository.projection.Record, artifact)
	}
}
