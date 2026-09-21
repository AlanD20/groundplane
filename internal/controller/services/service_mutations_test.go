package services

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
	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type fakeServiceMutationRepository struct {
	serviceMutationRepository
	environment         testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	project             testkeyvalue.Versioned[testhierarchy.ProjectRecord]
	tenant              testkeyvalue.Versioned[testhierarchy.TenantRecord]
	head                testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintHead]
	projection          testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]
	zones               testkeyvalue.Page[testzones.Record]
	record              testservices.ServiceRecord
	references          testservices.ServiceMutationReferences
	marker              testidempotency.IdempotencyMarker
	withoutDesiredState bool
}

func (fake *fakeServiceMutationRepository) GetTenant(
	context.Context,
	string,
) (testkeyvalue.Versioned[testhierarchy.TenantRecord], error) {
	return fake.tenant, nil
}

func (fake *fakeServiceMutationRepository) GetEnvironmentBlueprintHead(
	context.Context,
	string,
) (testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintHead], bool, error) {
	if fake.withoutDesiredState {
		return testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintHead]{}, false, nil
	}
	return fake.head, true, nil
}

func (fake *fakeServiceMutationRepository) GetEnvironmentComposeProjection(
	context.Context,
	string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	if fake.withoutDesiredState {
		return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{}, false, nil
	}
	return fake.projection, true, nil
}

func (fake *fakeServiceMutationRepository) GetEnvironment(
	context.Context,
	string,
) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	return fake.environment, nil
}

func (fake *fakeServiceMutationRepository) GetProject(
	context.Context,
	string,
) (testkeyvalue.Versioned[testhierarchy.ProjectRecord], error) {
	return fake.project, nil
}

func (fake *fakeServiceMutationRepository) GetService(
	context.Context,
	string,
) (testkeyvalue.Versioned[testservices.ServiceRecord], error) {
	return testkeyvalue.Versioned[testservices.ServiceRecord]{Record: fake.record, Revision: 10, ReadRevision: 10}, nil
}

func (fake *fakeServiceMutationRepository) ListZones(
	context.Context,
	string, testkeyvalue.PageRequest,

) (testkeyvalue.Page[testzones.Record], error) {
	return fake.zones, nil
}

func (fake *fakeServiceMutationRepository) ListServices(
	context.Context,
	string, testkeyvalue.PageRequest,

) (testkeyvalue.Page[testservices.ServiceRecord], error) {
	return testkeyvalue.Page[testservices.ServiceRecord]{}, nil
}

func (fake *fakeServiceMutationRepository) ClaimEnvironmentBlueprintStage(
	_ context.Context,
	request testblueprints.EnvironmentBlueprintStageClaimRequest,
) (testblueprints.EnvironmentBlueprintStageClaim, error) {
	return testblueprints.EnvironmentBlueprintStageClaim{
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
	request testblueprints.EnvironmentBlueprintStageRequest,
) (testblueprints.EnvironmentBlueprintSeal, error) {
	fake.projection.Record = request.Projection
	return testblueprints.EnvironmentBlueprintSeal{}, nil
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
	context.Context, testidempotency.IdempotencyReplayTarget,

	string,
	string,
	string,
) (testidempotency.IdempotencyLocator, bool, error) {
	return testidempotency.IdempotencyLocator{}, false, nil
}

func (fake *fakeServiceMutationIdempotency) MatchesStaged(
	context.Context,
	serviceMutationEvidence, testidempotency.ProtectedIntentRecord,

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
	context.Context, testidempotency.IdempotencyLocator,

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
	context.Context, testidempotency.IdempotencyLocator,

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
	canonical := []byte(
		"networks:\n  backend:\n    ipam:\n      config:\n        - subnet: 10.40.0.0/24\nservices: {}\n",
	)
	digest := sha256.Sum256(canonical)
	artifact, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: environmentID, ProjectName: "gp-" + strings.ToLower(environmentID),
		CanonicalYaml: canonical, YamlSha256: digest[:],
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/tenant/project/" + environmentID,
		Networks: []*agentpb.ComposeNetwork{{
			NetworkId: zoneID, ComposeName: "backend", DockerName: "gp_net_" + zoneID,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	repository := &fakeServiceMutationRepository{
		environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{
				ID:                environmentID,
				ProjectID:         projectID,
				ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
			},
			Revision:     7,
			ReadRevision: 7,
		},
		project: testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
			Record: testhierarchy.ProjectRecord{
				ID:       projectID,
				TenantID: tenantID,
				Kind:     testhierarchy.ProjectKindTenant,
			},
			Revision:     8,
			ReadRevision: 8,
		},
		tenant: testkeyvalue.Versioned[testhierarchy.TenantRecord]{
			Record: testhierarchy.TenantRecord{ID: tenantID}, Revision: 6, ReadRevision: 6,
		},
		head: testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintHead]{
			Record:   testblueprints.EnvironmentBlueprintHead{EnvironmentID: environmentID, RevisionID: revisionID},
			Revision: 11, ReadRevision: 11,
		},
		projection: testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
			Record: testenvironmentprojection.EnvironmentComposeProjection{
				EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: 1,
				ComposeArtifact: artifact, NormalizedCompose: canonical,
				DesiredZones: []testenvironmentprojection.EnvironmentZoneProjection{{
					EnvironmentID: environmentID,
					Desired: core.Zone{
						ID: zoneID, Name: "backend", Subnet: "10.40.0.0/24",
						OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
					},
				}},
			},
			Revision: 11, ReadRevision: 11,
		},
		zones: testkeyvalue.Page[testzones.Record]{
			Items: []testkeyvalue.Versioned[testzones.Record]{
				{
					Record: testzones.Record{
						EnvironmentID: environmentID,
						Desired: core.Zone{
							ID: zoneID, Name: "backend", Subnet: "10.40.0.0/24",
							OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
						},
					},
					Revision: 9, ReadRevision: 9,
				},
			},
		},
	}
	idempotency := &fakeServiceMutationIdempotency{
		evidence: serviceMutationEvidence{durable: serviceTestProtectedIntent()},
	}
	service, err := NewMutationService(repository, idempotency, nil)
	if err != nil {
		t.Fatalf("NewMutationService() error = %v", err)
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
		repository.references.Zones[0].Record.Desired.ID != zoneID ||
		len(repository.projection.Record.DesiredServices) != 1 ||
		repository.projection.Record.DesiredServices[0].Desired.ID != created.ID ||
		repository.projection.Record.DesiredServices[0].Desired.Name != input.Name {
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
		environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{
				ID: environmentID, ProjectID: projectID, VolumeDir: volumeDir,
				ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
			},
			Revision: 7, ReadRevision: 7,
		},
		project: testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
			Record: testhierarchy.ProjectRecord{
				ID:       projectID,
				TenantID: tenantID,
				Kind:     testhierarchy.ProjectKindTenant,
			},
			Revision: 8, ReadRevision: 8,
		},
		tenant: testkeyvalue.Versioned[testhierarchy.TenantRecord]{
			Record: testhierarchy.TenantRecord{ID: tenantID}, Revision: 6, ReadRevision: 6,
		},
		withoutDesiredState: true,
	}
	idempotency := &fakeServiceMutationIdempotency{
		evidence: serviceMutationEvidence{durable: serviceTestProtectedIntent()},
	}
	service, err := NewMutationService(repository, idempotency, nil)
	if err != nil {
		t.Fatalf("NewMutationService() error = %v", err)
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
		repository.projection.Record.RenderGeneration != 1 ||
		len(repository.projection.Record.DesiredServices) != 1 ||
		repository.projection.Record.DesiredServices[0].Desired.Name != "web" ||
		artifact.GetOwnerId() != environmentID || artifact.GetAuthorizedVolumeDir() != volumeDir {
		t.Fatalf(
			"bootstrap response/projection/artifact = %#v/%#v/%#v",
			response,
			repository.projection.Record,
			artifact,
		)
	}
}

func TestServiceMutationsRejectComponentGeneratedService(t *testing.T) {
	// Rationale: ordinary edit and remove actions must not acquire mutation
	// authority over a Service generated and managed by a Component.
	t.Parallel()
	at := time.Date(2026, time.August, 31, 9, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	serviceID := ids.NewAt(ids.KindService, at, 2)
	component, err := testcomponents.NewRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, at, 3), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindIngressCaddy,
		GeneratedServices: []string{serviceID},
	})
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	repository := &fakeServiceMutationRepository{
		record: testservices.ServiceRecord{
			EnvironmentID: environmentID,
			Desired: core.Service{
				ID: serviceID, Name: "caddy", Image: "caddy:2", Strategy: core.StrategyRecreate,
				OnFailure: core.OnFailureSwitchBack, Replicas: 1,
			},
			Runtime: core.ServiceRuntime{ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentRunning},
		},
		projection: testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
			Record: testenvironmentprojection.EnvironmentComposeProjection{
				EnvironmentID: environmentID, Components: []testcomponents.Record{component},
			},
			Revision: 10, ReadRevision: 10,
		},
	}
	service, err := NewMutationService(repository, &fakeServiceMutationIdempotency{}, nil)
	if err != nil {
		t.Fatalf("NewMutationService() error = %v", err)
	}
	if _, err := service.EditService(
		context.Background(), serviceID, apiTypes.ServiceEdit{}, "edit-generated-0001",
	); !isAppErrorKind(err, errs.KindResourceInUse) {
		t.Fatalf("EditService(generated) error = %v", err)
	}
	if _, err := service.RemoveService(
		context.Background(), serviceID, "remove-generated-0001",
	); !isAppErrorKind(err, errs.KindResourceInUse) {
		t.Fatalf("RemoveService(generated) error = %v", err)
	}
}

func isAppErrorKind(err error, kind errs.Kind) bool {
	actual, ok := errs.KindOf(err)
	return ok && actual == kind
}
