package environment

import (
	"context"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprovision"
	"net/http"
	"testing"
	"time"

	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testnetworkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const (
	environmentCreationTestTenantID  = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	environmentCreationTestProjectID = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

type fakeEnvironmentCreationRepository struct {
	project      testkeyvalue.Versioned[testhierarchy.ProjectRecord]
	poolRegistry testkeyvalue.Versioned[testnetworkreservations.EnvironmentPoolRegistry]
	volumeRoot   string
	environment  testhierarchy.EnvironmentRecord
	components   []testcomponents.Record
	input        environmentprovision.Input
	marker       testidempotency.IdempotencyMarker
	calls        int
}

func (*fakeEnvironmentCreationRepository) ResolveResourceHost(context.Context) (workexecution.Placement, error) {
	return workexecution.Placement{Executor: workexecution.Agent, HostID: "host_01ARZ3NDEKTSV4RRFFQ69G5FAV"}, nil
}

func (repository *fakeEnvironmentCreationRepository) GetEnvironmentPoolRegistry(
	context.Context,
) (testkeyvalue.Versioned[testnetworkreservations.EnvironmentPoolRegistry], error) {
	return repository.poolRegistry, nil
}

func (repository *fakeEnvironmentCreationRepository) GetProject(
	context.Context, string,

) (testkeyvalue.Versioned[testhierarchy.ProjectRecord], error) {
	return repository.project, nil
}

func (repository *fakeEnvironmentCreationRepository) CreateEnvironmentWithWork(
	_ context.Context,
	volumeRoot string,
	_ testkeyvalue.Versioned[testhierarchy.ProjectRecord],
	poolRegistry testkeyvalue.Versioned[testnetworkreservations.EnvironmentPoolRegistry],
	environment testhierarchy.EnvironmentRecord,
	components []testcomponents.Record,
	input environmentprovision.Input,
	marker testidempotency.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.calls++
	repository.volumeRoot = volumeRoot
	repository.poolRegistry = poolRegistry
	repository.environment = environment
	repository.components = append([]testcomponents.Record(nil), components...)
	repository.input = input
	repository.marker = marker
	repository.marker.Response.Body = append([]byte(nil), marker.Response.Body...)
	return etcd.IdempotencyTransactionResult{}, nil
}

type fakeEnvironmentCreationIdempotency struct {
	evidence   environmentCreationEvidence
	resolution idempotentintent.Resolution
	existing   bool
}

func (idempotency *fakeEnvironmentCreationIdempotency) Prepare(
	context.Context,
	CreateEnvironmentInput,
) (environmentCreationEvidence, error) {
	return idempotency.evidence, nil
}

func (idempotency *fakeEnvironmentCreationIdempotency) ResolveExisting(
	context.Context, testidempotency.IdempotencyLocator,

	environmentCreationEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotency.resolution, idempotency.existing, nil
}

func (idempotency *fakeEnvironmentCreationIdempotency) ResolveKnown(
	context.Context,
	environmentCreationEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func (idempotency *fakeEnvironmentCreationIdempotency) ResolveUnknown(
	context.Context, testidempotency.IdempotencyLocator,

	environmentCreationEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func TestEnvironmentCreationCapturesProvisioningAndProtectedResponse(t *testing.T) {
	project := testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
		Record: testhierarchy.ProjectRecord{
			ID: environmentCreationTestProjectID, TenantID: environmentCreationTestTenantID,
			Slug: "console", Name: "Console", Kind: testhierarchy.ProjectKindTenant,
		},
		Revision: 7, ReadRevision: 7,
	}
	repository := &fakeEnvironmentCreationRepository{
		project: project,
		poolRegistry: testkeyvalue.Versioned[testnetworkreservations.EnvironmentPoolRegistry]{
			Record: testnetworkreservations.EnvironmentPoolRegistry{Reservations: map[string]string{}},
		},
	}
	idempotency := &fakeEnvironmentCreationIdempotency{
		resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied},
	}
	service, err := NewCreationService(
		"/var/lib/groundplane/vol",
		"10.0.0.0/8",
		repository,
		idempotency,
	)
	if err != nil {
		t.Fatalf("NewCreationService() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	response, err := service.CreateEnvironment(context.Background(), CreateEnvironmentInput{
		ProjectID: environmentCreationTestProjectID, Name: "production", NetworkPool: "10.200.0.0/16",
	}, "environment-create-key-0001")
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	var accepted apiTypes.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatalf("CreateEnvironment() body = %s, %v", response.Body, err)
	}
	if response.Status != http.StatusAccepted || response.ContentKind != "application/json" ||
		accepted.TaskID == "" || accepted.TaskID != repository.input.TaskID {
		t.Fatalf("CreateEnvironment() response = %#v / %#v", response, accepted)
	}
	environment := repository.environment
	captured := repository.input
	if repository.calls != 1 || repository.volumeRoot != "/var/lib/groundplane/vol" ||
		environment.ProjectID != environmentCreationTestProjectID || environment.Name != "production" ||
		environment.NetworkPool != "10.200.0.0/16" ||
		environment.ProvisioningState != testhierarchy.EnvironmentProvisioningProvisioning ||
		environment.CreateTaskID != captured.TaskID || environment.CreatedAt != now || captured.CreatedAt != now ||
		captured.Validate() != nil || captured.Environment != environment ||
		captured.Scope.TenantID != environmentCreationTestTenantID ||
		captured.Scope.ProjectID != environmentCreationTestProjectID || captured.Scope.EnvironmentID != environment.ID ||
		captured.HostID != "host_01ARZ3NDEKTSV4RRFFQ69G5FAV" || captured.VolumeRoot != repository.volumeRoot {
		t.Fatalf("captured Environment provisioning = %#v / %#v", environment, captured)
	}
	if repository.poolRegistry.Record.Reservations[environment.ID] != environment.NetworkPool {
		t.Fatalf("Environment pool registry = %#v", repository.poolRegistry.Record)
	}
	if len(repository.components) != 2 {
		t.Fatalf("component count = %d, want 2", len(repository.components))
	}
	wantKinds := []core.ComponentKind{
		core.ComponentKindIngressCaddy,
		core.ComponentKindEdgeCloudflare,
	}
	for index, component := range repository.components {
		if component.Desired.Owner != core.ComponentOwnerEnvironment ||
			component.Desired.OwnerID != environment.ID || component.Desired.Kind != wantKinds[index] {
			t.Fatalf("component[%d] identity = %#v", index, component.Desired)
		}
		if component.Desired.Enabled || !component.Desired.Config.Empty() ||
			len(component.Runtime.GeneratedServices) != 0 || component.Runtime.PinnedIPv4 != "" ||
			component.Runtime.Healthy {
			t.Fatalf("component[%d] initial state = %#v", index, component)
		}
	}
	if repository.marker.TaskID != captured.TaskID || repository.marker.Response.Status != http.StatusAccepted ||
		repository.marker.Locator.ScopeKind != testidempotency.IdempotencyScopeProject ||
		repository.marker.Locator.ScopeID != environmentCreationTestProjectID {
		t.Fatalf("Task idempotency marker = %#v", repository.marker)
	}
}
