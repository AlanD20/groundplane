package environment

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testnetworkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
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

func (repository *fakeEnvironmentCreationRepository) PrepareEnvironmentCreation(
	_ context.Context,
	volumeRoot string,
	_ testkeyvalue.Versioned[testhierarchy.ProjectRecord],
	poolRegistry testkeyvalue.Versioned[testnetworkreservations.EnvironmentPoolRegistry],
	environment testhierarchy.EnvironmentRecord,
	components []testcomponents.Record,
	input environmentprovision.Input,
) (workqueue.Change, error) {
	repository.calls++
	repository.volumeRoot = volumeRoot
	repository.poolRegistry = poolRegistry
	repository.environment = environment
	repository.components = append([]testcomponents.Record(nil), components...)
	repository.input = input
	return workqueue.Change{}, nil
}

func TestEnvironmentCreationCapturesProvisioningFromAcceptedTask(t *testing.T) {
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
	service := &environmentCreationService{
		volumeRoot:      "/var/lib/groundplane/vol",
		environmentPool: netip.MustParsePrefix("10.0.0.0/8"),
		repository:      repository,
	}
	now := time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)
	input := CreateEnvironmentInput{
		ProjectID:   environmentCreationTestProjectID,
		Name:        "production",
		NetworkPool: "10.200.0.0/16",
	}
	reference := workexecution.Input{ID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV", SHA256: strings.Repeat("a", 64)}
	task := workexecution.Task{
		ID:         "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Actor:      workexecution.OperatorActor,
		Action:     workexecution.ActionCreate,
		TargetID:   "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		TargetName: input.Name,
		Scope: workexecution.Scope{
			TenantID:      project.Record.TenantID,
			ProjectID:     project.Record.ID,
			EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
		AcceptedIntent: &reference,
		CreatedAt:      now.Add(-time.Minute),
		UpdatedAt:      now.Add(-time.Minute),
		Deadline:       now.Add(time.Hour),
	}
	prepared, err := service.prepareAcceptedCreation(
		context.Background(),
		workqueue.TaskSnapshot{Task: task, Revision: 7},
		7,
		now,
		input,
	)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Task.ID != task.ID || !prepared.Task.CreatedAt.Equal(task.CreatedAt) || len(prepared.Graph) != 1 ||
		!prepared.Graph[0].CreatedAt.Equal(now) {
		t.Fatalf("compiler changed accepted identity or failed fresh capture: %+v", prepared)
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
}
