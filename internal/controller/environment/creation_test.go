package environment

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	environmentCreationTestTenantID  = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	environmentCreationTestProjectID = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

type fakeEnvironmentCreationRepository struct {
	project      etcd.Versioned[etcd.ProjectRecord]
	poolRegistry etcd.Versioned[etcd.EnvironmentPoolRegistry]
	volumeRoot   string
	environment  etcd.EnvironmentRecord
	components   []etcd.ComponentRecord
	task         etcd.TaskRecord
	marker       etcd.IdempotencyMarker
	calls        int
}

func (repository *fakeEnvironmentCreationRepository) GetEnvironmentPoolRegistry(
	context.Context,
) (etcd.Versioned[etcd.EnvironmentPoolRegistry], error) {
	return repository.poolRegistry, nil
}

func (repository *fakeEnvironmentCreationRepository) GetProject(
	context.Context,
	string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return repository.project, nil
}

func (repository *fakeEnvironmentCreationRepository) CreateEnvironmentWithTask(
	_ context.Context,
	volumeRoot string,
	_ etcd.Versioned[etcd.ProjectRecord],
	poolRegistry etcd.Versioned[etcd.EnvironmentPoolRegistry],
	environment etcd.EnvironmentRecord,
	components []etcd.ComponentRecord,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.calls++
	repository.volumeRoot = volumeRoot
	repository.poolRegistry = poolRegistry
	repository.environment = environment
	repository.components = append([]etcd.ComponentRecord(nil), components...)
	repository.task = task
	repository.task.Params = make(map[string]string, len(task.Params))
	for key, value := range task.Params {
		repository.task.Params[key] = value
	}
	repository.task.Steps = append([]etcd.TaskStepRecord(nil), task.Steps...)
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
	context.Context,
	etcd.IdempotencyLocator,
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
	context.Context,
	etcd.IdempotencyLocator,
	environmentCreationEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func TestEnvironmentCreationBuildsAtomicReplayableTask(t *testing.T) {
	project := etcd.Versioned[etcd.ProjectRecord]{
		Record: etcd.ProjectRecord{
			ID: environmentCreationTestProjectID, TenantID: environmentCreationTestTenantID,
			Slug: "console", Name: "Console", Kind: etcd.ProjectKindTenant,
		},
		Revision: 7, ReadRevision: 7,
	}
	repository := &fakeEnvironmentCreationRepository{
		project: project,
		poolRegistry: etcd.Versioned[etcd.EnvironmentPoolRegistry]{
			Record: etcd.EnvironmentPoolRegistry{Reservations: map[string]string{}},
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
		accepted.TaskID == "" || accepted.TaskID != repository.task.ID {
		t.Fatalf("CreateEnvironment() response = %#v / %#v", response, accepted)
	}
	environment := repository.environment
	task := repository.task
	if repository.calls != 1 || repository.volumeRoot != "/var/lib/groundplane/vol" ||
		environment.ProjectID != environmentCreationTestProjectID || environment.Name != "production" ||
		environment.NetworkPool != "10.200.0.0/16" ||
		environment.ProvisioningState != etcd.EnvironmentProvisioningProvisioning ||
		environment.CreateTaskID != task.ID || environment.CreatedAt != now || task.CreatedAt != now ||
		task.Executor != etcd.TaskExecutorAgent || task.Type != etcd.TaskCreate || task.Target != environment.ID ||
		task.Status != etcd.TaskStatusPending || task.NextEventSequence != 1 || len(task.Steps) != 1 {
		t.Fatalf("atomic Environment/Task = %#v / %#v", environment, task)
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
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: 1, PlanId: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
		Operation: agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE, TargetId: task.Target,
		Steps: []*agentpb.ExecutionStep{{
			StepId: task.Steps[0].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_EnvironmentDirectoryCreate{
				EnvironmentDirectoryCreate: &agentpb.EnvironmentDirectoryCreate{
					EnvironmentId:     task.Target,
					ExpectedVolumeDir: task.Params[taskcontract.EnvironmentCreateVolumeDirectoryParam],
				},
			},
		}},
	})
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	if err := executionplan.AuthorizeVolumeDirectories(plan, repository.volumeRoot); err != nil {
		t.Fatalf("AuthorizeVolumeDirectories() error = %v", err)
	}
	storedHash, err := hex.DecodeString(task.PlanHash)
	if err != nil || !bytes.Equal(storedHash, plan.PlanHash) {
		t.Fatalf("stored/resolved plan hash = %x / %x, %v", storedHash, plan.PlanHash, err)
	}
	if repository.marker.TaskID != task.ID || repository.marker.Response.Status != http.StatusAccepted ||
		repository.marker.Locator.ScopeKind != etcd.IdempotencyScopeProject ||
		repository.marker.Locator.ScopeID != environmentCreationTestProjectID {
		t.Fatalf("Task idempotency marker = %#v", repository.marker)
	}
}
