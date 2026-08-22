package app

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	controllerpkg "github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/hierarchy"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const (
	environmentCreationTestTenantID  = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	environmentCreationTestProjectID = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

type fakeEnvironmentCreationRepository struct {
	project     etcd.Versioned[etcd.ProjectRecord]
	volumeRoot  string
	environment etcd.EnvironmentRecord
	task        etcd.TaskRecord
	marker      etcd.IdempotencyMarker
	calls       int
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
	environment etcd.EnvironmentRecord,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.calls++
	repository.volumeRoot = volumeRoot
	repository.environment = environment
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
	hierarchy.CreateEnvironmentInput,
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
	repository := &fakeEnvironmentCreationRepository{project: project}
	idempotency := &fakeEnvironmentCreationIdempotency{
		resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied},
	}
	service, err := newEnvironmentCreationService(
		"/var/lib/groundplane/vol",
		repository,
		idempotency,
	)
	if err != nil {
		t.Fatalf("newEnvironmentCreationService() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	response, err := service.CreateEnvironment(context.Background(), hierarchy.CreateEnvironmentInput{
		ProjectID: environmentCreationTestProjectID, Name: "production",
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
		environment.ProvisioningState != etcd.EnvironmentProvisioningProvisioning ||
		environment.CreateTaskID != task.ID || environment.CreatedAt != now || task.CreatedAt != now ||
		task.Executor != etcd.TaskExecutorAgent || task.Type != etcd.TaskCreate || task.Target != environment.ID ||
		task.Status != etcd.TaskStatusPending || task.NextEventSequence != 1 || len(task.Steps) != 1 {
		t.Fatalf("atomic Environment/Task = %#v / %#v", environment, task)
	}
	resolver, err := controllerpkg.NewTaskPlanResolver(repository.volumeRoot)
	if err != nil {
		t.Fatalf("NewTaskPlanResolver() error = %v", err)
	}
	plan, err := resolver.ResolveExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
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
