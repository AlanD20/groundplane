package etcd

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestEnvironmentCreationAtomicallyPublishesProvisioningRecordAndTask(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenant := TenantRecord{ID: hierarchyTestID(ids.KindTenant, 701), Slug: "acme", Name: "Acme"}
	if _, err := repository.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	projectRecord := ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 702), TenantID: tenant.ID,
		Slug: "console", Name: "Console", Kind: ProjectKindTenant,
	}
	if _, err := repository.CreateProject(ctx, projectRecord); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	project, err := repository.GetProject(ctx, projectRecord.ID)
	if err != nil {
		t.Fatalf("GetProject() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 16, 0, 0, 0, time.UTC)
	task := validTaskRecord(now)
	task.ID = ids.NewAt(ids.KindTask, now, 703)
	task.OperationID = ids.NewAt(ids.KindOperation, now, 704)
	task.Type = TaskCreate
	task.Target = ids.NewAt(ids.KindEnvironment, now, 705)
	task.IdempotencyKey = "environment-create-key-0001"
	task.Executor = TaskExecutorAgent
	record, err := NewProvisioningEnvironment(
		environmentpath.DefaultVolumeRoot,
		projectRecord,
		task.Target,
		"production",
		task.ID,
		now,
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment() error = %v", err)
	}
	marker := pendingTaskMarker(task)
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeProject, ScopeID: projectRecord.ID,
		Method: http.MethodPost, Route: "/environments", Key: task.IdempotencyKey,
	}
	result, err := repository.CreateEnvironmentWithTask(
		ctx, environmentpath.DefaultVolumeRoot, project, record, task, marker,
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("CreateEnvironmentWithTask() = %#v, %v", result, err)
	}
	storedEnvironment, err := repository.GetEnvironment(ctx, record.ID)
	if err != nil || storedEnvironment.Record.ProvisioningState != EnvironmentProvisioningProvisioning ||
		storedEnvironment.Record.CreateTaskID != task.ID {
		t.Fatalf("stored Environment = %#v, %v", storedEnvironment.Record, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	storedTask, err := tasks.GetTask(ctx, task.ID)
	if err != nil || storedTask.Record.Target != record.ID || storedTask.Record.Type != TaskCreate {
		t.Fatalf("stored Task = %#v, %v", storedTask.Record, err)
	}
	agentID := ids.NewAt(ids.KindAgent, now, 706)
	assignedAt := now.Add(time.Second)
	if _, found, err := tasks.ClaimNextTask(ctx, agentID, 1, assignedAt); err != nil || !found {
		t.Fatalf("ClaimNextTask() found/error = %t/%v", found, err)
	}
	resultRecord := TaskResultRecord{
		Kind: TaskResultEnvironmentDirectory, Diagnostic: TaskResultDiagnosticNone,
	}
	terminalAt := assignedAt.Add(time.Second)
	if _, err := tasks.AcknowledgeTask(
		ctx, agentID, 1, task.ID, TaskStatusCompleted, resultRecord, terminalAt,
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("generic AcknowledgeTask(Environment create) error = %v", err)
	}
	terminal, err := tasks.AcknowledgeEnvironmentCreation(
		ctx, agentID, 1, task.ID, record.ID, TaskStatusCompleted, resultRecord, terminalAt,
	)
	if err != nil || terminal.Record.Status != TaskStatusCompleted {
		t.Fatalf("AcknowledgeEnvironmentCreation() = %#v, %v", terminal.Record, err)
	}
	ready, err := repository.GetEnvironment(ctx, record.ID)
	if err != nil || ready.Record.ProvisioningState != EnvironmentProvisioningReady {
		t.Fatalf("ready Environment = %#v, %v", ready.Record, err)
	}
}
