package etcd

import (
	"context"
	"net/http"
	"net/netip"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
)

func TestEnvironmentCreationRetryAtomicallyTransfersProvisioningOwnership(t *testing.T) {
	// Rationale: a retry Task must own the failed Environment in the same
	// transaction that publishes the Task, or its Agent result cannot be
	// acknowledged and the Environment remains permanently failed.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenant := TenantRecord{ID: hierarchyTestID(ids.KindTenant, 711), Slug: "retry", Name: "Retry"}
	if _, err := hierarchy.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	projectRecord := ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 712), TenantID: tenant.ID,
		Slug: "console", Name: "Console", Kind: ProjectKindTenant,
	}
	if _, err := hierarchy.CreateProject(ctx, projectRecord); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	project, err := hierarchy.GetProject(ctx, projectRecord.ID)
	if err != nil {
		t.Fatalf("GetProject() error = %v", err)
	}

	now := time.Date(2026, 8, 22, 17, 0, 0, 0, time.UTC)
	source := validTaskRecord(now)
	source.ID = ids.NewAt(ids.KindTask, now, 713)
	source.OperationID = ids.NewAt(ids.KindOperation, now, 714)
	source.Type = TaskCreate
	source.Target = ids.NewAt(ids.KindEnvironment, now, 715)
	source.IdempotencyKey = "environment-retry-source-key-0001"
	source.Executor = TaskExecutorAgent
	environment, err := NewProvisioningEnvironment(
		environmentpath.DefaultVolumeRoot,
		projectRecord,
		source.Target,
		"production",
		"10.200.0.0/16",
		source.ID,
		now,
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment() error = %v", err)
	}
	source.Owner = mustEnvironmentTaskOwner(t, project.Record, environment)
	marker := pendingTaskMarker(source)
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeProject, ScopeID: projectRecord.ID,
		Method: http.MethodPost, Route: "/environments", Key: source.IdempotencyKey,
	}
	poolRegistry, err := hierarchy.GetEnvironmentPoolRegistry(ctx)
	if err != nil {
		t.Fatalf("GetEnvironmentPoolRegistry() error = %v", err)
	}
	nextRegistry, _, err := poolRegistry.Record.Reserve(
		netip.MustParsePrefix("10.0.0.0/8"),
		environment.ID,
		environment.NetworkPool,
	)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	poolRegistry.Record = nextRegistry
	creation, err := hierarchy.CreateEnvironmentWithTask(
		ctx,
		environmentpath.DefaultVolumeRoot,
		project,
		poolRegistry,
		environment,
		environmentCreationTestComponents(t, environment.ID, now),
		source,
		marker,
	)
	if err != nil || creation.kind != idempotencyTransactionApplied {
		t.Fatalf("CreateEnvironmentWithTask() = %#v, %v", creation, err)
	}

	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	agentID := ids.NewAt(ids.KindAgent, now, 716)
	assignedAt := now.Add(time.Second)
	claim, found, err := tasks.ClaimNextTask(ctx, agentID, 1, assignedAt)
	if err != nil || !found || claim.Task.Record.ID != source.ID {
		t.Fatalf("ClaimNextTask(source) = %#v, %t, %v", claim, found, err)
	}
	failedResult := TaskResultRecord{
		Kind: TaskResultEnvironmentDirectory, Diagnostic: TaskResultDiagnosticNone, ExitCode: 1,
	}
	terminalAt := assignedAt.Add(time.Second)
	failed, err := tasks.AcknowledgeEnvironmentCreation(
		ctx,
		agentID,
		1,
		source.ID,
		environment.ID,
		TaskStatusFailed,
		failedResult,
		terminalAt,
	)
	if err != nil || failed.Record.Status != TaskStatusFailed {
		t.Fatalf("AcknowledgeEnvironmentCreation(failed) = %#v, %v", failed.Record, err)
	}

	retryAt := terminalAt.Add(time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 717)
	retryMarker := pendingRetryMarker(
		failed.Record,
		retryID,
		retryAt,
		"environment-retry-request-key-0001",
	)
	retryResult, err := tasks.RetryTask(ctx, source.ID, retryID, TaskActorOperator, retryMarker)
	if err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := retryResult.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("RetryTask() outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	retryTask, err := tasks.GetTask(ctx, retryID)
	if err != nil {
		t.Fatalf("GetTask(retry) error = %v", err)
	}
	retrying, err := hierarchy.GetEnvironment(ctx, environment.ID)
	if err != nil {
		t.Fatalf("GetEnvironment(retrying) error = %v", err)
	}
	if retrying.Record.ProvisioningState != EnvironmentProvisioningProvisioning ||
		retrying.Record.CreateTaskID != retryID || retrying.Revision != retryTask.Revision {
		t.Fatalf("retrying Environment/Task = %#v/%#v", retrying, retryTask)
	}
	if retrying.Record.ID != environment.ID || retrying.Record.ProjectID != environment.ProjectID ||
		retrying.Record.NetworkPool != environment.NetworkPool || retrying.Record.VolumeDir != environment.VolumeDir {
		t.Fatalf("retry changed stable Environment allocation: %#v", retrying.Record)
	}

	retryAssignedAt := retryAt.Add(time.Second)
	retryClaim, found, err := tasks.ClaimNextTask(ctx, agentID, 1, retryAssignedAt)
	if err != nil || !found || retryClaim.Task.Record.ID != retryID {
		t.Fatalf("ClaimNextTask(retry) = %#v, %t, %v", retryClaim, found, err)
	}
	completedResult := TaskResultRecord{
		Kind: TaskResultEnvironmentDirectory, Diagnostic: TaskResultDiagnosticNone,
	}
	completed, err := tasks.AcknowledgeEnvironmentCreation(
		ctx,
		agentID,
		1,
		retryID,
		environment.ID,
		TaskStatusCompleted,
		completedResult,
		retryAssignedAt.Add(time.Second),
	)
	if err != nil || completed.Record.Status != TaskStatusCompleted {
		t.Fatalf("AcknowledgeEnvironmentCreation(retry) = %#v, %v", completed.Record, err)
	}
	ready, err := hierarchy.GetEnvironment(ctx, environment.ID)
	if err != nil || ready.Record.ProvisioningState != EnvironmentProvisioningReady ||
		ready.Record.CreateTaskID != retryID || ready.Revision != completed.Revision {
		t.Fatalf("ready Environment/completed Task = %#v/%#v, %v", ready, completed, err)
	}
}

// Rationale: aborting before assignment must terminalize the Task and its
// owned Environment together so the failed provisioning remains retryable.
func TestEnvironmentCreationPendingAbortAtomicallyFailsProvisioning(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenant := TenantRecord{ID: hierarchyTestID(ids.KindTenant, 718), Slug: "abort", Name: "Abort"}
	if _, err := hierarchy.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	projectRecord := ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 719), TenantID: tenant.ID,
		Slug: "console", Name: "Console", Kind: ProjectKindTenant,
	}
	if _, err := hierarchy.CreateProject(ctx, projectRecord); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	project, err := hierarchy.GetProject(ctx, projectRecord.ID)
	if err != nil {
		t.Fatalf("GetProject() error = %v", err)
	}

	now := time.Date(2026, 8, 22, 18, 0, 0, 0, time.UTC)
	task := validTaskRecord(now)
	task.ID = ids.NewAt(ids.KindTask, now, 720)
	task.OperationID = ids.NewAt(ids.KindOperation, now, 721)
	task.Type = TaskCreate
	task.Target = ids.NewAt(ids.KindEnvironment, now, 722)
	task.IdempotencyKey = "environment-abort-source-key-0001"
	task.Executor = TaskExecutorAgent
	environment, err := NewProvisioningEnvironment(
		environmentpath.DefaultVolumeRoot,
		projectRecord,
		task.Target,
		"production",
		"10.30.0.0/16",
		task.ID,
		now,
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment() error = %v", err)
	}
	task.Owner = mustEnvironmentTaskOwner(t, project.Record, environment)
	marker := pendingTaskMarker(task)
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeProject, ScopeID: projectRecord.ID,
		Method: http.MethodPost, Route: "/environments", Key: task.IdempotencyKey,
	}
	poolRegistry, err := hierarchy.GetEnvironmentPoolRegistry(ctx)
	if err != nil {
		t.Fatalf("GetEnvironmentPoolRegistry() error = %v", err)
	}
	nextRegistry, _, err := poolRegistry.Record.Reserve(
		netip.MustParsePrefix("10.0.0.0/8"), environment.ID, environment.NetworkPool,
	)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	poolRegistry.Record = nextRegistry
	creation, err := hierarchy.CreateEnvironmentWithTask(
		ctx,
		environmentpath.DefaultVolumeRoot,
		project,
		poolRegistry,
		environment,
		environmentCreationTestComponents(t, environment.ID, now),
		task,
		marker,
	)
	if err != nil || creation.kind != idempotencyTransactionApplied {
		t.Fatalf("CreateEnvironmentWithTask() = %#v, %v", creation, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}

	terminalAt := now.Add(time.Second)
	aborted, err := tasks.AbortPendingTask(ctx, task.ID, terminalAt)
	if err != nil || aborted.Record.Status != TaskStatusAborted {
		t.Fatalf("AbortPendingTask() = %#v, %v", aborted.Record, err)
	}
	failed, err := hierarchy.GetEnvironment(ctx, environment.ID)
	if err != nil || failed.Record.ProvisioningState != EnvironmentProvisioningFailed ||
		failed.Record.CreateTaskID != task.ID || failed.Revision != aborted.Revision {
		t.Fatalf("failed Environment/aborted Task = %#v/%#v, %v", failed, aborted, err)
	}
	replay, err := tasks.AbortPendingTask(ctx, task.ID, terminalAt.Add(time.Second))
	if err != nil || replay.Revision != aborted.Revision {
		t.Fatalf("AbortPendingTask(replay) = %#v, %v", replay, err)
	}

	retryAt := terminalAt.Add(2 * time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 723)
	retryMarker := pendingRetryMarker(aborted.Record, retryID, retryAt, "environment-abort-retry-key-0001")
	if _, err := tasks.RetryTask(ctx, task.ID, retryID, TaskActorOperator, retryMarker); err != nil {
		t.Fatalf("RetryTask(aborted Environment) error = %v", err)
	}
	retrying, err := hierarchy.GetEnvironment(ctx, environment.ID)
	if err != nil || retrying.Record.ProvisioningState != EnvironmentProvisioningProvisioning ||
		retrying.Record.CreateTaskID != retryID {
		t.Fatalf("retrying Environment = %#v, %v", retrying, err)
	}
}
