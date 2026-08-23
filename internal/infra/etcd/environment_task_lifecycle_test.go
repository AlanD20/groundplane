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
	retryResult, err := tasks.RetryTask(ctx, source.ID, retryID, retryMarker)
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
