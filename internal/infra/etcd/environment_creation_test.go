package etcd

import (
	"context"
	"net/http"
	"net/netip"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
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
		"10.200.0.0/16",
		task.ID,
		now,
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment() error = %v", err)
	}
	task.Owner = mustEnvironmentTaskOwner(t, project.Record, record)
	components := environmentCreationTestComponents(t, record.ID, now)
	marker := pendingTaskMarker(task)
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeProject, ScopeID: projectRecord.ID,
		Method: http.MethodPost, Route: "/environments", Key: task.IdempotencyKey,
	}
	poolRegistry, err := repository.GetEnvironmentPoolRegistry(ctx)
	if err != nil {
		t.Fatalf("GetEnvironmentPoolRegistry() error = %v", err)
	}
	nextRegistry, _, err := poolRegistry.Record.Reserve(
		netip.MustParsePrefix("10.0.0.0/8"),
		record.ID,
		record.NetworkPool,
	)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	poolRegistry.Record = nextRegistry
	result, err := repository.CreateEnvironmentWithTask(
		ctx, environmentpath.DefaultVolumeRoot, project, poolRegistry, record, components, task, marker,
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("CreateEnvironmentWithTask() = %#v, %v", result, err)
	}
	storedRegistry, err := repository.GetEnvironmentPoolRegistry(ctx)
	if err != nil || storedRegistry.Record.Reservations[record.ID] != record.NetworkPool {
		t.Fatalf("stored Environment pool registry = %#v, %v", storedRegistry.Record, err)
	}
	storedEnvironment, err := repository.GetEnvironment(ctx, record.ID)
	if err != nil || storedEnvironment.Record.ProvisioningState != EnvironmentProvisioningProvisioning ||
		storedEnvironment.Record.CreateTaskID != task.ID {
		t.Fatalf("stored Environment = %#v, %v", storedEnvironment.Record, err)
	}
	assertHierarchyCoordinationRecord(
		t,
		store,
		HierarchyDeletionTargetEnvironment,
		record.ID,
		storedEnvironment.Revision,
	)
	storedEpoch, err := store.Get(ctx, environmentMutationEpochKey(record.ID))
	if err != nil || storedEpoch.Entry == nil || storedEpoch.Entry.ModRevision != storedEnvironment.Revision {
		t.Fatalf("stored Environment mutation epoch = %#v, %v", storedEpoch, err)
	}
	epoch, err := decodeEnvironmentMutationEpochRecord(storedEpoch.Entry.Value)
	if err != nil || epoch.EnvironmentID != record.ID {
		t.Fatalf("decoded Environment mutation epoch = %#v, %v", epoch, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	storedTask, err := tasks.GetTask(ctx, task.ID)
	if err != nil || storedTask.Record.Target != record.ID || storedTask.Record.Type != TaskCreate {
		t.Fatalf("stored Task = %#v, %v", storedTask.Record, err)
	}
	componentRepository, err := newComponentRepository(store)
	if err != nil {
		t.Fatalf("newComponentRepository() error = %v", err)
	}
	storedComponents, err := componentRepository.ListEnvironmentComponents(
		ctx,
		record.ID,
		PageRequest{Limit: 20},
	)
	if err != nil || len(storedComponents.Items) != 2 {
		t.Fatalf("ListEnvironmentComponents() = %#v, %v", storedComponents, err)
	}
	for _, component := range storedComponents.Items {
		if component.Revision != storedEnvironment.Revision || component.Revision != storedTask.Revision {
			t.Fatalf(
				"Component/Environment/Task revisions = %d/%d/%d, want one transaction",
				component.Revision,
				storedEnvironment.Revision,
				storedTask.Revision,
			)
		}
		if component.Record.Desired.OwnerID != record.ID || component.Record.Desired.Enabled {
			t.Fatalf("stored initial Component = %#v", component.Record)
		}
	}
	agentID := ids.NewAt(ids.KindAgent, now, 708)
	assignedAt := now.Add(time.Second)
	if _, found, err := tasks.ClaimNextTask(ctx, agentID, 1, assignedAt); err != nil || !found {
		t.Fatalf("ClaimNextTask() found/error = %t/%v", found, err)
	}
	resultRecord := TaskResultRecord{
		Kind: TaskResultEnvironmentDirectory, Diagnostic: TaskResultDiagnosticNone,
	}
	terminalAt := assignedAt.Add(time.Second)
	if _, err := tasks.AcknowledgeTask(
		ctx, agentID, 1, task.ID, taskAssignmentIDForTest(t, tasks,
			task.ID),
		TaskStatusCompleted, resultRecord, terminalAt); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("generic AcknowledgeTask(Environment create) error = %v", err)
	}
	terminal, err := tasks.AcknowledgeEnvironmentCreation(
		ctx, agentID, 1, task.ID, taskAssignmentIDForTest(t, tasks,
			task.ID),
		record.ID, TaskStatusCompleted, resultRecord, terminalAt)

	if err != nil || terminal.Record.Status != TaskStatusCompleted {
		t.Fatalf("AcknowledgeEnvironmentCreation() = %#v, %v", terminal.Record, err)
	}
	ready, err := repository.GetEnvironment(ctx, record.ID)
	if err != nil || ready.Record.ProvisioningState != EnvironmentProvisioningReady {
		t.Fatalf("ready Environment = %#v, %v", ready.Record, err)
	}
	readyEpoch, err := store.Get(ctx, environmentMutationEpochKey(record.ID))
	if err != nil || readyEpoch.Entry == nil || readyEpoch.Entry.ModRevision != ready.Revision {
		t.Fatalf("ready Environment mutation epoch = %#v, %v", readyEpoch, err)
	}
}

func environmentCreationTestComponents(t *testing.T, environmentID string, now time.Time) []ComponentRecord {
	t.Helper()
	kinds := []core.ComponentKind{
		core.ComponentKindIngressCaddy,
		core.ComponentKindEdgeCloudflare,
	}
	components := make([]ComponentRecord, 0, len(kinds))
	for index, kind := range kinds {
		component, err := NewComponentRecord(core.Component{
			ID:      ids.NewAt(ids.KindComponent, now, int64(706+index)),
			Owner:   core.ComponentOwnerEnvironment,
			OwnerID: environmentID,
			Kind:    kind,
			Enabled: false,
		})
		if err != nil {
			t.Fatalf("NewComponentRecord() error = %v", err)
		}
		components = append(components, component)
	}
	return components
}
