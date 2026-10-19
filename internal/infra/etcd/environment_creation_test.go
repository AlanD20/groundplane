package etcd

import (
	"context"
	"fmt"
	"net/http"
	"net/netip"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentexecution"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprovision"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testhierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// OWN-03/TASK-54: native acceptance and result settlement keep provisioning
// metadata, captured work, ownership, replay and capacity consistent. The
// directory result is supplied; no real Agent/filesystem effect is asserted.
func TestEnvironmentCreationAtomicallyPublishesProvisioningRecordAndTask(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenant := testhierarchy.TenantRecord{ID: hierarchyTestID(ids.KindTenant, 701), Slug: "acme", Name: "Acme"}
	if _, err := repository.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	projectRecord := testhierarchy.ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 702), TenantID: tenant.ID,
		Slug: "console", Name: "Console", Kind: testhierarchy.ProjectKindTenant,
	}
	if _, err := repository.CreateProject(ctx, projectRecord); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	project, err := repository.GetProject(ctx, projectRecord.ID)
	if err != nil {
		t.Fatalf("GetProject() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 16, 0, 0, 0, time.UTC)
	taskID := ids.NewAt(ids.KindTask, now, 703)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 705)
	record, err := testhierarchy.NewProvisioningEnvironment(
		environmentpath.DefaultVolumeRoot,
		projectRecord,
		environmentID,
		"production",
		"10.200.0.0/16",
		taskID,
		now,
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment() error = %v", err)
	}
	agentID := ids.NewAt(ids.KindAgent, now, 708)
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	enrollment, err := hostbindings.PrepareLocalEnrollment(ctx, store, agentID)
	if err != nil {
		t.Fatal(err)
	}
	commitEnvironmentWork(t, queue, enrollment)
	host, err := hostbindings.ReadLocal(ctx, store, 0)
	if err != nil {
		t.Fatal(err)
	}
	input := environmentprovision.Input{TaskID: taskID, WorkItemID: ids.New(ids.KindWorkItem),
		PlanID: ids.New(ids.KindPlan), HostID: host.Binding.HostID, VolumeRoot: environmentpath.DefaultVolumeRoot,
		Scope:       workexecution.Scope{TenantID: tenant.ID, ProjectID: projectRecord.ID, EnvironmentID: record.ID},
		Environment: record, CreatedAt: now}
	task, _, err := input.Work()
	if err != nil {
		t.Fatal(err)
	}
	components := environmentCreationTestComponents(t, record.ID, now)
	marker := testDirectMarker()
	marker.Kind, marker.State, marker.TaskID = testidempotency.IdempotencyMarkerTask, testidempotency.IdempotencyMarkerPending, task.ID
	marker.CreatedAt, marker.UpdatedAt, marker.TerminalAt, marker.RetainUntil = now, now, time.Time{}, time.Time{}
	marker.Response.Status = http.StatusAccepted
	marker.Response.Body = []byte(fmt.Sprintf(`{"task_id":%q}`, task.ID))
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeProject, ScopeID: projectRecord.ID,
		Method: http.MethodPost, Route: "/environments", Key: "environment-create-key-0001",
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
	result, err := repository.CreateEnvironmentWithWork(
		ctx, environmentpath.DefaultVolumeRoot, project, poolRegistry, record, components, input, marker,
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("CreateEnvironmentWithWork() = %#v, %v", result, err)
	}
	storedRegistry, err := repository.GetEnvironmentPoolRegistry(ctx)
	if err != nil || storedRegistry.Record.Reservations[record.ID] != record.NetworkPool {
		t.Fatalf("stored Environment pool registry = %#v, %v", storedRegistry.Record, err)
	}
	storedEnvironment, err := repository.GetEnvironment(ctx, record.ID)
	if err != nil || storedEnvironment.Record.ProvisioningState != testhierarchy.EnvironmentProvisioningProvisioning ||
		storedEnvironment.Record.CreateTaskID != task.ID {
		t.Fatalf("stored Environment = %#v, %v", storedEnvironment.Record, err)
	}
	assertHierarchyCoordinationRecord(
		t,
		store, testhierarchydeletion.HierarchyDeletionTargetEnvironment, record.ID,
		storedEnvironment.Revision,
	)
	storedEpoch, err := store.Get(ctx, testhierarchy.EnvironmentMutationEpochKey(record.ID))
	if err != nil || storedEpoch.Entry == nil || storedEpoch.Entry.ModRevision != storedEnvironment.Revision {
		t.Fatalf("stored Environment mutation epoch = %#v, %v", storedEpoch, err)
	}
	epoch, err := testbackupruntime.DecodeEnvironmentMutationEpochRecord(storedEpoch.Entry.Value)
	if err != nil || epoch.EnvironmentID != record.ID {
		t.Fatalf("decoded Environment mutation epoch = %#v, %v", epoch, err)
	}
	view, err := queue.ReadWork(ctx, task.ID, input.WorkItemID, 0)
	if err != nil || !view.Task.Task.PublicationComplete || view.Item.Item.Phase != workexecution.Ready ||
		view.Task.Task.TargetID != record.ID || view.Task.Task.Action != workexecution.ActionCreate ||
		view.Item.Revision != storedEnvironment.Revision {
		t.Fatalf("native Environment publication was not atomic or executable: %#v, %v", view, err)
	}
	storedTask := view.Task
	componentRepository, err := newComponentRepository(store)
	if err != nil {
		t.Fatalf("newComponentRepository() error = %v", err)
	}
	storedComponents, err := componentRepository.ListEnvironmentComponents(
		ctx,
		record.ID, testkeyvalue.PageRequest{Limit: 20},
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
	completeNativeEnvironmentWork(t, store, queue, input, agentID)
	ready, err := repository.GetEnvironment(ctx, record.ID)
	if err != nil || ready.Record.ProvisioningState != testhierarchy.EnvironmentProvisioningReady ||
		ready.Record.CreateTaskID != task.ID {
		t.Fatalf("native result did not make the same Environment ready: %#v, %v", ready.Record, err)
	}
	readyEpoch, err := store.Get(ctx, testhierarchy.EnvironmentMutationEpochKey(record.ID))
	if err != nil || readyEpoch.Entry == nil || readyEpoch.Entry.ModRevision != ready.Revision {
		t.Fatalf("ready Environment mutation epoch = %#v, %v", readyEpoch, err)
	}
	before := store.revision
	replayed, err := repository.CreateEnvironmentWithWork(ctx, environmentpath.DefaultVolumeRoot, project,
		poolRegistry, record, components, input, marker)
	if err != nil || replayed.kind != idempotencyTransactionExisting || store.revision != before {
		t.Fatalf("creation replay after provisioning rewrote resource state: %v", err)
	}
}

func environmentCreationTestComponents(t *testing.T, environmentID string, now time.Time) []testcomponents.Record {
	t.Helper()
	kinds := []core.ComponentKind{
		core.ComponentKindIngressCaddy,
		core.ComponentKindEdgeCloudflare,
	}
	components := make([]testcomponents.Record, 0, len(kinds))
	for index, kind := range kinds {
		component, err := testcomponents.NewRecord(core.Component{
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

func completeNativeEnvironmentWork(t *testing.T, store *memoryHierarchyStore, queue *workqueue.Repository,
	input environmentprovision.Input, agentID string,
) {
	t.Helper()
	ctx := t.Context()
	at := input.CreatedAt
	registered, identity, err := workqueue.PrepareRegistration(workqueue.ExecutorSnapshot{}, workqueue.ExecutorRecord{
		Identity: workexecution.Identity{
			Executor: workexecution.Agent,
			HostID:   input.HostID,
			AgentID:  agentID,
		},
		ProcessGeneration: [16]byte{1}, Capacity: 1, Operations: []workexecution.Operation{workexecution.Environment}})
	if err != nil {
		t.Fatal(err)
	}
	commitEnvironmentWork(t, queue, registered)
	placement := workexecution.Placement{Executor: workexecution.Agent, HostID: input.HostID}
	executor, err := queue.ReadExecutor(ctx, placement)
	if err != nil {
		t.Fatal(err)
	}
	available, err := workqueue.PrepareAvailability(executor, identity.Identity, true, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	commitEnvironmentWork(t, queue, available)
	page, err := queue.ScanReady(ctx, placement, "")
	if err != nil || len(page.Entries) != 1 {
		t.Fatalf("Environment ready queue: %#v, %v", page, err)
	}
	candidate, err := queue.ReadCandidate(ctx, page.Entries[0], identity.Identity, at.Add(time.Second), nil)
	if err != nil || candidate.Wait != nil || candidate.Stopped {
		t.Fatalf("Environment assignment: %#v, %v", candidate, err)
	}
	request := candidate.Assignment
	request.AttemptID, request.At, request.Deadline = ids.New(ids.KindAttempt), at.Add(time.Second), at.Add(time.Minute)
	assignment, attempt, err := workqueue.PrepareAssignment(request)
	if err != nil {
		t.Fatal(err)
	}
	commitEnvironmentWork(t, queue, assignment)
	view, err := queue.ReadResume(ctx, input.TaskID, input.WorkItemID, attempt.ID, identity.Identity)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := environmentprovision.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	_, source, err := owner.ReadyForHost(ctx, view.Work)
	if err != nil {
		t.Fatal(err)
	}
	executor, err = queue.ReadExecutor(ctx, placement)
	if err != nil {
		t.Fatal(err)
	}
	started, err := workqueue.PrepareStart(
		view.Work.Task,
		view.Work.Item,
		view.Attempt,
		executor,
		at.Add(2*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	commitEnvironmentWork(t, queue, source, started)
	view, err = queue.ReadResume(ctx, input.TaskID, input.WorkItemID, attempt.ID, identity.Identity)
	if err != nil {
		t.Fatal(err)
	}
	report := environmentexecution.Result{AttemptID: attempt.ID, DirectoryReady: true,
		HelpersAbsent: true, ObservedAt: at.Add(3 * time.Second)}
	resource, completion, err := owner.PrepareResult(ctx, view, report, at.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer testkeyvalue.ClearMutationValues(resource.Mutations)
	ending, err := queue.ReadCompletionAt(ctx, view)
	if err != nil {
		t.Fatal(err)
	}
	ending.Result, ending.At = completion, at.Add(4*time.Second)
	settled, err := workqueue.PrepareCompletion(ending)
	if err != nil {
		t.Fatal(err)
	}
	commitEnvironmentWork(t, queue, resource, settled)
	finished, err := queue.ReadResume(ctx, input.TaskID, input.WorkItemID, attempt.ID, identity.Identity)
	if err != nil || finished.Work.Item.Item.Phase != workexecution.Settled ||
		finished.Work.Item.Item.Outcome != workexecution.Succeeded || finished.Attempt.Attempt.Outcome != workexecution.Succeeded {
		t.Fatalf("Environment native settlement: %#v, %v", finished, err)
	}
	current, err := store.Get(ctx, testhierarchy.EnvironmentKey(input.Environment.ID))
	if err != nil || current.Entry == nil || current.Entry.ModRevision != finished.Work.Item.Revision ||
		finished.Attempt.Revision != finished.Work.Item.Revision || finished.Work.Task.Revision != finished.Work.Item.Revision {
		t.Fatalf("Environment readiness/evidence/progress did not commit together: %v", err)
	}
	if _, recorded, err := queue.ReadCompletion(ctx, input.TaskID, input.WorkItemID, attempt.ID, identity.Identity); err != nil ||
		!recorded {
		t.Fatalf("repeated result did not find its retained native attempt: %v", err)
	}
	slots, err := store.Range(ctx, testkeyvalue.RangeRequest{Prefix: workqueue.SlotKey(identity.Identity, 0), Limit: 1})
	if err != nil || len(slots.Values) != 0 {
		t.Fatalf("settled Environment retained worker capacity: %v", err)
	}
}

func commitEnvironmentWork(t *testing.T, queue *workqueue.Repository, changes ...workqueue.Change) {
	t.Helper()
	result, err := queue.Commit(t.Context(), changes...)
	if err != nil || !result.Succeeded {
		t.Fatalf("native Environment transaction: committed=%t, %v", result.Succeeded, err)
	}
}
