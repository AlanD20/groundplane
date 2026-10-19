package etcd

import (
	context "context"
	errors "errors"
	testing "testing"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"

	ids "github.com/AlanD20/groundplane/internal/common/ids"
	runnerallocation "github.com/AlanD20/groundplane/internal/common/runnerallocation"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrunners "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	errs "github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: one atomic create must publish exactly one owner index while the
// Tenant-wide quota and host-global registries receive the lowest free slots.
func TestRunnerCreatePublishesOneOwnerAndLowestFreeAllocations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, repository, tenantID, projectID := newRunnerRepositoryFixture(t)
	config := runnerTestAllocationConfig()

	tenantRunner := runnerTestDesired(10, testrunners.RunnerOwnerTenant, tenantID, tenantID)
	firstFixture, err := prepareRunnerCreationFixture(t, repository, config, tenantRunner, 20, "runner-create-key-0001")
	if err != nil {
		t.Fatal(err)
	}
	first := firstFixture.publish(t, repository)
	if outcome, _, _, classifyErr := first.Classify(); classifyErr != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("CreateRunnerWithTask(tenant) outcome = %v, %v", outcome, classifyErr)
	}

	projectRunner := runnerTestDesired(11, testrunners.RunnerOwnerProject, projectID, tenantID)
	secondFixture, err := prepareRunnerCreationFixture(
		t,
		repository,
		config,
		projectRunner,
		21,
		"runner-create-key-0002",
	)
	if err != nil {
		t.Fatal(err)
	}
	secondFixture.publish(t, repository)
	firstRecord, err := repository.GetRunner(ctx, tenantRunner.ID)
	if err != nil {
		t.Fatalf("GetRunner(first) error = %v", err)
	}
	secondRecord, err := repository.GetRunner(ctx, projectRunner.ID)
	if err != nil {
		t.Fatalf("GetRunner(second) error = %v", err)
	}
	if firstRecord.Record.ProvisioningState != testrunners.RunnerProvisioningProvisioning ||
		firstRecord.Record.Allocation.Slot != 0 ||
		firstRecord.Record.Allocation.HostUID != config.HostPool.HostUIDStart ||
		firstRecord.Record.Allocation.NetworkCIDR != "10.240.0.0/29" {
		t.Fatalf("first Runner = %#v", firstRecord.Record)
	}
	assertRunnerNativePublication(t, store, firstRecord, firstFixture)
	if secondRecord.Record.Allocation.Slot != 1 ||
		secondRecord.Record.Allocation.NetworkCIDR != "10.240.0.8/29" {
		t.Fatalf("second Runner = %#v", secondRecord.Record)
	}
	indexes, err := store.GetMany(
		ctx,
		testkeyvalue.GetManyRequest{
			Keys: []string{
				testrunners.RunnerOwnerKey(testrunners.RunnerOwnerProject, projectID, projectRunner.ID),
				testrunners.RunnerOwnerKey(testrunners.RunnerOwnerTenant, tenantID, projectRunner.ID),
			},
		},
	)
	if err != nil {
		t.Fatalf("GetMany(owner indexes) error = %v", err)
	}
	if indexes.Values[0] == nil || indexes.Values[1] != nil {
		t.Fatalf("project Runner owner indexes = %#v, want exactly the Project index", indexes.Values)
	}
	page, err := repository.ListRunners(
		ctx,
		testrunners.RunnerFilter{TenantID: tenantID},
		testkeyvalue.PageRequest{Limit: 1},
	)
	if err != nil || len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatalf("ListRunners(first page) = %#v, %v", page, err)
	}
	next, err := repository.ListRunners(
		ctx, testrunners.RunnerFilter{TenantID: tenantID}, testkeyvalue.PageRequest{Limit: 1, Cursor: page.NextCursor},
	)
	if err != nil || len(next.Items) != 1 || next.NextCursor != "" {
		t.Fatalf("ListRunners(second page) = %#v, %v", next, err)
	}
	// RUN-10: host maintenance must discover every ownership scope and hydrate
	// its lifecycle, while operator listing still requires an explicit scope.
	runtimePage, err := repository.ListRuntimeRunners(ctx, testkeyvalue.PageRequest{Limit: 1})
	if err != nil || len(runtimePage.Items) != 1 || runtimePage.NextCursor == "" ||
		runtimePage.Items[0].Record.Desired.ID != tenantRunner.ID ||
		runtimePage.Items[0].Record.LifecycleRevision != firstRecord.Record.LifecycleRevision {
		t.Fatalf("runtime first page = %#v, %v", runtimePage, err)
	}
	runtimeNext, err := repository.ListRuntimeRunners(ctx, testkeyvalue.PageRequest{
		Limit: 1, Cursor: runtimePage.NextCursor, Revision: runtimePage.Revision,
	})
	if err != nil || len(runtimeNext.Items) != 1 || runtimeNext.NextCursor != "" ||
		runtimeNext.Items[0].Record.Desired.ID != projectRunner.ID ||
		runtimeNext.Items[0].Record.LifecycleRevision != secondRecord.Record.LifecycleRevision {
		t.Fatalf("runtime next page = %#v, %v", runtimeNext, err)
	}
	if _, err := repository.ListRunners(ctx, testrunners.RunnerFilter{}, testkeyvalue.PageRequest{}); err == nil {
		t.Fatal("operator listing accepted no ownership scope")
	}
}

// Rationale: all ownership scopes consume one Tenant quota and exact
// idempotency replay must not allocate a second host identity or subnet.
func TestRunnerCreateReplayAndCombinedTenantQuota(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, repository, tenantID, projectID := newRunnerRepositoryFixture(t)
	environmentID := runnerTestEnvironment(t, store, tenantID, projectID)
	config := runnerTestAllocationConfig()
	for index := 0; index < runnerallocation.MaximumTenantRunners; index++ {
		ownerKind, ownerID := testrunners.RunnerOwnerTenant, tenantID
		if index%3 == 1 {
			ownerKind, ownerID = testrunners.RunnerOwnerProject, projectID
		} else if index%3 == 2 {
			ownerKind, ownerID = testrunners.RunnerOwnerEnvironment, environmentID
		}
		desired := runnerTestDesired(30+index, ownerKind, ownerID, tenantID)
		if ownerKind == testrunners.RunnerOwnerEnvironment {
			desired.ParentProjectID = projectID
		}
		key := "runner-quota-key-000" + string(rune('0'+index))
		fixture, err := prepareRunnerCreationFixture(t, repository, config, desired, 40+index, key)
		if err != nil {
			t.Fatal(err)
		}
		fixture.publish(t, repository)
		if index == 0 {
			before := store.revision
			replay := fixture.publish(t, repository)
			if store.revision != before {
				t.Fatal("replay allocated or rewrote Runner state")
			}
			outcome, marker, conflict, classifyErr := replay.Classify()
			if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownExisting ||
				marker.TaskID != fixture.task.ID {
				t.Fatalf("CreateRunnerWithTask(replay) = %v, %#v, %v, %v", outcome, marker, conflict, classifyErr)
			}
		}
	}
	sixth := runnerTestDesired(99, testrunners.RunnerOwnerTenant, tenantID, tenantID)
	if _, err := repository.PrepareRunnerCreation(ctx, config, sixth,
		ids.NewAt(ids.KindTask, runnerStorageTime(), 999), runnerStorageTime()); !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("over-quota preparation = %v", err)
	}
}

func assertRunnerNativePublication(t *testing.T, store *memoryHierarchyStore,
	record testkeyvalue.Versioned[testrunners.RunnerRecord], fixture runnerCreationFixture,
) {
	t.Helper()
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	view, err := queue.ReadWork(t.Context(), fixture.task.ID, fixture.input.WorkItemID, record.Revision)
	if err != nil || view.Item.Item.Phase != workexecution.Ready ||
		!view.Task.Task.PublicationComplete || view.Task.Task.Scope != fixture.task.Scope ||
		view.Task.Revision != record.Revision || view.Item.Revision != record.Revision {
		t.Fatalf("Runner resource and native work were not published atomically: %#v, %v", view, err)
	}
	for _, key := range []string{
		testrunners.RunnerLifecycleKey(record.Record.Desired.ID),
		testrunners.RunnerOwnerKey(record.Record.Desired.OwnerKind, record.Record.Desired.OwnerID, record.Record.Desired.ID),
		testrunners.RunnerTenantSlugKey(record.Record.Desired.TenantID, record.Record.Desired.Slug),
		testrunners.RunnerTenantQuotaKey(record.Record.Desired.TenantID),
		testrunners.RunnerHostSlotKey(record.Record.Allocation.Slot), testrunners.SystemPoolRegistryKey,
		runnerwork.InputKey(fixture.input.WorkItemID), runnerwork.AllocationPlanKey(record.Record.Desired.ID),
	} {
		read, err := store.GetMany(
			t.Context(),
			testkeyvalue.GetManyRequest{Keys: []string{key}, Revision: record.Revision},
		)
		if err != nil || read == nil || len(read.Values) != 1 || read.Values[0] == nil ||
			read.Values[0].ModRevision != record.Revision {
			t.Fatalf("%s did not join Runner publication", key)
		}
	}
}
