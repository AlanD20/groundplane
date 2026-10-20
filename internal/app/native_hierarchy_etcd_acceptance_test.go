//go:build etcd_acceptance

package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	deletion "github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TASK-166: actual native admission, captured-plan publication and metadata
// scheduling must remove children before parents, preserve same-key replay after
// definition removal and refuse a sibling Project claiming another's request.
// Empty Projects need no Agent; this does not qualify host/resource cleanup.
func TestNativeHierarchyRealEtcdParentLastAndDeletedTargetReplay(t *testing.T) {
	ctx, _, open := nativeEtcdAcceptanceConnector(t)
	store := open()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	parents, err := etcd.NewHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	tenant := hierarchy.TenantRecord{ID: ids.New(ids.KindTenant), Slug: "durable-removal", Name: "Durable removal"}
	if _, err := parents.CreateTenant(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	projects := []hierarchy.ProjectRecord{
		{
			ID:       ids.New(ids.KindProject),
			TenantID: tenant.ID,
			Slug:     "first",
			Name:     "First",
			Kind:     hierarchy.ProjectKindTenant,
		},
		{
			ID:       ids.New(ids.KindProject),
			TenantID: tenant.ID,
			Slug:     "second",
			Name:     "Second",
			Kind:     hierarchy.ProjectKindTenant,
		},
	}
	for _, project := range projects {
		if _, err := parents.CreateProject(ctx, project); err != nil {
			t.Fatal(err)
		}
	}
	key, err := ageinfra.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	owner := newNativeHierarchyEtcdRuntime(t, store, key)
	request := deletion.DeleteRequest{TargetKind: deletion.TargetProject, TargetID: projects[0].ID,
		IdempotencyKey: "native-real-etcd-project-removal"}
	accepted, err := owner.service.Delete(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	owner.finish(t, ctx, accepted.TaskID)
	if _, err := parents.GetProject(ctx, projects[0].ID); !errors.Is(err, errs.New(errs.KindProjectNotFound, "")) {
		t.Fatalf("completed removal retained Project definition: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = open()
	owner = newNativeHierarchyEtcdRuntime(t, store, key)
	replayed, err := owner.service.Delete(ctx, request)
	if err != nil || !replayed.Existing || replayed.TaskID != accepted.TaskID ||
		replayed.OperationID != accepted.OperationID {
		t.Fatalf("removed Project lost original accepted response: %+v, %v", replayed, err)
	}
	request.TargetID = projects[1].ID
	if _, err := owner.service.Delete(ctx, request); !errors.Is(err, errs.New(errs.KindIdempotencyMismatch, "")) {
		t.Fatalf("sibling Project reused another Project's owner-scoped request: %v", err)
	}
	parents, err = etcd.NewHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parents.GetProject(ctx, projects[1].ID); err != nil {
		t.Fatalf("rejected replay changed sibling Project: %v", err)
	}
	request = deletion.DeleteRequest{TargetKind: deletion.TargetTenant, TargetID: tenant.ID,
		IdempotencyKey: "native-real-etcd-tenant-removal"}
	accepted, err = owner.service.Delete(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	owner.finish(t, ctx, accepted.TaskID)
	page, err := owner.queue.ListWork(ctx, accepted.TaskID, "", 10, 0)
	if err != nil || len(page.Items) != 2 {
		t.Fatalf("Tenant removal did not retain child and parent work: %v", err)
	}
	var child, parent workqueue.ItemSnapshot
	for _, item := range page.Items {
		if item.Item.TargetID == tenant.ID {
			parent = item
		} else if item.Item.TargetID == projects[1].ID {
			child = item
		}
	}
	if child.Revision == 0 || parent.Revision <= child.Revision ||
		child.Item.Outcome != workexecution.Succeeded || parent.Item.Outcome != workexecution.Succeeded {
		t.Fatal("parent removal did not follow acknowledged child removal")
	}
	prior, err := store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{hierarchy.TenantKey(tenant.ID), hierarchy.ProjectKey(projects[1].ID)}, Revision: child.Revision,
	})
	if err != nil || prior == nil || len(prior.Values) != 2 {
		t.Fatalf("read child completion revision: %v", err)
	}
	defer keyvalue.ClearValues(prior.Values)
	if prior.Values[0] == nil || prior.Values[1] != nil {
		t.Fatal("Tenant disappeared before its child removal completed")
	}
	if _, err := parents.GetTenant(ctx, tenant.ID); !errors.Is(err, errs.New(errs.KindTenantNotFound, "")) {
		t.Fatalf("completed removal retained Tenant definition: %v", err)
	}
	replayed, err = owner.service.Delete(ctx, request)
	if err != nil || !replayed.Existing || replayed.TaskID != accepted.TaskID {
		t.Fatalf("removed Tenant lost captured replay: %+v, %v", replayed, err)
	}
}

type nativeHierarchyEtcdRuntime struct {
	service     *deletion.Service
	records     *etcd.HierarchyDeletionRepository
	queue       *workqueue.Repository
	publication *workdispatch.Publications
	scheduler   *workdispatch.Scheduler
	identity    workexecution.Identity
}

func newNativeHierarchyEtcdRuntime(
	t *testing.T,
	store nativeEtcdStore,
	key ageinfra.Keypair,
) *nativeHierarchyEtcdRuntime {
	t.Helper()
	cipher := nativeVolumeCipher{key: key}
	protector, err := secretvalue.NewProtector(cipher, cipher)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := etcd.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	service, err := deletion.NewRuntime(store, evidence, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	records, err := etcd.NewHierarchyDeletionRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	attachRecords, err := etcd.NewAttachRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := attachments.NewFactService(attachRecords, protector)
	if err != nil {
		t.Fatal(err)
	}
	removals, err := attachplan.NewRemovalPlanner(facts)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := deletion.NewWorkPublication(service, records, queue, removals, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	publications, err := workdispatch.NewPublications(queue, []workdispatch.PublicationRegistration{
		{Action: workexecution.ActionRemove, Owner: publication},
	})
	if err != nil {
		t.Fatal(err)
	}
	controller, err := deletion.NewControllerWork(records)
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := workdispatch.NewScheduler(queue, []workdispatch.Registration{
		{Operation: workexecution.Hierarchy, Executor: workexecution.Controller, Owner: controller, DecisionOnly: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	previous, err := queue.ReadExecutor(t.Context(), workexecution.Placement{Executor: workexecution.Controller})
	if err != nil {
		t.Fatal(err)
	}
	registration, registered, err := workqueue.PrepareRegistration(previous, workqueue.ExecutorRecord{
		Identity: workexecution.Identity{Executor: workexecution.Controller}, Capacity: 1,
		Operations: []workexecution.Operation{workexecution.Hierarchy},
	})
	if err != nil {
		t.Fatal(err)
	}
	commitNativeEtcd(t, queue, registration)
	previous, err = queue.ReadExecutor(t.Context(), workexecution.Placement{Executor: workexecution.Controller})
	if err != nil {
		t.Fatal(err)
	}
	availability, err := workqueue.PrepareAvailability(previous, registered.Identity, true, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	commitNativeEtcd(t, queue, availability)
	return &nativeHierarchyEtcdRuntime{service, records, queue, publications, scheduler, registered.Identity}
}

func (owner *nativeHierarchyEtcdRuntime) finish(t *testing.T, ctx context.Context, taskID string) {
	t.Helper()
	for batch := 0; batch < 10; batch++ {
		if err := owner.publication.Advance(ctx, taskID, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		task, _, err := owner.queue.ReadTask(ctx, taskID, 0)
		if err != nil {
			t.Fatal(err)
		}
		if task.Task.PublicationComplete {
			break
		}
		if batch == 9 {
			t.Fatal("native hierarchy publication did not finish within its bound")
		}
	}
	for sweep := 0; sweep < 10; sweep++ {
		result, err := owner.scheduler.Schedule(ctx, owner.identity, "", time.Now().UTC())
		if err != nil || len(result.Issues) != 0 {
			t.Fatalf("native hierarchy scheduling: %+v, %v", result.Issues, err)
		}
		task, _, err := owner.queue.ReadTask(ctx, taskID, 0)
		if err != nil {
			t.Fatal(err)
		}
		if task.Task.Progress.Succeeded == task.Task.ExpectedItems {
			break
		}
		if sweep == 9 {
			t.Fatalf("native hierarchy did not settle within its bound: %+v", task.Task.Progress)
		}
	}
	finished, err := owner.queue.ReadFinishedWork(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	proof, err := owner.records.PrepareHierarchyWorkFinish(ctx, finished, at)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := owner.queue.PrepareTaskFinish(ctx, finished.Task, at, at.Add(workexecution.TaskHistoryRetention))
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(terminal.Mutations)
	commitNativeEtcd(t, owner.queue, proof, terminal)
}

func commitNativeEtcd(t *testing.T, queue *workqueue.Repository, changes ...workqueue.Change) {
	t.Helper()
	for _, change := range changes {
		defer keyvalue.ClearMutationValues(change.Mutations)
	}
	result, err := queue.Commit(t.Context(), changes...)
	defer keyvalue.ClearValues(result.FailureReads)
	if err != nil || !result.Succeeded {
		t.Fatalf("native real-etcd commit: succeeded=%t error=%v", result.Succeeded, err)
	}
}
