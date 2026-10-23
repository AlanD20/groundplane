//go:build etcd_acceptance

package app

import (
	"bytes"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	deletion "github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/controller/tasks"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// TASK-166: Abort must fence a prepared deletion before commit, but must never
// hide or undo a deletion which committed first. The real stop cursor settles
// remaining work without removing the parent or blocking unrelated metadata work.
func TestNativeHierarchyRealEtcdAbortFencesEffectsAndKeepsCompletedWork(t *testing.T) {
	for _, commitFirst := range []bool{false, true} {
		name := "abort-before-effect"
		if commitFirst {
			name = "abort-after-effect"
		}
		t.Run(name, func(t *testing.T) {
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
			tenant := hierarchy.TenantRecord{ID: ids.New(ids.KindTenant), Slug: "abort-removal", Name: "Abort removal"}
			if _, err := parents.CreateTenant(ctx, tenant); err != nil {
				t.Fatal(err)
			}
			project := hierarchy.ProjectRecord{ID: ids.New(ids.KindProject), TenantID: tenant.ID,
				Slug: "child", Name: "Child", Kind: hierarchy.ProjectKindTenant}
			if _, err := parents.CreateProject(ctx, project); err != nil {
				t.Fatal(err)
			}
			key, err := ageinfra.GenerateKeypair()
			if err != nil {
				t.Fatal(err)
			}
			owner := newNativeHierarchyEtcdRuntime(t, store, key, deletion.SystemClock{})
			accepted, err := owner.service.Delete(ctx, deletion.DeleteRequest{
				TargetKind: deletion.TargetTenant, TargetID: tenant.ID, IdempotencyKey: "native-etcd-abort-removal"})
			if err != nil {
				t.Fatal(err)
			}
			owner.publish(t, ctx, accepted.TaskID, time.Now().UTC())
			page, err := owner.queue.ListWork(ctx, accepted.TaskID, "", 10, 0)
			if err != nil || len(page.Items) != 2 {
				t.Fatalf("captured child/parent work: %v", err)
			}
			var child workqueue.ItemSnapshot
			for _, item := range page.Items {
				if item.Item.TargetID == project.ID {
					child = item
				}
			}
			if child.Revision == 0 {
				t.Fatal("captured graph has no child finalizer")
			}
			queued, err := store.Get(ctx, workqueue.QueueKey(child.Item))
			if err != nil || queued.Entry == nil {
				t.Fatalf("child queue membership: %v", err)
			}
			defer clear(queued.Entry.Value)
			at := time.Now().UTC()
			candidate, err := owner.queue.ReadCandidate(ctx, workqueue.QueueEntry{TaskID: accepted.TaskID,
				WorkItemID: child.Item.ID, Revision: queued.Entry.ModRevision}, owner.identity, at,
				[]workexecution.Operation{workexecution.Hierarchy})
			if err != nil || candidate.Wait != nil || candidate.Stopped {
				t.Fatalf("child admission: %v", err)
			}
			controller, err := deletion.NewControllerWork(owner.records)
			if err != nil {
				t.Fatal(err)
			}
			decision, err := controller.Admit(ctx, candidate.Assignment, at)
			if err != nil || decision.Disposition != workdispatch.Settle {
				t.Fatalf("metadata effect preparation: %v", err)
			}
			defer keyvalue.ClearMutationValues(decision.Change.Mutations)
			fence, err := workqueue.PrepareDecisionFence(candidate.Assignment)
			if err != nil {
				t.Fatal(err)
			}
			if commitFirst {
				applied, err := owner.queue.Commit(ctx, decision.Change, fence)
				defer keyvalue.ClearValues(applied.FailureReads)
				if err != nil || !applied.Succeeded {
					t.Fatalf("child effect commit: %v", err)
				}
			}
			committedChild, err := owner.queue.ReadWork(ctx, accepted.TaskID, child.Item.ID, 0)
			if err != nil {
				t.Fatal(err)
			}
			abort, err := tasks.NewAbortService(owner.queue)
			if err != nil {
				t.Fatal(err)
			}
			response, err := abort.AbortTask(ctx, accepted.TaskID, "native-etcd-abort-request")
			if err != nil {
				t.Fatal(err)
			}
			defer clear(response.Body)
			stale, err := owner.queue.Commit(ctx, decision.Change, fence)
			defer keyvalue.ClearValues(stale.FailureReads)
			if err != nil || stale.Succeeded {
				t.Fatalf("old prepared effect survived Abort or duplicated completion: %v", err)
			}
			stopping, err := owner.queue.Stopping(ctx, "")
			if err != nil || len(stopping.Items) != 1 || stopping.Items[0].Record.TaskID != accepted.TaskID {
				t.Fatalf("Abort did not retain its stop cursor: %v", err)
			}
			// A sweep may begin before the Abort/concurrent completion updates
			// the Task. Its fresh reads must still allow cancellation to settle.
			if err := owner.scheduler.ContinueStop(ctx, stopping.Items[0], at); err != nil {
				t.Fatal(err)
			}
			owner.finalize(t, ctx, accepted.TaskID)
			result, err := owner.queue.ReadWork(ctx, accepted.TaskID, child.Item.ID, 0)
			if err != nil || result.Task.Task.FinishedAt == nil || result.Task.Task.Outcome != workexecution.Cancelled {
				t.Fatalf("Abort did not finalize cancelled Task: %v", err)
			}
			if commitFirst {
				if result.Item.Item.Outcome != workexecution.Succeeded ||
					result.Item.Revision != committedChild.Item.Revision ||
					result.Item.Item.Result == nil ||
					committedChild.Item.Item.Result == nil ||
					*result.Item.Item.Result != *committedChild.Item.Item.Result {
					t.Fatal("Abort rewrote a completed irreversible child deletion")
				}
			} else if result.Item.Item.Outcome != workexecution.Cancelled {
				t.Fatal("unexecuted child was not cancelled")
			}
			physical, err := store.GetMany(ctx, keyvalue.GetManyRequest{
				Keys: []string{hierarchy.TenantKey(tenant.ID), hierarchy.ProjectKey(project.ID)},
			})
			if err != nil || physical == nil || len(physical.Values) != 2 {
				t.Fatalf("metadata after Abort: %v", err)
			}
			defer keyvalue.ClearValues(physical.Values)
			if physical.Values[0] == nil || (physical.Values[1] == nil) != commitFirst {
				t.Fatal("Abort changed parent metadata or claimed to undo a committed child effect")
			}
			repeated, err := abort.AbortTask(ctx, accepted.TaskID, "native-etcd-abort-request")
			if err != nil {
				t.Fatal(err)
			}
			defer clear(repeated.Body)
			after, err := store.Get(ctx, hierarchy.TenantKey(tenant.ID))
			if err != nil || after.ReadRevision != physical.ReadRevision || !bytes.Equal(response.Body, repeated.Body) {
				t.Fatalf("finished Abort replay wrote state or changed response: %v", err)
			}
			stopping, err = owner.queue.Stopping(ctx, "")
			if err != nil || len(stopping.Items) != 0 {
				t.Fatalf("completed cancellation retained its stop cursor: %v", err)
			}
			// Another removal must not inherit the cancelled Task's queue/fence.
			independent := hierarchy.TenantRecord{ID: ids.New(ids.KindTenant), Slug: "independent", Name: "Independent"}
			if _, err := parents.CreateTenant(ctx, independent); err != nil {
				t.Fatal(err)
			}
			next, err := owner.service.Delete(ctx, deletion.DeleteRequest{TargetKind: deletion.TargetTenant,
				TargetID: independent.ID, IdempotencyKey: "native-etcd-independent-removal"})
			if err != nil {
				t.Fatal(err)
			}
			owner.finish(t, ctx, next.TaskID)
			nextTask, _, err := owner.queue.ReadTask(ctx, next.TaskID, 0)
			if err != nil || nextTask.Task.Outcome != workexecution.Succeeded {
				t.Fatalf("Abort blocked unrelated removal: %v", err)
			}
		})
	}
}
