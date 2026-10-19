package app

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/volume"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// TASK-160: executor completion alone cannot release deletion protection.
// Visible settlement releases it; reopening the same failed work must reacquire
// it without crossing a parent-deletion or newly captured Script boundary.
func TestNativeVolumeRemovalOwnershipSettlementAndRetry(t *testing.T) {
	for _, mode := range []string{"complete", "retry", "parent", "late parent", "Script"} {
		t.Run(mode, func(t *testing.T) {
			f := newNativeVolumeFixture(t)
			finishNativeVolumeFixture(t, f, f.created.TaskID)
			view, _, _ := f.remove(t)
			taskID, workID := view.Work.Task.Task.ID, view.Work.Item.Item.ID
			assertOwnership := func(present bool) {
				t.Helper()
				for _, key := range []string{removalrecord.OwnerKey(f.created.Volume.ID), removalrecord.EnvironmentLockKey(f.environmentID)} {
					read, err := f.store.Get(t.Context(), key)
					if err != nil || (read.Entry != nil) != present {
						t.Fatalf("removal ownership present=%v at %s: %v", present, key, err)
					}
					if present {
						owner, err := removalrecord.DecodeOwner(read.Entry.Value)
						if err != nil || owner.TaskID != taskID || owner.WorkItemID != workID ||
							owner.VolumeID != f.created.Volume.ID {
							t.Fatalf("ownership differs from accepted native work: %v", err)
						}
					}
				}
			}
			assertOwnership(true)
			if mode == "complete" {
				progress := f.progress(t, view)
				for progress.Stage != volumeexecution.Complete {
					progress = f.advance(t, view, progress)
				}
				f.complete(t, view, progress)
			} else {
				wire, err := workexecution.EncodeAgentAttempt(view.Attempt.Attempt)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.reports.FailVolume(t.Context(), f.authorization, &agentpb.VolumeWorkFailureRequest{
					Attempt: wire, Reason: string(volumeexecution.ExecutionFailed),
				}); err != nil {
					t.Fatal(err)
				}
			}
			assertOwnership(true)
			finishNativeVolumeTask(t, f, taskID)
			assertOwnership(false)
			if mode == "complete" {
				return
			}
			parent := nativeVolumeParentDeletion(
				t,
				view.Work.Task.Task.Scope,
				hierarchydeletion.HierarchyDeletionTargetProject,
			)
			removeParent := func() {
				t.Helper()
				journal, err := etcd.NewHierarchyDeletionRepository(f.store)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := journal.Begin(t.Context(), parent); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "parent" {
				removeParent()
			}
			if mode == "Script" {
				release := nativeVolumePreparedSource(t, f)
				defer release()
			}
			current, previous, err := f.queue.ReadAttempt(t.Context(), taskID, workID, 1, 0)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := volume.NewAgentWork(f.inputs, f.queue)
			if err != nil {
				t.Fatal(err)
			}
			change, err := owner.PrepareWorkRetry(
				t.Context(),
				current.Task,
				current.Item,
				previous,
				current.ReadRevision,
				time.Now().UTC(),
			)
			if mode == "parent" || mode == "Script" {
				if kind, _ := errs.KindOf(err); kind != errs.KindTaskNotRetryable {
					t.Fatalf("Retry crossed protected resource boundary: %v", err)
				}
				assertOwnership(false)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer keyvalue.ClearMutationValues(change.Mutations)
			if mode == "late parent" {
				f.store.beforeCommit = func(mutations []keyvalue.Mutation) {
					for _, mutation := range mutations {
						if mutation.Key == removalrecord.OwnerKey(f.created.Volume.ID) {
							f.store.beforeCommit = nil
							removeParent()
							return
						}
					}
				}
			}
			committed, err := f.queue.Commit(t.Context(), change)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "late parent" {
				if committed.Succeeded || f.store.beforeCommit != nil {
					t.Fatal("late parent deletion did not defeat Retry publication")
				}
				assertOwnership(false)
				return
			}
			if !committed.Succeeded {
				t.Fatal("native same-Task Retry did not commit")
			}
			assertOwnership(true)
			reopened, _, err := f.queue.ReadTask(t.Context(), taskID, 0)
			if err != nil || reopened.Task.FinishedAt != nil {
				t.Fatalf("Retry did not reopen the same Task: %v", err)
			}
		})
	}
}
