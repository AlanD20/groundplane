package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// TASK-156: absence, source progress and replay must be one commit. A failed
// deletion keeps surviving objects selected; an already absent object is never
// resurrected or reported as restored. Cleanup preserves the original failure.
func TestNativeBackupPruneLifecycle(t *testing.T) {
	for _, test := range []struct {
		name          string
		count, absent int
		success       bool
	}{
		{"failure_before_delete", 1, 0, false},
		{"failure_after_delete", 1, 1, false},
		{"maximum_selection_partial_failure", backupruntime.MaximumBackupPruneDispatchPoints, 1, false},
		{"maximum_selection_success", backupruntime.MaximumBackupPruneDispatchPoints, backupruntime.MaximumBackupPruneDispatchPoints, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newNativePruneFixture(t, test.count)
			view := fixture.start(t)
			if _, err := fixture.owner.PreparePruneCompletion(t.Context(), view); err == nil {
				t.Fatal("completion accepted without exact remote absence")
			}
			absent := min(test.absent, len(fixture.points))
			for index := 0; index < absent; index++ {
				view = fixture.absence(t, view, uint32(index))
			}
			at := fixture.task.CreatedAt.Add(time.Duration(10+test.count) * time.Second)
			var resources workqueue.Change
			var err error
			outcome := workexecution.Failed
			if test.success {
				outcome = workexecution.Succeeded
				resources, err = fixture.owner.PreparePruneCompletion(t.Context(), view)
			} else {
				resources, err = fixture.owner.PreparePruneFailure(t.Context(), view, at)
			}
			if err != nil {
				t.Fatal(err)
			}
			completion, err := fixture.queue.ReadCompletionAt(t.Context(), view)
			if err != nil {
				t.Fatal(err)
			}
			evidence := *fixture.item.Input
			if view.Checkpoint.Record != nil {
				evidence = view.Checkpoint.Record.Evidence
			}
			completion.Result = workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: true}
			completion.At = at
			settlement, err := workqueue.PrepareCompletion(completion)
			if err != nil {
				t.Fatal(err)
			}
			commit := fixture.commit(t, resources, settlement)
			finished, err := fixture.queue.ReadFinishedWork(t.Context(), fixture.task.ID)
			if err != nil || len(finished.Items) != 1 || finished.Items[0].Item.Outcome != outcome {
				t.Fatalf("prune result: %#v, %v", finished, err)
			}
			for index, point := range fixture.points {
				prune := mustOptionalKey(t, fixture.store, backupruntime.BackupRecoveryPointPruneKey(point.ID))
				primary := mustOptionalKey(t, fixture.store, backupruntime.BackupRecoveryPointKey(point.ID))
				if index < absent {
					if prune != nil || primary != nil {
						t.Fatal("absent object regained local ownership")
					}
				} else {
					if prune == nil || primary == nil {
						t.Fatal("surviving object lost cleanup ownership")
					}
					record, err := backupruntime.DecodeBackupRecoveryPointPruneRecord(prune.Value)
					if err != nil || record.State != backupruntime.BackupPrunePending || record.TaskID != "" || prune.ModRevision != commit.Revision {
						t.Fatalf("surviving prune selection: %v/%v", record, err)
					}
				}
			}
			for _, pending := range fixture.unselected {
				point := pending.Record.Point
				if mustOptionalKey(t, fixture.store, backupruntime.BackupRecoveryPointKey(point.ID)) == nil {
					t.Fatal("budget selection lost an unselected Point")
				}
				value := mustOptionalKey(t, fixture.store, backupruntime.BackupRecoveryPointPruneKey(point.ID))
				if value == nil || value.ModRevision != pending.Revision {
					t.Fatal("budget selection modified unselected cleanup")
				}
			}
			for _, key := range []string{backupruntime.BackupRecoveryPointPruneDispatchKey(fixture.task.ID),
				hierarchy.EnvironmentOperationLockKey(fixture.task.Scope.EnvironmentID), workqueue.SlotKey(view.Attempt.Attempt.Executor, 0)} {
				if mustOptionalKey(t, fixture.store, key) != nil {
					t.Fatalf("settlement retained %s", key)
				}
			}
			proof, err := fixture.owner.PreparePruneFinish(t.Context(), finished)
			if err != nil {
				t.Fatal(err)
			}
			terminal, err := fixture.queue.PrepareTaskFinish(
				t.Context(),
				finished.Task,
				at.Add(time.Second),
				at.Add(24*time.Hour),
			)
			if err != nil {
				t.Fatal(err)
			}
			fixture.commit(t, proof, terminal)
			saved, _, err := fixture.queue.ReadTask(t.Context(), fixture.task.ID, 0)
			if err != nil || saved.Task.FinishedAt == nil || saved.Task.Outcome != outcome {
				t.Fatalf("visible prune outcome: %v/%v", saved, err)
			}
		})
	}
}

func (fixture nativePruneFixture) absence(
	t *testing.T,
	view workqueue.ResumeView,
	ordinal uint32,
) workqueue.ResumeView {
	t.Helper()
	assignment, _, err := fixture.owner.LoadAssignment(t.Context(), view)
	if err != nil {
		t.Fatal(err)
	}
	object := assignment.Input.Steps[ordinal].GetPrune().Objects[0]
	report := &agentpb.BackupWorkCheckpointRequest{
		Attempt:            proto.CloneOf(assignment.Attempt),
		SourceOrdinal:      ordinal,
		CheckpointSequence: 1,
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_PruneObjectDeleted{
			PruneObjectDeleted: &agentpb.BackupPruneObjectDeleted{
				Ordinal: object.Ordinal, PointId: object.PointId, Object: proto.CloneOf(object.Object)},
		},
	}
	before := fixture.store.revision
	changed := proto.CloneOf(report)
	changed.GetPruneObjectDeleted().Object.GetVersionId().Value = "different-version"
	invalid, err := fixture.owner.InspectCheckpoint(t.Context(), view, changed)
	if err == nil {
		_, err = fixture.owner.PreparePruneSettlement(
			t.Context(),
			view,
			invalid,
			fixture.task.CreatedAt.Add(3*time.Second),
		)
	}
	if err == nil {
		t.Fatal("wrong object version accepted")
	}
	if fixture.store.revision != before {
		t.Fatal("invalid absence report wrote state")
	}
	admitted, err := fixture.owner.InspectCheckpoint(t.Context(), view, report)
	if err != nil {
		t.Fatal(err)
	}
	at := fixture.task.CreatedAt.Add(time.Duration(3+ordinal) * time.Second)
	resources, err := fixture.owner.PreparePruneSettlement(t.Context(), view, admitted, at)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, replay, err := fixture.owner.PrepareCheckpoint(
		t.Context(),
		view,
		report,
		backupwork.CheckpointEvidence{},
		at,
	)
	if err != nil || replay != nil {
		t.Fatalf("prune checkpoint: %v/%v", replay, err)
	}
	commit := fixture.commit(t, resources, checkpoint)
	for _, key := range []string{backupruntime.BackupRecoveryPointPruneKey(object.PointId),
		backupwork.CheckpointKey(fixture.item.ID, ordinal, 1), backupwork.ProgressKey(fixture.item.ID)} {
		entry := mustOptionalKey(t, fixture.store, key)
		if entry == nil || entry.ModRevision != commit.Revision {
			t.Fatalf("absence not joined with checkpoint: %s", key)
		}
	}
	if mustOptionalKey(t, fixture.store, backupruntime.BackupRecoveryPointKey(object.PointId)) != nil {
		t.Fatal("absence retained Point primary")
	}
	ack, err := fixture.owner.ReadCheckpointAcknowledgement(
		t.Context(),
		report,
		fixture.input,
		view.Attempt.Attempt.Executor,
		commit.Revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	view = fixture.resume(t, view)
	revision := fixture.store.revision
	duplicate, err := fixture.owner.InspectCheckpoint(t.Context(), view, report)
	if err != nil || !proto.Equal(duplicate.Replay, ack) || fixture.store.revision != revision {
		t.Fatalf("lost acknowledgement replay: %v", err)
	}
	if _, err := fixture.owner.InspectCheckpoint(t.Context(), view, changed); err == nil {
		t.Fatal("changed duplicate accepted")
	}
	return view
}

// TASK-156: automatic retention rejects Cancel; expiry of never-assigned work
// releases its selection without pretending a remote deletion ran.
func TestNativeBackupPruneDeadlineBeforeAssignment(t *testing.T) {
	fixture := newNativePruneFixture(t, backupruntime.MaximumBackupPruneDispatchPoints)
	view, err := fixture.queue.ReadWork(t.Context(), fixture.task.ID, fixture.item.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	at := fixture.task.CreatedAt.Add(time.Second)
	if _, err := workqueue.PrepareStop(view.Task, workexecution.Cancelled, at); err == nil {
		t.Fatal("automatic retention accepted operator Cancel")
	}
	settlement, err := fixture.owner.PrepareUnstartedStop(t.Context(), view, fixture.task.Deadline.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	fixture.commit(t, settlement)
	finished, err := fixture.queue.ReadFinishedWork(t.Context(), fixture.task.ID)
	if err != nil || finished.Items[0].Item.Outcome != workexecution.Failed {
		t.Fatalf("unstarted prune stop: %v/%v", finished, err)
	}
	for _, point := range fixture.points {
		if mustOptionalKey(t, fixture.store, backupruntime.BackupRecoveryPointKey(point.ID)) == nil {
			t.Fatal("unstarted stop deleted Point")
		}
		prune := mustOptionalKey(t, fixture.store, backupruntime.BackupRecoveryPointPruneKey(point.ID))
		if prune == nil {
			t.Fatal("unstarted stop lost pending cleanup")
		}
		record, err := backupruntime.DecodeBackupRecoveryPointPruneRecord(prune.Value)
		if err != nil || record.State != backupruntime.BackupPrunePending || record.TaskID != "" {
			t.Fatalf("unstarted selection: %v/%v", record, err)
		}
	}
}
