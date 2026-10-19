package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// QA: BAK-20. A Restore admitted after selection must defeat deletion's
// publication, leaving the Point visible and without a partial tombstone/Task.
// The successful case proves one atomic transition into protected cleanup.
func TestRecoveryPointRemovalPublishesAtomicallyAgainstConcurrentOperation(t *testing.T) {
	for _, racingOperation := range []bool{false, true} {
		t.Run(map[bool]string{false: "publish", true: "concurrent_restore"}[racingOperation], func(t *testing.T) {
			repository, store, run := newBackupRuntimeBareFixture(t)
			queue, err := workqueue.NewRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			enrollment, err := hostbindings.PrepareLocalEnrollment(t.Context(), store, ids.New(ids.KindAgent))
			if err != nil {
				t.Fatal(err)
			}
			commit, err := queue.Commit(t.Context(), enrollment)
			if err != nil || !commit.Succeeded {
				t.Fatalf("enroll prune host = %t, %v", commit.Succeeded, err)
			}
			source := run.Sources[0]
			source.State = backupruntime.BackupSourceAttemptStaged
			source.Phase = backupruntime.BackupSourcePhaseUpload
			backupRuntimeCompleteSourceArtifact(run, &source)
			point := backupRuntimeTestPoint(run, source, run.CreatedAt.Add(time.Second))
			seedBackupRuntimePointAuthority(t, store, point)
			sweep := backupruntime.BackupRetentionSweepRecord{
				SourceID: point.SourceID, TriggerRecoveryPointID: point.ID, Keep: 3,
				Revision: run.PolicyRevision, PolicySHA256: run.PolicySHA256,
				State:     backupruntime.BackupRetentionPending,
				CreatedAt: point.VerifiedAt, UpdatedAt: point.VerifiedAt,
			}
			sweepKey := backupruntime.BackupRetentionKey(point.SourceID, point.ID)
			seed := func(record backupruntime.BackupRetentionSweepRecord) {
				t.Helper()
				value, err := backupruntime.EncodeBackupRetentionSweepRecord(record)
				if err != nil {
					t.Fatal(err)
				}
				defer clear(value)
				result, err := store.Transact(context.Background(), nil,
					[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: sweepKey, Value: value}})
				if err != nil || !result.Succeeded {
					t.Fatalf("seed sweep: %v/%v", result, err)
				}
			}
			seed(sweep)
			sweep.State = backupruntime.BackupRetentionCompleted
			sweep.SelectionRevision = store.revision
			sweep.PruneOperationID = ids.New(ids.KindOperation)
			sweep.UpdatedAt = sweep.UpdatedAt.Add(time.Second)
			seed(sweep)
			at := run.CreatedAt.Add(3 * time.Second)
			prepared, err := repository.PrepareRecoveryPointRemoval(context.Background(), run.EnvironmentID,
				point.ID, ids.New(ids.KindTask), ids.New(ids.KindOperation), at)
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Publication.Clear()
			task, item, input, marker := nativeBackupPrunePublicationFixture(t, prepared)
			if racingOperation {
				lock := backupruntime.BackupOperationLockRecord{
					EnvironmentID: run.EnvironmentID, OperationID: ids.New(ids.KindOperation),
					TaskID: ids.New(ids.KindTask), Kind: backupruntime.BackupOperationRestore,
					CreatedAt: at, UpdatedAt: at,
				}
				value, err := backupruntime.EncodeBackupOperationLockRecord(lock)
				if err != nil {
					t.Fatal(err)
				}
				defer clear(value)
				result, err := store.Transact(context.Background(), nil, []etcdstore.Mutation{{
					Type: etcdstore.MutationPut, Key: hierarchy.EnvironmentOperationLockKey(run.EnvironmentID), Value: value,
				}})
				if err != nil || !result.Succeeded {
					t.Fatalf("seed lock: %v/%v", result, err)
				}
			}
			result, err := prepared.Publication.PublishWork(context.Background(), task, item, input, marker)
			if err != nil {
				t.Fatal(err)
			}
			outcome, _, conflict, err := result.Classify()
			if err != nil {
				t.Fatal(err)
			}
			if racingOperation {
				if conflict == nil || outcome == IdempotencyKnownApplied {
					t.Fatal("racing Restore did not reject deletion")
				}
				if _, err := repository.GetBackupRecoveryPoint(context.Background(), point.ID); err != nil {
					t.Fatal(err)
				}
				for _, key := range []string{backupruntime.BackupRecoveryPointPruneKey(point.ID), workqueue.TaskKey(task.ID), workqueue.ItemKey(item.ID), backupwork.InputKey(item.ID)} {
					if mustOptionalKey(t, store, key) != nil {
						t.Fatalf("partial deletion at %s", key)
					}
				}
			} else {
				if conflict != nil || outcome != IdempotencyKnownApplied {
					t.Fatalf("publication: %v/%v", outcome, conflict)
				}
				if _, err := repository.GetBackupRecoveryPoint(context.Background(), point.ID); !errors.Is(err, errs.New(errs.KindRecoveryPointNotFound, "")) {
					t.Fatalf("selected Point still offered for Restore: %v", err)
				}
				if mustOptionalKey(t, store, backupruntime.BackupRecoveryPointKey(point.ID)) == nil {
					t.Fatal("Point discarded before remote proof")
				}
				prune, err := backupruntime.DecodeBackupRecoveryPointPruneRecord(mustOptionalKey(t, store, backupruntime.BackupRecoveryPointPruneKey(point.ID)).Value)
				if err != nil || prune.TaskID != task.ID || prune.Point.ID != point.ID {
					t.Fatalf("exact assigned selection: %v/%v", prune, err)
				}
				view, err := queue.ReadWork(t.Context(), task.ID, item.ID, 0)
				if err != nil || !view.Task.Task.PublicationComplete || view.Item.Item.Phase != workexecution.Ready || view.Item.Item.AttemptNumber != 0 {
					t.Fatalf("Point removal did not publish ready native work: %#v, %v", view, err)
				}
				for _, key := range []string{backupruntime.BackupRecoveryPointPruneKey(point.ID), backupwork.InputKey(item.ID),
					hierarchy.EnvironmentOperationLockKey(run.EnvironmentID)} {
					entry := mustOptionalKey(t, store, key)
					if entry == nil || entry.ModRevision != view.Task.Revision || view.Item.Revision != view.Task.Revision {
						t.Fatalf("Point removal companion did not join acceptance: %s", key)
					}
				}
				owner, err := backupwork.NewInputRepository(store)
				if err != nil {
					t.Fatal(err)
				}
				captured, _, err := owner.Load(t.Context(), view)
				if err != nil || captured.Steps[0].GetPrune().Objects[0].PointId != point.ID ||
					captured.Steps[0].GetPrune().Objects[0].Object.GetVersionId().Value != point.Object.Discriminator.Value {
					t.Fatalf("Point removal lost the exact object selection: %v", err)
				}
			}
		})
	}
}

// Supplied wire construction exercises the repository publication boundary;
// actual Controller buildPruneWork and remote object effects are separate QA.
func nativeBackupPrunePublicationFixture(
	t *testing.T,
	prepared PreparedBackupPrune,
) (workexecution.Task, workexecution.Item, *agentpb.BackupWorkInput, idempotency.IdempotencyMarker) {
	t.Helper()
	dispatch, scope := prepared.Dispatch, prepared.Scope
	deadline := dispatch.CreatedAt.Add(30 * time.Minute)
	input := &agentpb.BackupWorkInput{Schema: 1, Operation: agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE,
		ProjectId: scope.ProjectId, Project: scope.Project, EnvironmentId: scope.EnvironmentId, Environment: scope.Environment}
	for index, evidence := range prepared.Evidence {
		step, err := executionplan.SealBackupStepAuthority(&agentpb.BackupStepAuthority{
			StepId: ids.New(
				ids.KindStep,
			), ExecutionId: ids.NewULID(), StepDeadlineUnixNano: uint64(deadline.UnixNano()),
			Operation: &agentpb.BackupStepAuthority_Prune{Prune: &agentpb.BackupPruneAuthority{
				RetentionPolicy: proto.CloneOf(
					evidence.RetentionPolicy,
				), Objects: []*agentpb.BackupPruneObject{backupRuntimePruneObject(t, evidence, uint32(index+1))}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		input.Steps = append(input.Steps, step)
	}
	owned, raw, reference, err := executionplan.CaptureBackupWorkInput(ids.New(ids.KindPlan), input)
	clear(raw)
	if err != nil {
		t.Fatal(err)
	}
	task := workexecution.Task{ID: dispatch.TaskID, Actor: workexecution.SystemActor,
		Scope: workexecution.Scope{
			TenantID:      prepared.Owner.TenantID,
			ProjectID:     prepared.Owner.ProjectID,
			EnvironmentID: prepared.Owner.EnvironmentID,
		},
		Action: workexecution.ActionBackupPrune, TargetID: dispatch.EnvironmentID, TargetName: dispatch.EnvironmentID,
		Intent: reference, ExpectedItems: 1, CreatedAt: dispatch.CreatedAt, UpdatedAt: dispatch.CreatedAt, Deadline: deadline}
	item, err := backupwork.NewItem(task, ids.New(ids.KindWorkItem), prepared.HostID, owned)
	if err != nil {
		t.Fatal(err)
	}
	marker := testDirectMarker()
	marker.Kind, marker.State, marker.TaskID = idempotency.IdempotencyMarkerTask, idempotency.IdempotencyMarkerPending, task.ID
	marker.Locator = idempotency.IdempotencyLocator{
		ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID:   dispatch.EnvironmentID,
		Method:    "POST",
		Route:     "/internal/backup-prunes",
		Key:       "native-backup-point-removal",
	}
	marker.CreatedAt, marker.UpdatedAt, marker.TerminalAt, marker.RetainUntil = dispatch.CreatedAt, dispatch.CreatedAt, time.Time{}, time.Time{}
	marker.Response.Status, marker.Response.Body = 202, []byte(`{"task_id":"`+task.ID+`"}`)
	return task, item, owned, marker
}
