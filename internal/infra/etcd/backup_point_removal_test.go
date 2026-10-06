package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// QA: BAK-20. A Restore admitted after selection must defeat deletion's
// publication, leaving the Point visible and without a partial tombstone/Task.
// The successful case proves one atomic transition into protected cleanup.
func TestRecoveryPointRemovalPublishesAtomicallyAgainstConcurrentOperation(t *testing.T) {
	for _, racingOperation := range []bool{false, true} {
		t.Run(map[bool]string{false: "publish", true: "concurrent_restore"}[racingOperation], func(t *testing.T) {
			repository, store, run := newBackupRuntimeBareFixture(t)
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
			pending := []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
				{Record: prepared.Evidence[0].Prune},
			}
			task, sealed, marker, _ := backupRuntimePrunePublicationTask(t, store, prepared.Dispatch,
				pending, prepared.Publication.state.plan.conditions)
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
			result, err := prepared.Publication.Publish(context.Background(), task, sealed, marker)
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
				for _, key := range []string{backupruntime.BackupRecoveryPointPruneKey(point.ID), taskjournal.TaskStorageKey(task.ID)} {
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
			}
		})
	}
}
