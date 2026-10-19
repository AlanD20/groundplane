package app

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionfinalization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// TASK-130: native Backup never writes the retired generic terminal receipt.
// Detachment must instead require its native terminal Task and compare that
// authority, so a concurrent Retry cannot lose Environment membership.
// Task/run outcomes are supplied: this does not qualify physical Backup effects.
func TestNativeBackupHistoryDetachmentUsesTerminalAuthority(t *testing.T) {
	for _, scenario := range []string{"settled", "unfinished", "foreign scope", "changed after preparation"} {
		t.Run(scenario, func(t *testing.T) {
			f, task, action, membership := nativeBackupHistoryFixture(t)
			if scenario == "unfinished" {
				task.FinishedAt, task.RetainUntil, task.Outcome = nil, nil, ""
				task.Progress = workexecution.Progress{Ready: 1}
			}
			if scenario == "foreign scope" {
				task.Scope.EnvironmentID = ids.New(ids.KindEnvironment)
			}
			raw, err := workqueue.EncodeTask(task)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.Put(t.Context(), workqueue.TaskKey(task.ID), raw); err != nil {
				t.Fatal(err)
			}
			preparer := hierarchydeletionfinalization.NewPreparer(f.store)
			effects, err := preparer.Prepare(t.Context(), hierarchydeletion.HierarchyDeletionOperation{}, action)
			if scenario == "unfinished" || scenario == "foreign scope" {
				if err == nil {
					t.Fatal("unsettled or foreign execution authorized history detachment")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer keyvalue.ClearMutationValues(effects.Mutations())
			defer func() {
				for _, value := range effects.Values() {
					clear(value)
				}
			}()
			if scenario == "changed after preparation" {
				if _, err := f.store.Put(t.Context(), workqueue.TaskKey(task.ID), raw); err != nil {
					t.Fatal(err)
				}
			}
			result, err := f.store.Transact(t.Context(), effects.Conditions(), effects.Mutations())
			want := scenario == "settled"
			if err != nil || result.Succeeded != want {
				t.Fatalf("native detachment committed=%t, want %t: %v", result.Succeeded, want, err)
			}
			member, err := f.store.Get(t.Context(), membership)
			if err != nil || (member.Entry == nil) != want {
				t.Fatalf("detachment lost or retained the wrong membership: %v", err)
			}
			for _, key := range []string{workqueue.TaskKey(task.ID), backupruntime.BackupRunKey(task.ID)} {
				retained, err := f.store.Get(t.Context(), key)
				if err != nil || retained.Entry == nil {
					t.Fatalf("detachment erased retained execution/resource history: %v", err)
				}
			}
		})
	}
}

// TASK-130: retention cannot confuse a damaged membership with completed parent
// removal. Both absence checks must join collection; recreating either key
// after preparation must invalidate the transaction.
func TestNativeBackupHistoryRetentionFencesParentAndMembership(t *testing.T) {
	for _, kind := range []backupruntime.BackupOperationKind{backupruntime.BackupOperationBackup,
		backupruntime.BackupOperationRestore, backupruntime.BackupOperationRotation} {
		t.Run(string(kind), func(t *testing.T) {
			f, task, _, backupMembership := nativeBackupHistoryFixture(t)
			membership := backupMembership
			var err error
			if kind == backupruntime.BackupOperationRestore {
				membership, err = backupruntime.BackupRestoreEnvironmentIndexKey(f.environmentID, task.ID)
			} else if kind == backupruntime.BackupOperationRotation {
				membership, err = backupruntime.BackupKeyRotationEnvironmentIndexKey(f.environmentID, task.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			if kind != backupruntime.BackupOperationBackup {
				if _, err := f.store.Put(t.Context(), membership, []byte(task.ID)); err != nil {
					t.Fatal(err)
				}
			}
			guards := func() ([]keyvalue.Condition, error) {
				return backupruntime.HistoryMembershipRetentionGuards(t.Context(), f.store, kind,
					f.environmentID, task.ID, f.store.revision)
			}
			if _, err := guards(); err != nil {
				t.Fatalf("intact native membership cannot be collected: %v", err)
			}
			if _, err := f.store.Delete(t.Context(), membership); err != nil {
				t.Fatal(err)
			}
			if _, err := guards(); err == nil {
				t.Fatal("retention ignored missing membership under an existing Environment")
			}
			parent, err := f.store.Get(t.Context(), hierarchy.EnvironmentKey(f.environmentID))
			if err != nil || parent.Entry == nil {
				t.Fatalf("parent fixture unavailable: %v", err)
			}
			if _, err := f.store.Delete(t.Context(), parent.Entry.Key); err != nil {
				t.Fatal(err)
			}
			conditions, err := guards()
			if err != nil {
				t.Fatalf("completed parent removal stranded retained history: %v", err)
			}
			for _, replacement := range []keyvalue.Mutation{
				{Type: keyvalue.MutationPut, Key: parent.Entry.Key, Value: parent.Entry.Value},
				{Type: keyvalue.MutationPut, Key: membership, Value: []byte(task.ID)},
			} {
				if _, err := f.store.Transact(t.Context(), nil, []keyvalue.Mutation{replacement}); err != nil {
					t.Fatal(err)
				}
				result, err := f.store.Transact(t.Context(), conditions, []keyvalue.Mutation{
					{Type: keyvalue.MutationDelete, Key: backupruntime.BackupRunKey(task.ID)},
				})
				if err != nil || result.Succeeded {
					t.Fatalf("history collection crossed a changed parent/membership: %v", err)
				}
				if _, err := guards(); err == nil {
					t.Fatal("one-sided parent/membership resurrection was accepted")
				}
				if _, err := f.store.Delete(t.Context(), replacement.Key); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func nativeBackupHistoryFixture(t *testing.T) (*nativeVolumeFixture, workexecution.Task,
	hierarchydeletion.HierarchyDeletionAction, string,
) {
	t.Helper()
	f := newNativeVolumeFixture(t)
	accepted, _, err := f.queue.ReadTask(t.Context(), f.created.TaskID, 0)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(time.Millisecond).Add(-time.Minute)
	finished := at.Add(time.Second)
	retainedUntil := finished.Add(workexecution.TaskHistoryRetention)
	task := workexecution.Task{
		ID:                  ids.New(ids.KindTask),
		Actor:               workexecution.OperatorActor,
		Scope:               accepted.Task.Scope,
		Action:              workexecution.ActionBackup,
		TargetID:            f.environmentID,
		Intent:              workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)},
		ExpectedItems:       1,
		PublicationComplete: true,
		Progress:            workexecution.Progress{Failed: 1},
		Outcome:             workexecution.Failed,
		CreatedAt:           at,
		UpdatedAt:           finished,
		Deadline:            at.Add(time.Hour),
		FinishedAt:          &finished,
		RetainUntil:         &retainedUntil,
	}
	source, point := ids.New(ids.KindBackupSource), ids.NewAt(ids.KindRecoveryPoint, at, 1)
	run := backupruntime.BackupRunRecord{TaskID: task.ID, OperationID: ids.New(ids.KindOperation),
		EnvironmentID: f.environmentID, PolicyRevision: 1, PolicySHA256: strings.Repeat("b", 64), RetentionKeep: 3,
		Initiator: backupruntime.BackupRunInitiatorOperator, ConnectorID: ids.New(ids.KindConnector), ConnectorRevision: 1,
		Encryption: backupruntime.BackupRuntimeEncryptionNone, State: backupruntime.BackupRunFailed,
		CreatedAt: at, UpdatedAt: finished,
		Sources: []backupruntime.BackupRunSourceAttemptRecord{
			{SourceID: source, Kind: backupruntime.BackupRuntimeSourceVolume,
				TargetID: f.created.Volume.ID, SourceRevision: 1, TargetRevision: 1,
				Snapshot: backupruntime.BackupRunSourceSnapshot{Volume: &backupruntime.BackupVolumeSourceSnapshot{
					EnvironmentID: f.environmentID, EnvironmentRevision: 1, VolumeID: f.created.Volume.ID,
					DesiredRevisionID: task.ID, HeadRevision: 1, HeadSHA256: strings.Repeat("c", 64), ProjectionRoot: 1,
					DependencyDigest: strings.Repeat("d", 64), RenderGeneration: 1, ComposeVolumeKey: "data",
					DockerVolumeName: "gp_vol_" + strings.ToLower(
						f.created.Volume.ID,
					), AuthorizedVolumeDir: "/var/lib/groundplane/vol/qa",
				}}, Format: backupruntime.BackupRuntimeFormatVolume, RecoveryPointID: point, RecoveryPointCreatedAt: at,
				ObjectKey: f.environmentID + "/" + source + "/" + point + "/artifact.bin",
				State:     backupruntime.BackupSourceAttemptFailed, Phase: backupruntime.BackupSourcePhaseCapture,
				FailureCode: backupruntime.BackupFailureCapture},
		}}
	raw, err := backupruntime.EncodeBackupRunRecord(run)
	if err != nil {
		t.Fatal(err)
	}
	primary := backupruntime.BackupRunKey(task.ID)
	revision, err := f.store.Put(t.Context(), primary, raw)
	if err != nil {
		t.Fatal(err)
	}
	clear(raw)
	membership, err := backupruntime.BackupRunEnvironmentIndexKey(f.environmentID, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Put(t.Context(), membership, []byte(task.ID)); err != nil {
		t.Fatal(err)
	}
	return f, task, hierarchydeletion.HierarchyDeletionAction{
		ActionKind: hierarchydeletion.HierarchyDeletionBackupRunDetach, TargetID: task.ID, TargetRevision: revision,
	}, membership
}
