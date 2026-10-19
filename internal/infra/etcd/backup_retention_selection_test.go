package etcd

import (
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Supplied post-capture metadata exercises current retention, not capture or
// native checkpoint settlement. The run lock/exclusions use current admission.
func seedBackupRetentionSelection(t *testing.T) (*BackupRuntimeRepository, *memoryHierarchyStore,
	keyvalue.Versioned[backupruntime.BackupRunRecord], backupruntime.BackupRecoveryPointRecord,
	keyvalue.Versioned[backupruntime.BackupRetentionSweepRecord]) {
	t.Helper()
	repository, store, run := newBackupRuntimeBareFixture(t)
	created, err := repository.createBackupRunForTest(
		t.Context(),
		run,
		backupRuntimeCurrentRevision(t, store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	run.State = backupruntime.BackupRunRunning
	run.Sources[0].State = backupruntime.BackupSourceAttemptPointCommitted
	run.Sources[0].Phase = backupruntime.BackupSourcePhaseRetention
	backupRuntimeCompleteSourceArtifact(run, &run.Sources[0])
	run.UpdatedAt = run.CreatedAt.Add(2 * time.Second)
	current, err := repository.replaceBackupRunForTest(t.Context(), created, run)
	if err != nil {
		t.Fatal(err)
	}
	point := backupRuntimeTestPoint(run, run.Sources[0], run.UpdatedAt)
	seedBackupRuntimePointAuthority(t, store, point)
	sweep := backupruntime.BackupRetentionSweepRecord{SourceID: point.SourceID, TriggerRecoveryPointID: point.ID,
		Keep: run.RetentionKeep, Revision: run.PolicyRevision, PolicySHA256: run.PolicySHA256,
		State: backupruntime.BackupRetentionPending, CreatedAt: point.VerifiedAt, UpdatedAt: point.VerifiedAt}
	raw, err := backupruntime.EncodeBackupRetentionSweepRecord(sweep)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(raw)
	key := backupruntime.BackupRetentionKey(point.SourceID, point.ID)
	commit, err := store.Transact(
		t.Context(),
		[]keyvalue.Condition{{Key: key}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: raw}},
	)
	if err != nil || !commit.Succeeded {
		t.Fatalf("seed retention: %v", err)
	}
	return repository, store, current, point, keyvalue.Versioned[backupruntime.BackupRetentionSweepRecord]{
		Record:       sweep,
		Revision:     commit.Revision,
		ReadRevision: commit.Revision,
	}
}

func olderBackupPoint(
	t *testing.T,
	store *memoryHierarchyStore,
	newest backupruntime.BackupRecoveryPointRecord,
	offset int,
) backupruntime.BackupRecoveryPointRecord {
	t.Helper()
	point := newest
	point.CreatedAt = newest.CreatedAt.Add(-time.Duration(offset) * time.Second)
	point.ID = ids.NewAt(ids.KindRecoveryPoint, point.CreatedAt, int64(6100+offset))
	point.ObjectKey = point.ConnectorPrefix + point.EnvironmentID + "/" + point.SourceID + "/" + point.ID + "/artifact.bin"
	point.Object.Target.ObjectKey = point.ObjectKey
	point.VerifiedAt = point.CreatedAt.Add(time.Millisecond)
	seedBackupRuntimePointAuthority(t, store, point)
	return point
}

// TASK-157: keep newest verified Points per source and use one fixed scan even
// when an older Point appears between bounded pages. Never select late writes.
func TestBackupRetentionSelectionKeepsNewestAtFixedRevision(t *testing.T) {
	repository, store, run, newest, sweep := seedBackupRetentionSelection(t)
	for index := 1; index <= 12; index++ {
		olderBackupPoint(t, store, newest, index)
	}
	first, prunes, err := repository.AdvanceBackupRetentionSweep(
		t.Context(),
		run,
		sweep,
		sweep.Record.UpdatedAt.Add(time.Second),
	)
	if err != nil || first.Record.State != backupruntime.BackupRetentionScanning || first.Record.RetainedCount != 3 ||
		len(prunes) != 8 {
		t.Fatalf("first retention page: %v/%d/%v", first, len(prunes), err)
	}
	late := olderBackupPoint(t, store, newest, 20)
	last, remaining, err := repository.AdvanceBackupRetentionSweep(
		t.Context(),
		run,
		first,
		first.Record.UpdatedAt.Add(time.Second),
	)
	if err != nil || last.Record.State != backupruntime.BackupRetentionCompleted || len(remaining) != 2 ||
		last.Record.SelectionRevision != first.Record.SelectionRevision {
		t.Fatalf("fixed retention continuation: %v/%d/%v", last, len(remaining), err)
	}
	if mustOptionalKey(t, store, backupruntime.BackupRecoveryPointPruneKey(late.ID)) != nil {
		t.Fatal("interpage insertion entered pinned selection")
	}
	if _, err := repository.GetBackupRecoveryPoint(t.Context(), newest.ID); err != nil {
		t.Fatal(err)
	}
	for _, prune := range append(prunes, remaining...) {
		if prune.Record.State != backupruntime.BackupPrunePending ||
			prune.Record.OperationID != last.Record.PruneOperationID {
			t.Fatal("retention split its operation identity")
		}
		if _, err := repository.GetBackupRecoveryPoint(t.Context(), prune.Record.Point.ID); !errors.Is(
			err,
			errs.New(errs.KindRecoveryPointNotFound, ""),
		) {
			t.Fatalf("selected Point still offered for Restore: %v", err)
		}
	}
}

// TASK-157: corrupt memberships are not a partial page; malformed authority must
// stop the entire retention decision without a new sweep or prune write.
func TestBackupRetentionAndPointReadsRejectChangedMembership(t *testing.T) {
	for _, kind := range []string{"Connector removed", "Environment rewritten", "source rewritten"} {
		t.Run(kind, func(t *testing.T) {
			repository, store, run, point, sweep := seedBackupRetentionSelection(t)
			var key string
			var err error
			switch kind {
			case "Connector removed":
				key, err = backupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
			case "Environment rewritten":
				key, err = backupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
			case "source rewritten":
				key, err = backupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			entry := mustOptionalKey(t, store, key)
			mutation := keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: entry.Value}
			if kind == "Connector removed" {
				mutation = keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key}
			}
			commit, err := store.Transact(
				t.Context(),
				[]keyvalue.Condition{{Key: key, ModRevision: entry.ModRevision}},
				[]keyvalue.Mutation{mutation},
			)
			if err != nil || !commit.Succeeded {
				t.Fatal(err)
			}
			before := store.revision
			if _, err := repository.ListBackupRecoveryPointsBySource(t.Context(), point.SourceID, backupruntime.BackupRuntimeListRequest{Limit: 1}); !errors.Is(
				err,
				errs.New(errs.KindInternal, ""),
			) {
				t.Fatalf("point page accepted corruption: %v", err)
			}
			if _, _, err := repository.AdvanceBackupRetentionSweep(t.Context(), run, sweep, sweep.Record.UpdatedAt.Add(time.Second)); !errors.Is(
				err,
				errs.New(errs.KindInternal, ""),
			) {
				t.Fatalf("retention accepted corruption: %v", err)
			}
			if store.revision != before {
				t.Fatal("failed retention wrote state")
			}
		})
	}
}

// TASK-157: once the public Keep maximum is reached, additional Points must be
// selected without incrementing/overflowing the retained counter.
func TestBackupRetentionSelectionAtKeepMaximum(t *testing.T) {
	repository, store, run, point, sweep := seedBackupRetentionSelection(t)
	for index := 1; index <= 2; index++ {
		olderBackupPoint(t, store, point, index)
	}
	run.Record.RetentionKeep = backuppolicy.MaximumBackupPolicyKeep
	sweep.Record.Keep = run.Record.RetentionKeep
	sweep.Record.State = backupruntime.BackupRetentionScanning
	sweep.Record.SelectionRevision = store.revision
	sweep.Record.Cursor = ids.NewAt(ids.KindRecoveryPoint, point.CreatedAt.Add(time.Second), 1)
	sweep.Record.RetainedCount = sweep.Record.Keep
	sweep.Record.PruneOperationID = ids.New(ids.KindOperation)
	sweep.Record.UpdatedAt = sweep.Record.UpdatedAt.Add(time.Second)
	runRaw, err := backupruntime.EncodeBackupRunRecord(run.Record)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(runRaw)
	sweepRaw, err := backupruntime.EncodeBackupRetentionSweepRecord(sweep.Record)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(sweepRaw)
	key := backupruntime.BackupRetentionKey(point.SourceID, point.ID)
	commit, err := store.Transact(
		t.Context(),
		[]keyvalue.Condition{
			{Key: backupruntime.BackupRunKey(run.Record.TaskID), ModRevision: run.Revision},
			{Key: key, ModRevision: sweep.Revision},
		},
		[]keyvalue.Mutation{
			{
				Type:  keyvalue.MutationPut,
				Key:   backupruntime.BackupRunKey(run.Record.TaskID),
				Value: runRaw,
			}, {Type: keyvalue.MutationPut, Key: key, Value: sweepRaw}},
	)
	if err != nil || !commit.Succeeded {
		t.Fatal(err)
	}
	run.Revision, run.ReadRevision = commit.Revision, commit.Revision
	sweep.Revision, sweep.ReadRevision = commit.Revision, commit.Revision
	next, prunes, err := repository.AdvanceBackupRetentionSweep(
		t.Context(),
		run,
		sweep,
		sweep.Record.UpdatedAt.Add(time.Second),
	)
	if err != nil || next.Record.State != backupruntime.BackupRetentionCompleted ||
		next.Record.RetainedCount != sweep.Record.Keep ||
		len(prunes) != 3 {
		t.Fatalf("Keep maximum: %v/%d/%v", next, len(prunes), err)
	}
}
