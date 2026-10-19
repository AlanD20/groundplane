package etcd

import (
	context "context"
	errors "errors"
	testing "testing"

	testbackupconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	errs "github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: run publication must compare source revisions and Environment
// operation authority before writing. These checks do not execute a Backup.
func TestBackupRuntimeRepositoryRejectsWrongLockAndStaleEpochWithoutWrites(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		run  func(*testing.T, *memoryHierarchyStore, testbackupruntime.BackupRunRecord)
	}{
		{name: "wrong lock", run: func(t *testing.T, store *memoryHierarchyStore, run testbackupruntime.BackupRunRecord) {
			lock := testbackupruntime.BackupOperationLockRecord{
				EnvironmentID: run.EnvironmentID, OperationID: testBackupOperationID,
				TaskID: testBackupRetryTaskID, Kind: testbackupruntime.BackupOperationBackup,
				CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
			}
			putBackupRuntimeLock(t, store, lock)
		}},
		{name: "stale epoch", run: func(_ *testing.T, _ *memoryHierarchyStore, _ testbackupruntime.BackupRunRecord) {
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, store, run := newBackupRuntimeBareFixture(t)
			test.run(t, store, run)
			var boundary hierarchyStore = store
			if test.name == "stale epoch" {
				racing := &entryVolumeEpochRaceStore{hierarchyStore: store}
				racing.beforeTransact = func() {
					advanceEnvironmentMutationFenceEpoch(t, store, run.EnvironmentID)
				}
				boundary = racing
			}
			repository, err := newBackupRuntimeRepository(boundary)
			if err != nil {
				t.Fatal(err)
			}
			_, err = repository.createBackupRunForTest(
				context.Background(),
				run,
				backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
			)
			if test.name == "wrong lock" && !isKind(err, errs.KindResourceInUse) {
				t.Fatalf("CreateBackupRun(wrong lock) error = %v", err)
			}
			if test.name == "stale epoch" && !isKind(err, errs.KindStateConflict) {
				t.Fatalf("CreateBackupRun(stale epoch) error = %v", err)
			}
			stored, getErr := store.Get(context.Background(), testbackupruntime.BackupRunKey(run.TaskID))
			if getErr != nil || stored.Entry != nil {
				t.Fatalf("failed run primary = %#v/%v", stored, getErr)
			}
		})
	}
}

// Rationale: a caller-selected fixed revision must bind source construction;
// advancing one exact source primary before CAS cannot publish a fabricated run.
func TestBackupRuntimeRepositoryRunPublicationRejectsChangedSourceEvidence(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	fixedRevision := backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID)
	entry := mustOptionalKey(t, store, testbackuppolicy.BackupSourceKey(run.Sources[0].SourceID))
	result, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: entry.Key, ModRevision: entry.ModRevision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: entry.Key, Value: entry.Value}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("advance backup source = %#v, %v", result, err)
	}
	if _, err := repository.createBackupRunForTest(
		context.Background(), run, fixedRevision,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("CreateBackupRun(changed source) error = %v", err)
	}
	if entry := mustOptionalKey(t, store, testbackupruntime.BackupRunKey(run.TaskID)); entry != nil {
		t.Fatalf("run published with changed source = %#v", entry)
	}
}

// Rationale: config capture authority is created with the run and pins the
// original fixed Entry read revision rather than reconstructing it on retry.
func TestBackupRuntimeRepositoryPublishesPinnedConfigSnapshotAtomically(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	source := &run.Sources[0]
	source.Kind = testbackupruntime.BackupRuntimeSourceConfig
	source.TargetID = run.EnvironmentID
	source.TargetRevision = mustOptionalKey(t, store, testhierarchy.EnvironmentKey(run.EnvironmentID)).ModRevision
	source.Format = testbackupruntime.BackupRuntimeFormatConfig
	source.Snapshot = testbackupruntime.BackupRunSourceSnapshot{Config: &testbackupruntime.BackupConfigSourceSnapshot{
		ConfigSnapshotID: run.TaskID,
	}}
	policyValue, err := testbackuppolicy.EncodeBackupPolicyRecord(testbackuppolicy.BackupPolicyRecord{
		EnvironmentID: run.EnvironmentID, Enabled: true, Frequency: "0 2 * * *", Keep: 3,
		Encryption: string(run.Encryption), ConnectorID: run.ConnectorID,
		SourceIDs: []string{source.SourceID}, UpdatedAt: run.CreatedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceValue, err := testbackuppolicy.EncodeBackupSourceRecord(backupRuntimeSourceRecord(
		t, source.SourceID, run.EnvironmentID, "config", run.EnvironmentID, run.CreatedAt,
	))
	if err != nil {
		clear(policyValue)
		t.Fatal(err)
	}
	result, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testbackuppolicy.BackupPolicyKey(run.EnvironmentID), Value: policyValue},
		{Type: testkeyvalue.MutationPut, Key: testbackuppolicy.BackupSourceKey(source.SourceID), Value: sourceValue},
	})
	clear(policyValue)
	clear(sourceValue)
	if err != nil || !result.Succeeded {
		t.Fatalf("replace config publication evidence = %#v, %v", result, err)
	}
	run.PolicyRevision = result.Revision
	source.SourceRevision = result.Revision
	fixedRevision := backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID)
	source.Snapshot.Config.ReadRevision = fixedRevision
	created, err := repository.createBackupRunForTest(context.Background(), run, fixedRevision)
	if err != nil {
		t.Fatalf("CreateBackupRun(config) error = %v", err)
	}
	for _, key := range []string{testbackupruntime.BackupRunKey(run.TaskID), testbackupconfiguration.BackupConfigSnapshotKey(run.TaskID), testbackupconfiguration.BackupConfigSnapshotTaskReferenceKey(run.TaskID, run.TaskID), testbackupconfiguration.BackupConfigSnapshotReferenceTaskKey(run.TaskID, run.TaskID)} {
		entry := mustOptionalKey(t, store, key)
		if entry == nil || entry.ModRevision != created.Revision {
			t.Fatalf("config publication companion %q = %#v", key, entry)
		}
	}
	entry := mustOptionalKey(t, store, testbackupconfiguration.BackupConfigSnapshotKey(run.TaskID))
	snapshot, err := testbackupconfiguration.DecodeBackupConfigSnapshotRecord(entry.Value)
	if err != nil || snapshot.ReadRevision != fixedRevision ||
		snapshot.State != testbackupconfiguration.BackupConfigSnapshotBuilding {
		t.Fatalf("config publication snapshot = %#v, %v", snapshot, err)
	}
}

// Rationale: accepted run metadata and its exclusion must remain readable
// without inferring a completed capture or manufacturing Agent checkpoints.
func TestBackupRunStoragePublicationKeepsSourceExclusion(t *testing.T) {
	t.Parallel()
	repository, _, run := newBackupRuntimeRepositoryFixture(t)
	created, err := repository.createBackupRunForTest(
		context.Background(),
		run,
		backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatalf("CreateBackupRun() error = %v", err)
	}
	if created.Record.State != testbackupruntime.BackupRunQueued || created.Revision <= 0 {
		t.Fatalf("CreateBackupRun() = %#v", created)
	}
	read, err := repository.GetBackupRun(context.Background(), run.TaskID)
	if err != nil || read.Revision != created.Revision {
		t.Fatalf("GetBackupRun() = %#v, %v", read, err)
	}
	page, err := repository.ListBackupRunsByEnvironment(
		context.Background(),
		run.EnvironmentID, testbackupruntime.BackupRuntimeListRequest{Limit: 1},
	)
	if err != nil || len(page.Items) != 1 || page.Items[0].Record.TaskID != run.TaskID {
		t.Fatalf("ListBackupRunsByEnvironment() = %#v, %v", page, err)
	}
	if _, found, err := repository.GetBackupSourceTargetExclusion(
		context.Background(), testbackupruntime.BackupSourceTargetAttach, run.Sources[0].TargetID,
	); err != nil || !found {
		t.Fatalf("GetBackupSourceTargetExclusion() = %v/%v", found, err)
	}
}
