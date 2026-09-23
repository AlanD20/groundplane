package etcd

import (
	context "context"
	testing "testing"
	time "time"

	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	errs "github.com/AlanD20/groundplane/pkg/errs"
)

func seedBackupRuntimePointAuthority(
	t *testing.T,
	store *memoryHierarchyStore,
	point testbackupruntime.BackupRecoveryPointRecord,
) int64 {
	t.Helper()
	value, err := testbackupruntime.EncodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	environmentIndex, err := testbackupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	sourceIndex, err := testbackupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	connectorIndex, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Transact(context.Background(), []testkeyvalue.Condition{
		{Key: testbackupruntime.BackupRecoveryPointKey(point.ID)}, {Key: environmentIndex},
		{Key: sourceIndex}, {Key: connectorIndex},
	}, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointKey(point.ID), Value: value},
		{Type: testkeyvalue.MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
	})
	if err != nil || !result.Succeeded {
		t.Fatalf("seed Recovery Point authority = %#v, %v", result, err)
	}
	return result.Revision
}

type backupRuntimeAuthorityAuditStore struct {
	hierarchyStore
	maximumKeys       int
	revisions         []int64
	truncateNextChunk bool
}

func (store *backupRuntimeAuthorityAuditStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	if len(request.Keys) > store.maximumKeys {
		store.maximumKeys = len(request.Keys)
	}
	store.revisions = append(store.revisions, request.Revision)
	result, err := store.hierarchyStore.GetMany(ctx, request)
	if err == nil && result != nil && store.truncateNextChunk && request.Revision > 0 {
		store.truncateNextChunk = false
		if len(result.Values) > 0 {
			result.Values = result.Values[:len(result.Values)-1]
		}
	}
	return result, err
}

type backupRuntimeUnknownOutcomeStore struct {
	hierarchyStore
	failNext bool
}

func (store *backupRuntimeUnknownOutcomeStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	result, err := store.hierarchyStore.Transact(ctx, conditions, mutations)
	if err == nil && result.Succeeded && store.failNext {
		store.failNext = false
		return testkeyvalue.TransactionResult{}, errs.New(
			errs.KindStorageUnavailable,
			"unknown backup runtime outcome",
		)
	}
	return result, err
}

func newBackupRuntimeID(kind ids.Kind, at time.Time, seed int64) string {
	return ids.NewAt(kind, at, seed)
}

func backupRuntimeSourceRecord(
	t *testing.T,
	sourceID string,
	environmentID string,
	kind string,
	targetID string,
	createdAt time.Time,
) testbackuppolicy.BackupSourceRecord {
	t.Helper()
	value, err := testrecordcodec.Encode("backup-source", map[string]any{
		"id": sourceID, "environment_id": environmentID, "kind": kind,
		"target_id": targetID, "created_at": createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	record, err := testbackuppolicy.DecodeBackupSourceRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func putBackupRuntimeLock(
	t *testing.T,
	store *memoryHierarchyStore,
	lock testbackupruntime.BackupOperationLockRecord,
) {
	t.Helper()
	value, err := testbackupruntime.EncodeBackupOperationLockRecord(lock)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	result, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: testhierarchy.EnvironmentOperationLockKey(lock.EnvironmentID)}},
		[]testkeyvalue.Mutation{
			{
				Type:  testkeyvalue.MutationPut,
				Key:   testhierarchy.EnvironmentOperationLockKey(lock.EnvironmentID),
				Value: value,
			},
		},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("put backup runtime lock = %#v/%v", result, err)
	}
}

func backupRuntimeTestPoint(
	run testbackupruntime.BackupRunRecord,
	source testbackupruntime.BackupRunSourceAttemptRecord,
	verifiedAt time.Time,
) testbackupruntime.BackupRecoveryPointRecord {
	return testbackupruntime.BackupRecoveryPointRecord{
		BackupRecoveryPointSnapshot: testbackupruntime.BackupRecoveryPointSnapshot{
			ID: source.RecoveryPointID, EnvironmentID: run.EnvironmentID,
			SourceID: source.SourceID, SourceKind: source.Kind, TargetID: source.TargetID,
			ConnectorID: run.ConnectorID, ConnectorPrefix: run.ConnectorPrefix,
			ObjectKey: source.ObjectKey, SourceFormat: source.Format,
			Encryption: run.Encryption, KeyEra: run.KeyEra, Recipient: run.Recipient,
			SizeBytes: source.SizeBytes, SHA256: source.SHA256,
			CreatedAt: source.RecoveryPointCreatedAt,
		},
		VerifiedAt: verifiedAt,
	}
}

func createBackupRuntimeOrphanForReadTest(
	t *testing.T,
) (*BackupRuntimeRepository, *memoryHierarchyStore, testbackupruntime.BackupOrphanRecord) {
	t.Helper()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
	orphaned := staged
	orphaned.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), staged.Sources...)
	orphaned.Sources[0].State = testbackupruntime.BackupSourceAttemptOrphaned
	orphaned.Sources[0].Phase = testbackupruntime.BackupSourcePhasePointCommit
	orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
	orphan := testbackupruntime.BackupOrphanRecord{
		Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: testbackupruntime.BackupOrphanInspect,
		CreatedAt: orphaned.UpdatedAt, UpdatedAt: orphaned.UpdatedAt,
	}
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload.Kind = testbackupruntime.BackupCheckpointUploadVerified
	if _, err := repository.CreateBackupOrphan(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint),
		stagedVersion, orphaned, 0, orphan,
	); err != nil {
		t.Fatalf("CreateBackupOrphan() error = %v", err)
	}
	return repository, store, orphan
}

func (repository *BackupRuntimeRepository) replaceBackupRunForTest(
	ctx context.Context,
	current testkeyvalue.Versioned[testbackupruntime.BackupRunRecord],
	next testbackupruntime.BackupRunRecord,
) (testkeyvalue.Versioned[testbackupruntime.BackupRunRecord], error) {
	value, err := testbackupruntime.EncodeBackupRunRecord(next)
	if err != nil {
		return testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{}, err
	}
	defer clear(value)
	anchor, err := repository.ReadCurrentKeys(ctx, []string{testbackupruntime.BackupRunKey(current.Record.TaskID)})
	if err != nil {
		return testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{}, err
	}
	defer testkeyvalue.ClearValues(anchor.Values)
	fence, err := environmentfence.LoadOwned(ctx, repository.store, current.Record.EnvironmentID,
		anchor.ReadRevision, environmentfence.Owner{
			Kind:        testbackupruntime.BackupOperationBackup,
			OperationID: current.Record.OperationID, TaskID: current.Record.TaskID,
		})
	if err != nil {
		return testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{}, err
	}
	conditions := []testkeyvalue.Condition{
		{Key: testbackupruntime.BackupRunKey(current.Record.TaskID), ModRevision: current.Revision},
	}
	conditions = append(conditions, fence.TransactionConditions()...)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{}, err
	}
	defer clear(epoch.Value)
	result, err := repository.TransactRuntime(ctx, conditions, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRunKey(next.TaskID), Value: value}, epoch,
	})
	if err != nil || !result.Succeeded {
		return testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{}, err
	}
	return testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{
		Record:       next,
		Revision:     result.Revision,
		ReadRevision: result.Revision,
	}, nil
}
