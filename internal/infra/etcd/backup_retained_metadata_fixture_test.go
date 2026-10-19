package etcd

import (
	context "context"
	hex "encoding/hex"
	testing "testing"
	time "time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type backupRuntimeAuthorityAuditStore struct {
	hierarchyStore
	maximumKeys       int
	revisions         []int64
	truncateNextChunk bool
}

// This starts at retained resource metadata, not an executed Backup. Native
// cleanup proof is supplied separately by reconciliation fixtures below.
func createBackupRuntimeOrphanForReadTest(
	t *testing.T,
) (*BackupRuntimeRepository, *memoryHierarchyStore, testbackupruntime.BackupOrphanRecord) {
	t.Helper()
	repository, store, run := newBackupRuntimeBareFixture(t)
	run.State = testbackupruntime.BackupRunRunning
	run.Sources[0].State = testbackupruntime.BackupSourceAttemptOrphaned
	run.Sources[0].Phase = testbackupruntime.BackupSourcePhasePointCommit
	backupRuntimeCompleteSourceArtifact(run, &run.Sources[0])
	run.UpdatedAt = run.CreatedAt.Add(time.Second)
	orphan := testbackupruntime.BackupOrphanRecordFromRun(run, 0)
	raw, err := testbackupruntime.EncodeBackupOrphanRecord(orphan)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(raw)
	connectorIndex, err := testbackupruntime.BackupOrphanConnectorIndexKey(orphan.Target.ConnectorID, orphan.Target.ID)
	if err != nil {
		t.Fatal(err)
	}
	environmentIndex, err := testbackupruntime.BackupOrphanEnvironmentIndexKey(
		orphan.Target.EnvironmentID,
		orphan.Target.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	key := testbackupruntime.BackupOrphanKey(orphan.Target.ID)
	commit, err := store.Transact(
		t.Context(),
		[]testkeyvalue.Condition{{Key: key}, {Key: connectorIndex}, {Key: environmentIndex}},
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationPut, Key: key, Value: raw},
			{Type: testkeyvalue.MutationPut, Key: connectorIndex, Value: []byte(orphan.Target.ID)},
			{Type: testkeyvalue.MutationPut, Key: environmentIndex, Value: []byte(orphan.Target.ID)}},
	)
	if err != nil || !commit.Succeeded {
		t.Fatalf("seed retained orphan metadata: %v", err)
	}
	return repository, store, orphan
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

func backupRuntimeOrphanPoint(
	orphan testbackupruntime.BackupOrphanRecord,
) testbackupruntime.BackupRecoveryPointSnapshot {
	return testbackupruntime.BackupRecoveryPointSnapshot{
		BackupRecoveryPointTargetSnapshot: orphan.Target,
		Evidence:                          orphan.Evidence, Object: orphan.Object, Postgres: orphan.Postgres,
		PostgresArchive: orphan.PostgresArchive, MySQL: orphan.MySQL, MySQLArchive: orphan.MySQLArchive,
		ConfigArchive: orphan.ConfigArchive, VolumeArchive: orphan.VolumeArchive,
	}
}

func seedBackupOrphanAcknowledgedCleanup(t *testing.T, store *memoryHierarchyStore,
	current testkeyvalue.Versioned[testbackupruntime.BackupOrphanRecord],
) testkeyvalue.Versioned[testbackupruntime.BackupOrphanRecord] {
	t.Helper()
	next := current.Record
	next.UpdatedAt = next.UpdatedAt.Add(time.Millisecond)
	executionID := "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	digest, err := executionplan.BackupSourceStagingKey(next.TaskID, executionID, next.Target.ID)
	if err != nil {
		t.Fatal(err)
	}
	next.CleanupProof = testbackupruntime.BackupOrphanCleanupProof{
		TaskID: next.TaskID, WorkItemID: ids.NewAt(ids.KindWorkItem, next.CreatedAt, 303),
		AttemptID: ids.NewAt(ids.KindAttempt, next.CreatedAt, 304),
		Executor: workexecution.Identity{Executor: workexecution.Agent,
			HostID:  ids.NewAt(ids.KindHost, next.CreatedAt, 305),
			AgentID: ids.NewAt(ids.KindAgent, next.CreatedAt, 306), Generation: 1},
		Input:       workexecution.Input{ID: ids.NewAt(ids.KindPlan, next.CreatedAt, 307), SHA256: testBackupDigest},
		Result:      workexecution.Input{ID: ids.NewAt(ids.KindPlan, next.CreatedAt, 308), SHA256: testBackupDigest},
		ExecutionID: executionID, RecoveryKeySHA256: hex.EncodeToString(digest), RecordedAt: next.UpdatedAt,
	}
	value, err := testbackupruntime.EncodeBackupOrphanRecord(next)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	connectorIndex, err := testbackupruntime.BackupOrphanConnectorIndexKey(next.Target.ConnectorID, next.Target.ID)
	if err != nil {
		t.Fatal(err)
	}
	environmentIndex, err := testbackupruntime.BackupOrphanEnvironmentIndexKey(
		next.Target.EnvironmentID,
		next.Target.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{testbackupruntime.BackupOrphanKey(next.Target.ID), connectorIndex, environmentIndex}
	conditions := make([]testkeyvalue.Condition, len(keys))
	for i, key := range keys {
		conditions[i] = testkeyvalue.Condition{Key: key, ModRevision: current.Revision}
	}
	result, err := store.Transact(context.Background(), conditions, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: keys[0], Value: value},
		{Type: testkeyvalue.MutationPut, Key: keys[1], Value: []byte(next.Target.ID)},
		{Type: testkeyvalue.MutationPut, Key: keys[2], Value: []byte(next.Target.ID)},
	})
	if err != nil || !result.Succeeded {
		t.Fatalf("seed acknowledged orphan cleanup = %#v, %v", result, err)
	}
	return testkeyvalue.Versioned[testbackupruntime.BackupOrphanRecord]{
		Record: next, Revision: result.Revision, ReadRevision: result.Revision,
	}
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
