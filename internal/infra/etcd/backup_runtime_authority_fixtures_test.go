package etcd

import (
	context "context"
	hex "encoding/hex"
	testing "testing"
	time "time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func retireBackupTestTerminalDelivery(t *testing.T, tasks *TaskRepository, task testkeyvalue.Versioned[TaskRecord]) {
	t.Helper()
	assignment := task.Record.TerminalAssignment
	planHash, err := hex.DecodeString(task.Record.PlanHash)
	if err != nil {
		t.Fatal(err)
	}
	recoveryRequired := task.Record.Result.ReconciliationRequired
	report := &agentpb.TaskAck{
		TaskId: task.Record.ID, AssignmentId: assignment.AssignmentID,
		AssignmentGeneration: assignment.AssignmentGeneration, PlanHash: planHash,
		ExecutionEpoch: task.Record.Result.ExecutionEpoch, Terminal: taskjournal.TaskTerminalWire(task.Record.Status),
		ExitCode: task.Record.Result.ExitCode,
		Result: &agentpb.TaskAck_BackupResult{
			BackupResult: &agentpb.BackupTaskResult{RecoveryRequired: &recoveryRequired},
		},
	}
	ackDigest, err := executionplan.TaskAcknowledgementSHA256(report)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := taskTerminalDeliveryReceipt(task.Record)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := taskjournal.TaskTerminalReceiptSHA256(stored)
	if err != nil {
		t.Fatal(err)
	}
	receiptDigest, err := hex.DecodeString(digest)
	if err != nil {
		t.Fatal(err)
	}
	process := [16]byte{1}
	receipt := &agentpb.TaskTerminalReceiptAck{
		ProcessGeneration: process[:], TaskId: report.TaskId, AssignmentId: report.AssignmentId,
		AssignmentGeneration: report.AssignmentGeneration, PlanHash: planHash, Terminal: report.Terminal,
		TaskAckSha256: ackDigest, TerminalReceiptSha256: receiptDigest, DurableTaskModRevision: task.Revision,
	}
	if err := tasks.BeginTaskTerminalDelivery(context.Background(), assignment.AgentID, assignment.AgentGeneration,
		receipt, report); err != nil {
		t.Fatal(err)
	}
	applied, err := executionplan.TaskTerminalReceiptApplied(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tasks.ApplyTaskTerminalReceipt(context.Background(), assignment.AgentID, assignment.AgentGeneration,
		applied); err != nil {
		t.Fatal(err)
	}
	if err := tasks.RetireTaskTerminalAssignment(context.Background(), assignment.AgentID, assignment.AgentGeneration,
		&agentpb.TaskTerminalAssignmentRetired{ProcessGeneration: process[:], TaskId: report.TaskId,
			AssignmentId: report.AssignmentId, AssignmentGeneration: report.AssignmentGeneration,
			PlanHash: planHash, Terminal: report.Terminal}); err != nil {
		t.Fatal(err)
	}
}

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
	point := testbackupruntime.BackupRecoveryPointRecord{
		BackupRecoveryPointSnapshot: testbackupruntime.BackupRecoveryPointSnapshot{
			BackupRecoveryPointTargetSnapshot: testbackupruntime.BackupRecoveryPointTargetSnapshot{
				ID: source.RecoveryPointID, EnvironmentID: run.EnvironmentID,
				SourceID: source.SourceID, SourceKind: source.Kind, TargetID: source.TargetID,
				ConnectorID: run.ConnectorID, ConnectorPrefix: run.ConnectorPrefix,
				ConnectorEndpoint: run.ConnectorEndpoint, ConnectorBucket: run.ConnectorBucket,
				ConnectorRegion: run.ConnectorRegion, ConnectorPathStyle: run.ConnectorPathStyle,
				ObjectKey: source.ObjectKey, SourceFormat: source.Format,
				Encryption: run.Encryption, KeyEra: run.KeyEra, Recipient: run.Recipient,
				CreatedAt: source.RecoveryPointCreatedAt,
			},
			Evidence: source.Evidence, Object: source.Object,
			ConfigArchive: source.ConfigArchive, VolumeArchive: source.VolumeArchive,
			PostgresArchive: source.PostgresArchive, MySQLArchive: source.MySQLArchive,
		},
		VerifiedAt: verifiedAt,
	}
	if source.Snapshot.Postgres != nil {
		point.Postgres = testbackupruntime.BackupPostgresPointIdentity{
			Database: source.Snapshot.Postgres.Database, Role: source.Snapshot.Postgres.Role,
			BackingEnvironmentID: source.Snapshot.Postgres.BackingEnvironmentID,
			BackingServiceID:     source.Snapshot.Postgres.BackingServiceID,
			ConsumerServiceID:    source.Snapshot.Postgres.ConsumerServiceID,
		}
	}
	return point
}

func backupRuntimeCompleteSourceArtifact(
	run testbackupruntime.BackupRunRecord,
	source *testbackupruntime.BackupRunSourceAttemptRecord,
) {
	source.Evidence = testBackupArtifact()
	if source.Format == testbackupruntime.BackupRuntimeFormatPostgres {
		source.PostgresArchive = testbackupruntime.BackupPostgresArchiveEvidence{
			PGDumpMajor: 16, AdapterContractVersion: 1, SourceServerVersion: "16.9", BackupToolVersion: "16.9",
		}
	}
	source.Object = testbackupruntime.BackupObjectIdentity{
		Target: testbackupruntime.BackupObjectTarget{
			ConnectorID: run.ConnectorID, ConnectorPrefix: run.ConnectorPrefix,
			ConnectorEndpoint: run.ConnectorEndpoint, ConnectorBucket: run.ConnectorBucket,
			ConnectorRegion: run.ConnectorRegion, ConnectorPathStyle: run.ConnectorPathStyle,
			ObjectKey: source.ObjectKey,
		},
		Discriminator: testBackupObject(source.ObjectKey).Discriminator,
	}
	source.Upload = testbackupruntime.BackupUploadOutcome{
		Kind:   testbackupruntime.BackupUploadReturned,
		Target: source.Object.Target, ReturnedObject: source.Object,
	}
}

func backupRuntimeSetSourceArtifact(
	run testbackupruntime.BackupRunRecord,
	source *testbackupruntime.BackupRunSourceAttemptRecord,
) {
	backupRuntimeCompleteSourceArtifact(run, source)
	switch source.Phase {
	case testbackupruntime.BackupSourcePhaseStaging, testbackupruntime.BackupSourcePhaseUpload:
		source.Upload = testbackupruntime.BackupUploadOutcome{
			Kind: testbackupruntime.BackupUploadPrepared, Target: source.Object.Target,
		}
		source.Object = testbackupruntime.BackupObjectIdentity{}
	case testbackupruntime.BackupSourcePhaseHeadVerification:
		source.Object = testbackupruntime.BackupObjectIdentity{}
	}
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

func backupCheckpointEvidence(
	t *testing.T,
	evidence testbackupruntime.BackupArtifactEvidence,
) *agentpb.BackupArtifactEvidence {
	t.Helper()
	source, err := hex.DecodeString(evidence.SourceSHA256)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := hex.DecodeString(evidence.StoredSHA256)
	if err != nil {
		t.Fatal(err)
	}
	return &agentpb.BackupArtifactEvidence{
		SourceSizeBytes: evidence.SourceSizeBytes, SourceSha256: source,
		StoredSizeBytes: evidence.StoredSizeBytes, StoredSha256: stored,
	}
}

func backupSourceCleanupCheckpoint(
	t *testing.T,
	input testbackupruntime.BackupCheckpointInput,
	pointID string,
	evidence testbackupruntime.BackupArtifactEvidence,
) testbackupruntime.BackupCheckpointInput {
	t.Helper()
	request := proto.CloneOf(input.Request)
	request.CheckpointSequence = input.Sequence
	request.Checkpoint = &agentpb.BackupCheckpointRequest_SourceCleanupCompleted{
		SourceCleanupCompleted: &agentpb.BackupSourceCleanupCompleted{
			PointId: pointID, Evidence: backupCheckpointEvidence(t, evidence),
		},
	}
	input.Request = request
	return input
}

func backupPruneObjectDeletedCheckpoint(
	input testbackupruntime.BackupCheckpointInput,
	sequence uint64,
	ordinal uint32,
	pointID string,
	object *agentpb.BackupObjectIdentity,
) testbackupruntime.BackupCheckpointInput {
	request := proto.CloneOf(input.Request)
	request.CheckpointSequence = sequence
	request.Checkpoint = &agentpb.BackupCheckpointRequest_PruneObjectDeleted{
		PruneObjectDeleted: &agentpb.BackupPruneObjectDeleted{
			Ordinal: ordinal, PointId: pointID, Object: object,
		},
	}
	input.Sequence = sequence
	input.Request = request
	return input
}

func backupUploadCompletedCheckpoint(
	t *testing.T,
	input testbackupruntime.BackupCheckpointInput,
	sequence uint64,
	precedingRevision int64,
	run testbackupruntime.BackupRunRecord,
	source testbackupruntime.BackupRunSourceAttemptRecord,
) testbackupruntime.BackupCheckpointInput {
	t.Helper()
	object := backupCheckpointPruneObject(run, source.RecoveryPointID)
	request := proto.CloneOf(input.Request)
	request.CheckpointSequence = sequence
	request.PrecedingCheckpoint = &agentpb.CheckpointFence{
		AuthorityDigest:      append([]byte(nil), request.AuthorityDigest...),
		DedupeKeyModRevision: precedingRevision,
	}
	request.Checkpoint = &agentpb.BackupCheckpointRequest_UploadCompleted{
		UploadCompleted: &agentpb.BackupUploadCompleted{
			PointId: source.RecoveryPointID, Evidence: backupCheckpointEvidence(t, source.Evidence),
			MetadataCount: 11, MetadataSha256: append([]byte(nil), request.AuthorityDigest...),
			Target: &agentpb.BackupObjectTarget{
				Connector: object.Connector, Bucket: object.Bucket, ObjectKey: object.ObjectKey,
			},
			Outcome: &agentpb.BackupUploadCompleted_ReturnedObject{ReturnedObject: object},
		},
	}
	input.Sequence = sequence
	input.PrecedingCheckpointRevision = precedingRevision
	input.Request = request
	return input
}

func backupUploadVerifiedCheckpoint(
	t *testing.T,
	input testbackupruntime.BackupCheckpointInput,
	sequence uint64,
	precedingRevision int64,
	run testbackupruntime.BackupRunRecord,
	source testbackupruntime.BackupRunSourceAttemptRecord,
) testbackupruntime.BackupCheckpointInput {
	t.Helper()
	request := proto.CloneOf(input.Request)
	request.CheckpointSequence = sequence
	request.PrecedingCheckpoint = &agentpb.CheckpointFence{
		AuthorityDigest:      append([]byte(nil), request.AuthorityDigest...),
		DedupeKeyModRevision: precedingRevision,
	}
	request.Checkpoint = &agentpb.BackupCheckpointRequest_UploadVerified{
		UploadVerified: &agentpb.BackupUploadVerified{
			PointId: source.RecoveryPointID, Evidence: backupCheckpointEvidence(t, source.Evidence),
			MetadataCount: 11, MetadataSha256: append([]byte(nil), request.AuthorityDigest...),
			Object: backupCheckpointPruneObject(run, source.RecoveryPointID),
		},
	}
	input.Sequence = sequence
	input.PrecedingCheckpointRevision = precedingRevision
	input.Request = request
	return input
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
	orphan := testbackupruntime.BackupOrphanRecordFromRun(orphaned, 0)
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	if _, err := repository.CreateBackupOrphan(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint),
		stagedVersion, orphaned, 0, orphan,
	); err != nil {
		t.Fatalf("CreateBackupOrphan() error = %v", err)
	}
	return repository, store, orphan
}

// These reconciliation tests start after acknowledged stage cleanup. This fixture
// seeds that durable boundary; it does not exercise the Agent cleanup handshake.
func seedBackupOrphanAcknowledgedCleanup(t *testing.T, store *memoryHierarchyStore,
	current testkeyvalue.Versioned[testbackupruntime.BackupOrphanRecord],
) testkeyvalue.Versioned[testbackupruntime.BackupOrphanRecord] {
	t.Helper()
	next := current.Record
	next.CleanupProof = testbackupruntime.BackupOrphanCleanupProof{
		TaskID: next.TaskID, TaskRevision: current.Revision,
		StepID:            ids.NewAt(ids.KindStep, next.CreatedAt, 303),
		RecoveryKeySHA256: testBackupDigest,
		AgentID:           ids.NewAt(ids.KindAgent, next.CreatedAt, 304), AgentGeneration: 1,
		DeliveryRevision: current.Revision + 1, DeliverySHA256: testBackupDigest, Disposition: "absent",
	}
	next.UpdatedAt = next.UpdatedAt.Add(time.Millisecond)
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
