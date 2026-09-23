package etcd

import (
	context "context"
	testing "testing"
	time "time"

	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
)

func TestBackupTerminalReceiptPruneCompanionRequiresAtomicRevisionAndOlderPrior(t *testing.T) {

	t.Parallel()
	terminal, _, _, plan := backupTerminalReceiptPruneFixture(t)
	defer plan.clear()
	key := testbackupruntime.BackupTerminalReceiptKey(terminal.ID)
	entry := &testkeyvalue.KeyValue{Key: key, Value: plan.mutations[0].Value, ModRevision: 20}
	companion, err := prepareBackupTerminalReceiptPruneCompanion(terminal, 20, entry)
	if err != nil || companion.revision != 20 {
		t.Fatalf("prepareBackupTerminalReceiptPruneCompanion() = %#v, %v", companion, err)
	}
	torn := *entry
	torn.ModRevision = 21
	if _, err := prepareBackupTerminalReceiptPruneCompanion(terminal, 20, &torn); err == nil {
		t.Fatal("prepareBackupTerminalReceiptPruneCompanion(torn) succeeded")
	}
	rewritten := plan.record
	rewritten.PriorTaskRevision = 20
	rewritten.ReceiptDigest, err = testbackupruntime.BackupTerminalReceiptDigest(rewritten)
	if err != nil {
		t.Fatal(err)
	}
	value, err := testbackupruntime.EncodeBackupTerminalReceiptRecord(rewritten)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	if _, err := prepareBackupTerminalReceiptPruneCompanion(
		terminal,
		20,
		&testkeyvalue.KeyValue{Key: key, Value: value, ModRevision: 20},
	); err == nil {
		t.Fatal("prepareBackupTerminalReceiptPruneCompanion(rewritten prior) succeeded")
	}
}

func TestBackupTerminalReceiptPruneFinalizationResumesAfterPrimaryDeletion(t *testing.T) {

	t.Parallel()
	ctx := context.Background()
	store := &taskPruneOperationStore{memoryTaskStore: newMemoryTaskStore(), trackPruning: true}
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	taskID := ids.NewAt(ids.KindTask, taskJournalTime(), 7501)
	receiptRevision, err := putTerminalReceiptTestValue(
		ctx,
		store, testbackupruntime.BackupTerminalReceiptKey(taskID), []byte("retained-proof"),
	)
	if err != nil {
		t.Fatal(err)
	}
	intent := testtaskjournal.PruneIntent{
		TaskID: taskID, TaskRevision: receiptRevision,
		BackupCheckpointCursorsComplete:        true,
		BackupCheckpointDeduplicationsComplete: true,
		TaskPrimaryDeleted:                     true,
		BackupTerminalReceiptRevision:          receiptRevision,
	}
	intentValue, err := testtaskjournal.EncodePruneIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(intentValue)
	intentRevision, err := putTerminalReceiptTestValue(
		ctx,
		store, testtaskjournal.TaskPruneIntentKey(taskID), intentValue,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.finishTaskPruneIntent(ctx, testkeyvalue.Versioned[testtaskjournal.PruneIntent]{
		Record: intent, Revision: intentRevision, ReadRevision: intentRevision,
	}); err != nil {
		t.Fatalf("finishTaskPruneIntent() error = %v", err)
	}
	if store.maximumOperations > testkeyvalue.MaximumOperations {
		t.Fatalf("receipt prune finalization operations = %d", store.maximumOperations)
	}
	for _, key := range []string{testbackupruntime.BackupTerminalReceiptKey(taskID), testtaskjournal.TaskPruneIntentKey(taskID)} {
		read, err := store.Get(ctx, key)
		if err != nil || read.Entry != nil {
			t.Fatalf("Get(%s) = %#v, %v", key, read, err)
		}
	}
}

func TestBackupTerminalReceiptDuplicateWriterLosesTaskCAS(t *testing.T) {

	t.Parallel()
	ctx := context.Background()
	terminal, _, _, plan := backupTerminalReceiptPruneFixture(t)
	defer plan.clear()
	value, err := EncodeTaskRecord(terminal)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	store := newMemoryTaskStore()
	store.revision = plan.record.PriorTaskRevision - 1
	prior, err := putTerminalReceiptTestValue(ctx, store, testtaskjournal.TaskStorageKey(terminal.ID), value)
	if err != nil || prior != plan.record.PriorTaskRevision {
		t.Fatalf("seed prior Task revision = %d, %v", prior, err)
	}
	conditions := []testkeyvalue.Condition{{Key: testtaskjournal.TaskStorageKey(terminal.ID), ModRevision: prior}}
	mutations := []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(terminal.ID), Value: value},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testbackupruntime.BackupTerminalReceiptKey(terminal.ID),
			Value: plan.mutations[0].Value,
		},
	}
	first, err := store.Transact(ctx, conditions, mutations)
	if err != nil || !first.Succeeded {
		t.Fatalf("first terminal writer = %#v, %v", first, err)
	}
	second, err := store.Transact(ctx, conditions, mutations)
	if err != nil || second.Succeeded {
		t.Fatalf("duplicate terminal writer = %#v, %v", second, err)
	}
	receiptRead, err := store.Get(ctx, testbackupruntime.BackupTerminalReceiptKey(terminal.ID))
	if err != nil || receiptRead.Entry == nil || receiptRead.Entry.Version != 1 ||
		receiptRead.Entry.ModRevision != first.Revision {
		t.Fatalf("terminal receipt after duplicate = %#v, %v", receiptRead, err)
	}
}

func backupTerminalReceiptPruneFixture(
	t *testing.T,
) (
	TaskRecord, testbackupruntime.BackupRecoveryPointPruneDispatchRecord, testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord],
	backupTerminalReceiptPlan,
) {
	t.Helper()
	now := taskJournalTime().Add(40 * time.Minute)
	point := testBackupVolumePoint(now, newTestBackupRecipient(t))
	operationID := ids.NewAt(ids.KindOperation, now, 7201)
	taskID := ids.NewAt(ids.KindTask, now, 7202)
	owner := testtaskjournal.TaskOwner{
		WorkspaceType: testtaskjournal.TaskWorkspacePlatform,
		ProjectID:     ids.NewAt(ids.KindProject, now, 7203),
		EnvironmentID: point.EnvironmentID,
	}
	current := validTaskRecord(now)
	current.ID = taskID
	current.OperationID = operationID
	current.Owner = owner
	current.Actor = testtaskjournal.TaskActorSystem
	current.Executor = testtaskjournal.TaskExecutorAgent
	current.Type = testtaskjournal.TaskBackupPrune
	current.Target = point.EnvironmentID
	current.RenderGeneration = 0
	current.Params = nil
	current.Materializations = nil
	current.Steps = nil
	current.TimeoutSeconds = backupTaskTimeoutSeconds
	startedAt := now.Add(time.Second)
	current.Status = testtaskjournal.TaskStatusRunning
	current.StartedAt = &startedAt
	current.UpdatedAt = startedAt
	terminalAt := startedAt.Add(time.Second)
	terminal, err := TransitionTaskStatus(
		current, testtaskjournal.TaskStatusRunning, testtaskjournal.TaskStatusFailed, terminalAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	terminal.Result = &testtaskjournal.TaskResultRecord{
		Kind:       testtaskjournal.TaskResultCompose,
		ExitCode:   1,
		Diagnostic: testtaskjournal.TaskResultDiagnosticComposeFailed,
	}
	terminal.TerminalAssignment = &testtaskjournal.TaskTerminalAssignmentRecord{
		AssignmentID:    ids.NewAt(ids.KindAssignment, now, 7204),
		AgentID:         ids.NewAt(ids.KindAgent, now, 7205),
		AgentGeneration: 1,
	}
	dispatch := testbackupruntime.BackupRecoveryPointPruneDispatchRecord{
		TaskID:           taskID,
		OperationID:      operationID,
		EnvironmentID:    point.EnvironmentID,
		RecoveryPointIDs: []string{point.ID},
		CreatedAt:        now,
	}
	assigned := testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{
		Record: testbackupruntime.BackupRecoveryPointPruneRecord{
			Point:         point,
			PointRevision: 19,
			OperationID:   operationID,
			State:         testbackupruntime.BackupPruneAssigned,
			TaskID:        taskID,
			CreatedAt:     now,
			UpdatedAt:     startedAt,
		},
		Revision: 18,
	}
	plan, err := prepareBackupPruneTerminalReceipt(
		testkeyvalue.Versioned[TaskRecord]{Record: current, Revision: 19},
		terminal,
		dispatch,
		[]testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{assigned},
	)
	if err != nil {
		t.Fatalf("prepareBackupPruneTerminalReceipt() error = %v", err)
	}
	return terminal, dispatch, assigned, plan
}

func seedBackupTerminalReceiptReplay(
	t *testing.T,
) (*TaskRepository, *memoryTaskStore, testkeyvalue.Versioned[TaskRecord]) {
	t.Helper()
	ctx := context.Background()
	terminal, _, assigned, plan := backupTerminalReceiptPruneFixture(t)
	defer plan.clear()
	store := newMemoryTaskStore()
	store.revision = plan.record.PriorTaskRevision
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	taskValue, err := EncodeTaskRecord(terminal)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(taskValue)
	pending := assigned.Record
	pending.State = testbackupruntime.BackupPrunePending
	pending.TaskID = ""
	pending.UpdatedAt = *terminal.FinishedAt
	pendingValue, err := testbackupruntime.EncodeBackupRecoveryPointPruneRecord(pending)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pendingValue)
	point := testbackupruntime.BackupRecoveryPointRecord{
		BackupRecoveryPointSnapshot: pending.Point,
		VerifiedAt:                  pending.CreatedAt,
	}
	pointValue, err := testbackupruntime.EncodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pointValue)
	environmentIndex, err := testbackupruntime.BackupRecoveryPointEnvironmentIndexKey(
		point.EnvironmentID,
		point.ID,
	)
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
	project := testhierarchy.ProjectRecord{
		ID: ids.NewAt(ids.KindProject, pending.CreatedAt, 7701), Slug: "receipt-owner",
		Name: "Receipt Owner", Kind: testhierarchy.ProjectKindBacking,
	}
	environment, err := testhierarchy.NewProvisioningEnvironment(
		"/srv/groundplane",
		project,
		point.EnvironmentID,
		"receipt-owner",
		"10.198.0.0/24",
		terminal.ID,
		pending.CreatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	environmentValue, err := testhierarchy.EncodeEnvironment(environment)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(environmentValue)
	epochValue, err := testbackupruntime.EncodeEnvironmentMutationEpochRecord(
		testbackupruntime.EnvironmentMutationEpochRecord{
			EnvironmentID: environment.ID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(epochValue)
	store.revision = plan.record.PriorTaskRevision - 1
	pointTransaction, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentKey(environment.ID), Value: environmentValue},
		{Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointKey(point.ID), Value: pointValue},
		{Type: testkeyvalue.MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
	})
	if err != nil || !pointTransaction.Succeeded ||
		pointTransaction.Revision != plan.record.PriorTaskRevision {
		t.Fatalf("seed retained point authority = %#v, %v", pointTransaction, err)
	}
	transaction, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(terminal.ID), Value: taskValue},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testbackupruntime.BackupTerminalReceiptKey(terminal.ID),
			Value: plan.mutations[0].Value,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testbackupruntime.BackupRecoveryPointPruneKey(pending.Point.ID),
			Value: pendingValue,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testhierarchy.EnvironmentMutationEpochKey(environment.ID),
			Value: epochValue,
		},
	})
	if err != nil || !transaction.Succeeded || transaction.Revision <= plan.record.PriorTaskRevision {
		t.Fatalf("seed terminal receipt = %#v, %v", transaction, err)
	}
	return repository, store, testkeyvalue.Versioned[TaskRecord]{
		Record:       terminal,
		Revision:     transaction.Revision,
		ReadRevision: transaction.Revision,
	}
}

type compactedTaskStore struct {
	*memoryTaskStore
	compactedRevision int64
	historicalReads   int
}

func (store *compactedTaskStore) compact(revision int64) {
	store.mu.Lock()
	defer store.mu.Unlock()
	for key, versions := range store.history {
		anchor := -1
		for index := range versions {
			if versions[index].revision <= revision {
				anchor = index
			}
		}
		if anchor < 0 {
			continue
		}
		compacted := make([]memoryTaskVersion, 0, len(versions)-anchor)
		compacted = append(compacted, versions[anchor])
		compacted = append(compacted, versions[anchor+1:]...)
		store.history[key] = compacted
	}
	store.compactedRevision = revision
}

type terminalReceiptTestTransactor interface {
	Transact(context.Context, []testkeyvalue.Condition, []testkeyvalue.Mutation) (testkeyvalue.TransactionResult, error)
}

func putTerminalReceiptTestValue(
	ctx context.Context,
	store terminalReceiptTestTransactor,
	key string,
	value []byte,
) (int64, error) {
	transaction, err := store.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: value}},
	)
	if err != nil {
		return 0, err
	}
	if !transaction.Succeeded {
		return 0, errs.New(errs.KindStateConflict, "test value put changed")
	}
	return transaction.Revision, nil
}

func deleteTerminalReceiptTestValue(
	ctx context.Context,
	store terminalReceiptTestTransactor,
	key string,
) (int64, error) {
	transaction, err := store.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: key}},
	)
	if err != nil {
		return 0, err
	}
	if !transaction.Succeeded {
		return 0, errs.New(errs.KindStateConflict, "test value delete changed")
	}
	return transaction.Revision, nil
}

func (store *compactedTaskStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	if request.Revision > 0 && request.Revision <= store.compactedRevision {
		store.historicalReads++
		return nil, errs.New(errs.KindInternal, "requested revision was compacted")
	}
	return store.memoryTaskStore.GetMany(ctx, request)
}

func (store *compactedTaskStore) Range(
	ctx context.Context,
	request testkeyvalue.RangeRequest,
) (*testkeyvalue.RangeResult, error) {
	if request.Revision > 0 && request.Revision <= store.compactedRevision {
		store.historicalReads++
		return nil, errs.New(errs.KindInternal, "requested revision was compacted")
	}
	return store.memoryTaskStore.Range(ctx, request)
}
