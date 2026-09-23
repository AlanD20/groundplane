package etcd

import (
	context "context"
	hex "encoding/hex"
	errors "errors"
	testing "testing"
	time "time"

	executionplan "github.com/AlanD20/groundplane/internal/common/executionplan"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	proto "google.golang.org/protobuf/proto"
)

func publishBackupPruneLifecycleTask(
	t *testing.T,
) (
	*BackupRuntimeRepository,
	*memoryHierarchyStore,
	*TaskRepository, testbackupruntime.BackupRecoveryPointPruneDispatchRecord,

	string,
) {
	t.Helper()
	repository, store, run := newBackupRuntimeBareFixture(t)
	source := run.Sources[0]
	source.State = testbackupruntime.BackupSourceAttemptStaged
	source.Phase = testbackupruntime.BackupSourcePhaseUpload
	source.SizeBytes = 123
	source.SHA256 = testBackupDigest
	point := backupRuntimeTestPoint(run, source, run.CreatedAt.Add(time.Second))
	pointValue, err := testbackupruntime.EncodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pointValue)
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
	operationID := ids.NewAt(ids.KindOperation, run.CreatedAt.Add(time.Second), 3991)
	keys := []string{
		testbackupruntime.BackupRecoveryPointKey(point.ID),
		environmentIndex,
		sourceIndex,
		connectorIndex,
		testbackupruntime.BackupRecoveryPointPruneKey(point.ID),
	}
	seeded, err := store.Transact(context.Background(), []testkeyvalue.Condition{
		{Key: keys[0]}, {Key: keys[1]}, {Key: keys[2]}, {Key: keys[3]}, {Key: keys[4]},
	}, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: keys[0], Value: pointValue},
		{Type: testkeyvalue.MutationPut, Key: keys[1], Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: keys[2], Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: keys[3], Value: []byte(point.ID)},
	})
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed prune point = %#v, %v", seeded, err)
	}
	prune := testbackupruntime.BackupRecoveryPointPruneRecord{
		Point: point.BackupRecoveryPointSnapshot, PointRevision: seeded.Revision,
		OperationID: operationID, State: testbackupruntime.BackupPrunePending,
		CreatedAt: point.VerifiedAt, UpdatedAt: point.VerifiedAt,
	}
	pruneValue, err := testbackupruntime.EncodeBackupRecoveryPointPruneRecord(prune)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pruneValue)
	pruneSeeded, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: keys[0], ModRevision: seeded.Revision}, {Key: keys[4]}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: keys[4], Value: pruneValue}},
	)
	if err != nil || !pruneSeeded.Succeeded {
		t.Fatalf("seed prune authority = %#v, %v", pruneSeeded, err)
	}
	pending := []testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{{
		Record: prune, Revision: pruneSeeded.Revision, ReadRevision: pruneSeeded.Revision,
	}}
	createdAt := run.CreatedAt.Add(2 * time.Second)
	dispatch := testbackupruntime.BackupRecoveryPointPruneDispatchRecord{
		TaskID: ids.NewAt(ids.KindTask, createdAt, 3992), OperationID: operationID,
		EnvironmentID: run.EnvironmentID, RecoveryPointIDs: []string{point.ID}, CreatedAt: createdAt,
	}
	plan, err := repository.prepareBackupPrunePublication(
		context.Background(), pending, dispatch, testbackupruntime.BackupOperationLockRecord{
			EnvironmentID: dispatch.EnvironmentID, OperationID: dispatch.OperationID,
			TaskID: dispatch.TaskID, Kind: testbackupruntime.BackupOperationPrune,
			CreatedAt: createdAt, UpdatedAt: createdAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer plan.clear()
	task, sealed, marker, initiation := backupRuntimePrunePublicationTask(
		t, store, dispatch, pending, plan.conditions,
	)
	publication, err := plan.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	applied, err := idempotency.Apply(context.Background(), marker, publication)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := applied.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("publish prune Task outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	return repository, store, tasks, dispatch, point.ID
}

// Rationale: a non-success terminal after remote absence retains no local
// point or prune authority, while deleting the dispatch and releasing the
// exact lock in the same domain plan.
func TestBackupRuntimeRepositoryPruneVerifiedAbsentFailureFinalization(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeBareFixture(t)
	source := run.Sources[0]
	source.State = testbackupruntime.BackupSourceAttemptStaged
	source.Phase = testbackupruntime.BackupSourcePhaseUpload
	source.SizeBytes = 123
	source.SHA256 = testBackupDigest
	verifiedAt := run.CreatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, source, verifiedAt)
	pointValue, err := testbackupruntime.EncodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pointValue)
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
	pointCommit, err := store.Transact(context.Background(), []testkeyvalue.Condition{
		{Key: testbackupruntime.BackupRecoveryPointKey(point.ID)}, {Key: environmentIndex},
		{Key: sourceIndex}, {Key: connectorIndex},
	}, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointKey(point.ID), Value: pointValue},
		{Type: testkeyvalue.MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
	})
	if err != nil || !pointCommit.Succeeded {
		t.Fatalf("seed point = %#v, %v", pointCommit, err)
	}
	pendingSweep := testbackupruntime.BackupRetentionSweepRecord{
		SourceID: point.SourceID, TriggerRecoveryPointID: point.ID, Keep: 3,
		Revision: run.PolicyRevision, State: testbackupruntime.BackupRetentionPending,
		CreatedAt: verifiedAt, UpdatedAt: verifiedAt,
	}
	pendingSweepValue, err := testbackupruntime.EncodeBackupRetentionSweepRecord(pendingSweep)
	if err != nil {
		t.Fatal(err)
	}
	sweepCreated, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: testbackupruntime.BackupRetentionKey(point.SourceID, point.ID)}},
		[]testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRetentionKey(point.SourceID, point.ID), Value: pendingSweepValue,
		}},
	)
	clear(pendingSweepValue)
	if err != nil || !sweepCreated.Succeeded {
		t.Fatalf("seed pending retention sweep = %#v, %v", sweepCreated, err)
	}
	completedSweep := pendingSweep
	completedSweep.State = testbackupruntime.BackupRetentionCompleted
	completedSweep.SelectionRevision = pointCommit.Revision
	completedSweep.PruneOperationID = run.OperationID
	completedSweep.UpdatedAt = verifiedAt.Add(time.Second)
	completedSweepValue, err := testbackupruntime.EncodeBackupRetentionSweepRecord(completedSweep)
	if err != nil {
		t.Fatal(err)
	}
	sweepCompleted, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{
			Key: testbackupruntime.BackupRetentionKey(point.SourceID, point.ID), ModRevision: sweepCreated.Revision,
		}},
		[]testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRetentionKey(point.SourceID, point.ID), Value: completedSweepValue,
		}},
	)
	clear(completedSweepValue)
	if err != nil || !sweepCompleted.Succeeded {
		t.Fatalf("complete retention sweep = %#v, %v", sweepCompleted, err)
	}
	prune := testbackupruntime.BackupRecoveryPointPruneRecord{
		Point: point.BackupRecoveryPointSnapshot, PointRevision: pointCommit.Revision,
		OperationID: run.OperationID,
		State:       testbackupruntime.BackupPrunePending, CreatedAt: verifiedAt.Add(time.Second),
		UpdatedAt: verifiedAt.Add(time.Second),
	}
	pruneValue, err := testbackupruntime.EncodeBackupRecoveryPointPruneRecord(prune)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pruneValue)
	pruneCreate, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: testbackupruntime.BackupRecoveryPointPruneKey(point.ID)}},
		[]testkeyvalue.Mutation{
			{
				Type:  testkeyvalue.MutationPut,
				Key:   testbackupruntime.BackupRecoveryPointPruneKey(point.ID),
				Value: pruneValue,
			},
		},
	)
	if err != nil || !pruneCreate.Succeeded {
		t.Fatalf("seed prune = %#v, %v", pruneCreate, err)
	}
	dispatch := testbackupruntime.BackupRecoveryPointPruneDispatchRecord{
		TaskID: run.TaskID, OperationID: run.OperationID, EnvironmentID: run.EnvironmentID,
		RecoveryPointIDs: []string{point.ID},
		CreatedAt:        prune.UpdatedAt.Add(time.Second),
	}
	lock := testbackupruntime.BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID, OperationID: run.OperationID, TaskID: run.TaskID,
		Kind: testbackupruntime.BackupOperationPrune, CreatedAt: dispatch.CreatedAt, UpdatedAt: dispatch.CreatedAt,
	}
	publication, err := repository.prepareBackupPrunePublication(
		context.Background(),
		[]testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{
			{Record: prune, Revision: pruneCreate.Revision, ReadRevision: pruneCreate.Revision},
		},
		dispatch,
		lock,
	)
	if err != nil {
		t.Fatalf("prepareBackupPrunePublication() error = %v", err)
	}
	publicationMarker := "/v1/test/backup-prune-tasks/" + dispatch.TaskID
	conditions, mutations, err := publication.composeTransaction(
		[]testkeyvalue.Condition{{Key: publicationMarker}},
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationPut, Key: publicationMarker, Value: []byte(dispatch.TaskID)},
		},
	)
	if err != nil {
		t.Fatalf("compose prune publication = %v", err)
	}
	published, err := repository.TransactRuntime(context.Background(), conditions, mutations)
	testkeyvalue.ClearMutationValues(mutations)
	publication.clear()
	if err != nil || !published.Succeeded {
		t.Fatalf("publish prune = %#v, %v", published, err)
	}
	assignedEntry := mustOptionalKey(t, store, testbackupruntime.BackupRecoveryPointPruneKey(point.ID))
	assigned, err := testbackupruntime.DecodeBackupRecoveryPointPruneRecord(assignedEntry.Value)
	if err != nil || assigned.State != testbackupruntime.BackupPruneAssigned {
		t.Fatalf("assigned prune = %#v, %v", assigned, err)
	}
	dispatchVersion := testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneDispatchRecord]{
		Record: dispatch, Revision: published.Revision, ReadRevision: published.Revision,
	}
	assignedVersion := testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{
		Record: assigned, Revision: assignedEntry.ModRevision, ReadRevision: published.Revision,
	}
	verified := assigned
	verified.State = testbackupruntime.BackupPruneVerifiedAbsent
	verified.UpdatedAt = assigned.UpdatedAt.Add(time.Second)
	checkpointInput, _ := seedBackupPruneCheckpointAssignment(
		t, store, run, dispatch.RecoveryPointIDs,
	)
	checkpointInput = backupRemoteAbsentCheckpoint(checkpointInput, 1, point.ID)
	checkpoint, err := repository.MarkBackupRecoveryPointPruneVerifiedAbsent(
		context.Background(), checkpointInput, dispatchVersion, assignedVersion, verified, true,
	)
	if err != nil {
		t.Fatalf("MarkBackupRecoveryPointPruneVerifiedAbsent() error = %v", err)
	}
	for _, key := range []string{testbackupruntime.BackupRecoveryPointKey(point.ID), environmentIndex, sourceIndex, connectorIndex, testbackupruntime.BackupRetentionKey(point.SourceID, point.ID)} {
		if entry := mustOptionalKey(t, store, key); entry != nil {
			t.Fatalf("verified-absent point authority %q = %#v", key, entry)
		}
	}
	completion, err := repository.prepareBackupPruneFailure(
		context.Background(),
		dispatchVersion,
		[]testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{checkpoint},
		checkpoint.Record.UpdatedAt.Add(time.Second),
	)
	if err != nil {
		t.Fatalf("prepareBackupPruneFailure() error = %v", err)
	}
	conditions, mutations, err = completion.composeTransaction(
		[]testkeyvalue.Condition{{Key: publicationMarker, ModRevision: published.Revision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: publicationMarker}},
	)
	if err != nil {
		t.Fatalf("compose prune completion = %v", err)
	}
	completed, err := repository.TransactRuntime(context.Background(), conditions, mutations)
	testkeyvalue.ClearMutationValues(mutations)
	completion.clear()
	if err != nil || !completed.Succeeded {
		t.Fatalf("complete prune = %#v, %v", completed, err)
	}
	for _, key := range []string{testbackupruntime.BackupRecoveryPointPruneKey(point.ID), testbackupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.TaskID), testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID), publicationMarker} {
		if entry := mustOptionalKey(t, store, key); entry != nil {
			t.Fatalf("completed prune authority %q = %#v", key, entry)
		}
	}
}

// Rationale: the durable eleven-point prune maximum must leave exact room for
// complete Task publication, failure release, retry reacquisition, and
// running-terminal parents.
func TestBackupRuntimeRepositoryComposesElevenPointPruneTaskBoundaries(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeBareFixture(t)
	operationID := ids.NewAt(ids.KindOperation, run.CreatedAt, 2100)
	secondaryConnector := testConnectorRecord(
		t, run.EnvironmentID, run.CreatedAt, 2101, "archive-store",
	)
	secondaryConnectorValue, err := testconnectors.EncodeRecord(secondaryConnector)
	if err != nil {
		t.Fatal(err)
	}
	secondaryConnectorCreated, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: testconnectors.RecordKey(secondaryConnector.Connector.ID)}},
		[]testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: testconnectors.RecordKey(secondaryConnector.Connector.ID),
			Value: secondaryConnectorValue,
		}},
	)
	clear(secondaryConnectorValue)
	if err != nil || !secondaryConnectorCreated.Succeeded {
		t.Fatalf("seed secondary prune Connector = %#v, %v", secondaryConnectorCreated, err)
	}
	pending := make(
		[]testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord],
		testbackupruntime.MaximumBackupPruneBatch,
	)
	pointIDs := make([]string, testbackupruntime.MaximumBackupPruneBatch)
	mutations := make([]testkeyvalue.Mutation, 0, testbackupruntime.MaximumBackupPruneBatch*4)
	for index := range pending {
		allocatedAt := run.CreatedAt.Add(time.Duration(index) * time.Millisecond)
		source := run.Sources[0]
		source.RecoveryPointID = ids.NewAt(ids.KindRecoveryPoint, allocatedAt, int64(2200+index))
		source.RecoveryPointCreatedAt = allocatedAt
		source.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + source.SourceID + "/" +
			source.RecoveryPointID + "/artifact.bin"
		source.State = testbackupruntime.BackupSourceAttemptStaged
		source.Phase = testbackupruntime.BackupSourcePhasePointCommit
		source.SizeBytes = 123
		source.SHA256 = testBackupDigest
		pointRun := run
		if index%2 == 1 {
			pointRun.ConnectorID = secondaryConnector.Connector.ID
			pointRun.ConnectorPrefix = secondaryConnector.Connector.Prefix
			source.ObjectKey = pointRun.ConnectorPrefix + run.EnvironmentID + "/" + source.SourceID + "/" +
				source.RecoveryPointID + "/artifact.bin"
		}
		point := backupRuntimeTestPoint(pointRun, source, allocatedAt.Add(time.Second))
		record := testbackupruntime.BackupRecoveryPointPruneRecord{
			Point:       point.BackupRecoveryPointSnapshot,
			OperationID: operationID,
			State:       testbackupruntime.BackupPrunePending,
			CreatedAt:   allocatedAt.Add(2 * time.Second),
			UpdatedAt:   allocatedAt.Add(2 * time.Second),
		}
		pointIDs[index] = point.ID
		pending[index] = testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{Record: record}
		pointValue, err := testbackupruntime.EncodeBackupRecoveryPointRecord(point)
		if err != nil {
			testkeyvalue.ClearMutationValues(mutations)
			t.Fatal(err)
		}
		environmentIndex, err := testbackupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
		if err != nil {
			clear(pointValue)
			testkeyvalue.ClearMutationValues(mutations)
			t.Fatal(err)
		}
		sourceIndex, err := testbackupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
		if err != nil {
			clear(pointValue)
			testkeyvalue.ClearMutationValues(mutations)
			t.Fatal(err)
		}
		connectorIndex, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
		if err != nil {
			clear(pointValue)
			testkeyvalue.ClearMutationValues(mutations)
			t.Fatal(err)
		}
		mutations = append(
			mutations,
			testkeyvalue.Mutation{
				Type:  testkeyvalue.MutationPut,
				Key:   testbackupruntime.BackupRecoveryPointKey(point.ID),
				Value: pointValue,
			},
			testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
			testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
			testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
		)
	}
	pointSeeded, err := store.Transact(context.Background(), nil, mutations)
	testkeyvalue.ClearMutationValues(mutations)
	if err != nil || !pointSeeded.Succeeded {
		t.Fatalf("seed eleven prune points = %#v, %v", pointSeeded, err)
	}
	mutations = make([]testkeyvalue.Mutation, 0, testbackupruntime.MaximumBackupPruneBatch)
	for index := range pending {
		pending[index].Record.PointRevision = pointSeeded.Revision
		value, encodeErr := testbackupruntime.EncodeBackupRecoveryPointPruneRecord(pending[index].Record)
		if encodeErr != nil {
			testkeyvalue.ClearMutationValues(mutations)
			t.Fatal(encodeErr)
		}
		mutations = append(mutations, testkeyvalue.Mutation{
			Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointPruneKey(pointIDs[index]), Value: value,
		})
	}
	pruneSeeded, err := store.Transact(context.Background(), nil, mutations)
	testkeyvalue.ClearMutationValues(mutations)
	if err != nil || !pruneSeeded.Succeeded {
		t.Fatalf("seed eleven prune authorities = %#v, %v", pruneSeeded, err)
	}
	for index := range pending {
		pending[index].Revision = pruneSeeded.Revision
		pending[index].ReadRevision = pruneSeeded.Revision
	}
	dispatch := testbackupruntime.BackupRecoveryPointPruneDispatchRecord{
		TaskID:           run.TaskID,
		OperationID:      operationID,
		EnvironmentID:    run.EnvironmentID,
		RecoveryPointIDs: pointIDs,
		CreatedAt:        run.CreatedAt.Add(3 * time.Second),
	}
	lock := testbackupruntime.BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID,
		OperationID:   operationID,
		TaskID:        run.TaskID,
		Kind:          testbackupruntime.BackupOperationPrune,
		CreatedAt:     dispatch.CreatedAt,
		UpdatedAt:     dispatch.CreatedAt,
	}
	publication, err := repository.prepareBackupPrunePublication(
		context.Background(), pending, dispatch, lock,
	)
	if err != nil {
		t.Fatal(err)
	}
	task, sealed, marker, initiation := backupRuntimePrunePublicationTask(
		t, store, dispatch, pending, publication.conditions,
	)
	genericTask := task
	genericTask.Params = map[string]string{"object_key": pending[0].Record.Point.ObjectKey}
	if _, err := publication.taskIdempotencyPlan(
		genericTask, sealed, marker, initiation,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("taskIdempotencyPlan(generic prune Params) error = %v", err)
	}
	fabricated := proto.Clone(sealed).(*agentpb.ExecutionPlan)
	fabricated.PlanHash = nil
	fabricated.Steps[0].GetBackupArtifactPrune().ConnectorRevision++
	fabricated, err = executionplan.Seal(fabricated)
	if err != nil {
		t.Fatal(err)
	}
	fabricatedTask := task
	fabricatedTask.PlanHash = hex.EncodeToString(fabricated.PlanHash)
	if _, err := publication.taskIdempotencyPlan(
		fabricatedTask, fabricated, marker, initiation,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("taskIdempotencyPlan(fabricated prune Connector) error = %v", err)
	}
	idempotencyPlan, err := publication.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	publicationResult, err := idempotency.Apply(
		context.Background(), marker, idempotencyPlan,
	)
	publication.clear()
	if err != nil {
		t.Fatalf("publish eleven-point prune Task error = %v", err)
	}
	outcome, _, conflict, err := publicationResult.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf(
			"publish eleven-point prune Task outcome/conflict/error = %v/%v/%v",
			outcome,
			conflict,
			err,
		)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	agentID := ids.NewAt(ids.KindAgent, dispatch.CreatedAt, 2400)
	claim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, dispatch.CreatedAt.Add(time.Second),
	)
	if err != nil || !found || claim.Task.Record.ID != dispatch.TaskID {
		t.Fatalf("ClaimNextTask() = %#v/%v/%v", claim, found, err)
	}
	assigned := make([]testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord], len(pointIDs))
	failedResult := completedComposeTaskResult()
	failedResult.ExitCode = 1
	failedTask, err := tasks.AcknowledgeTask(
		context.Background(),
		agentID,
		1,
		dispatch.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failedResult,
		dispatch.CreatedAt.Add(2*time.Second),
	)
	if err != nil || failedTask.Record.Status != testtaskjournal.TaskStatusFailed {
		t.Fatalf("AcknowledgeTask(prune failure) = %#v, %v", failedTask, err)
	}
	replay, err := tasks.AcknowledgeTask(
		context.Background(), agentID, 1, dispatch.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failedResult,
		dispatch.CreatedAt.Add(3*time.Second),
	)
	if err != nil || replay.Revision != failedTask.Revision {
		t.Fatalf("AcknowledgeTask(prune replay) = %#v, %v", replay, err)
	}
	if dispatchEntry := mustOptionalKey(
		t,
		store, testbackupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.TaskID),
	); dispatchEntry != nil {
		t.Fatalf("failed prune dispatch = %#v", dispatchEntry)
	}
	if lockEntry := mustOptionalKey(t, store, testhierarchy.EnvironmentOperationLockKey(dispatch.EnvironmentID)); lockEntry != nil {
		t.Fatalf("failed prune lock = %#v", lockEntry)
	}
	pending = make([]testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord], len(pointIDs))
	for index, pointID := range pointIDs {
		entry := mustOptionalKey(t, store, testbackupruntime.BackupRecoveryPointPruneKey(pointID))
		record, decodeErr := testbackupruntime.DecodeBackupRecoveryPointPruneRecord(entry.Value)
		if decodeErr != nil || record.State != testbackupruntime.BackupPrunePending || record.TaskID != "" ||
			record.OperationID != dispatch.OperationID {
			t.Fatalf("failed prune pending authority = %#v, %v", record, decodeErr)
		}
		pending[index] = testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{
			Record: record, Revision: entry.ModRevision, ReadRevision: failedTask.Revision,
		}
	}
	sourceTask, err := tasks.GetTask(context.Background(), dispatch.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	nextDispatch := dispatch
	nextDispatch.TaskID = testBackupRetryTaskID
	nextDispatch.CreatedAt = dispatch.CreatedAt.Add(4 * time.Second)
	transferredAt := dispatch.CreatedAt.Add(4 * time.Second)
	retryPublication, err := repository.prepareBackupPrunePublication(
		context.Background(), pending, nextDispatch, testbackupruntime.BackupOperationLockRecord{
			EnvironmentID: nextDispatch.EnvironmentID, OperationID: nextDispatch.OperationID,
			TaskID: nextDispatch.TaskID, Kind: testbackupruntime.BackupOperationPrune,
			CreatedAt: transferredAt, UpdatedAt: transferredAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	retryTask, err := CloneRetryTask(
		sourceTask.Record, nextDispatch.TaskID, testtaskjournal.TaskActorSystem, transferredAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	retryTask.PlanID = ids.NewAt(ids.KindPlan, transferredAt, 2450)
	retrySealed := backupRuntimeSealedPrunePlan(t, store, nextDispatch, pending, retryTask.PlanID)
	retryTask.PlanHash = hex.EncodeToString(retrySealed.PlanHash)
	retryTask.Steps = make([]testtaskjournal.TaskStepRecord, len(retrySealed.Steps))
	for index, step := range retrySealed.Steps {
		retryTask.Steps[index] = testtaskjournal.TaskStepRecord{
			Kind: testtaskjournal.TaskStepOperation,
			ID:   step.StepId,
		}
	}
	retryMarker := pendingTaskMarker(retryTask)
	retryMarker.Locator.ScopeID = dispatch.EnvironmentID
	retryMarker.Locator.Route = "/internal/backup-prunes/{task}/retry"
	retryMarker.Locator.Key = "backup-prune-retry-runtime-0001"
	staleSource := sourceTask
	staleSource.ReadRevision++
	if _, err := retryPublication.taskRetryIdempotencyPlan(
		staleSource, retryTask, retrySealed, retryMarker,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("taskRetryIdempotencyPlan(stale fixed read) error = %v", err)
	}
	if retryEntry := mustOptionalKey(t, store, testtaskjournal.TaskStorageKey(nextDispatch.TaskID)); retryEntry != nil {
		t.Fatalf("rejected stale prune retry Task = %#v", retryEntry)
	}
	retryPlan, err := retryPublication.taskRetryIdempotencyPlan(
		sourceTask, retryTask, retrySealed, retryMarker,
	)
	if err != nil {
		t.Fatal(err)
	}
	transferResult, err := idempotency.Apply(context.Background(), retryMarker, retryPlan)
	retryPublication.clear()
	if err != nil {
		t.Fatalf("transfer eleven-point prune Task error = %v", err)
	}
	outcome, _, conflict, err = transferResult.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf(
			"transfer eleven-point prune Task outcome/conflict/error = %v/%v/%v",
			outcome,
			conflict,
			err,
		)
	}
	transferredRevision := mustOptionalKey(t, store, testtaskjournal.TaskStorageKey(nextDispatch.TaskID)).ModRevision
	for index, pointID := range pointIDs {
		entry := mustOptionalKey(t, store, testbackupruntime.BackupRecoveryPointPruneKey(pointID))
		record, decodeErr := testbackupruntime.DecodeBackupRecoveryPointPruneRecord(entry.Value)
		if decodeErr != nil || record.State != testbackupruntime.BackupPruneAssigned ||
			record.TaskID != nextDispatch.TaskID {
			t.Fatalf("retry assigned prune = %#v, %v", record, decodeErr)
		}
		assigned[index] = testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{
			Record: record, Revision: entry.ModRevision, ReadRevision: transferredRevision,
		}
	}
	claim, found, err = tasks.ClaimNextTask(
		context.Background(), agentID, 1, dispatch.CreatedAt.Add(5*time.Second),
	)
	if err != nil || !found || claim.Task.Record.ID != nextDispatch.TaskID {
		t.Fatalf("ClaimNextTask(retry) = %#v/%v/%v", claim, found, err)
	}
	verifiedMutations := make([]testkeyvalue.Mutation, 0, len(assigned))
	verifiedConditions := make([]testkeyvalue.Condition, 0, len(assigned))
	for index := range assigned {
		record := assigned[index].Record
		record.State = testbackupruntime.BackupPruneVerifiedAbsent
		record.UpdatedAt = dispatch.CreatedAt.Add(6 * time.Second)
		value, encodeErr := testbackupruntime.EncodeBackupRecoveryPointPruneRecord(record)
		if encodeErr != nil {
			testkeyvalue.ClearMutationValues(verifiedMutations)
			t.Fatal(encodeErr)
		}
		verifiedConditions = append(verifiedConditions, testkeyvalue.Condition{
			Key:         testbackupruntime.BackupRecoveryPointPruneKey(record.Point.ID),
			ModRevision: assigned[index].Revision,
		})
		verifiedMutations = append(verifiedMutations, testkeyvalue.Mutation{
			Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointPruneKey(record.Point.ID), Value: value,
		})
		authorityKeys, keyErr := backupPruneAuthorityKeys(record.Point)
		if keyErr != nil {
			testkeyvalue.ClearMutationValues(verifiedMutations)
			t.Fatal(keyErr)
		}
		for _, key := range authorityKeys[1:] {
			verifiedMutations = append(
				verifiedMutations,
				testkeyvalue.Mutation{Type: testkeyvalue.MutationDelete, Key: key},
			)
		}
		assigned[index].Record = record
	}
	verified, err := store.Transact(
		context.Background(), verifiedConditions, verifiedMutations,
	)
	testkeyvalue.ClearMutationValues(verifiedMutations)
	if err != nil || !verified.Succeeded {
		t.Fatalf("verify eleven prunes = %#v, %v", verified, err)
	}
	for index := range assigned {
		assigned[index].Revision = verified.Revision
		assigned[index].ReadRevision = verified.Revision
	}
	completedTask, err := tasks.AcknowledgeTask(
		context.Background(),
		agentID,
		1,
		nextDispatch.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, completedComposeTaskResult(),
		dispatch.CreatedAt.Add(7*time.Second),
	)
	if err != nil {
		t.Fatalf("AcknowledgeTask(prune completion) error = %v", err)
	}
	terminalTask := mustOptionalKey(t, store, testtaskjournal.TaskStorageKey(nextDispatch.TaskID))
	if terminalTask == nil || terminalTask.ModRevision != completedTask.Revision {
		t.Fatalf("terminal prune Task = %#v", terminalTask)
	}
	if lockEntry := mustOptionalKey(t, store, testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID)); lockEntry != nil {
		t.Fatalf("completed eleven-point prune lock = %#v", lockEntry)
	}
	sourceTask, err = tasks.GetTask(context.Background(), dispatch.TaskID)
	if err != nil || sourceTask.Record.RetainUntil == nil {
		t.Fatalf("terminal source prune Task = %#v, %v", sourceTask, err)
	}
	pruneAt := sourceTask.Record.RetainUntil.Add(time.Nanosecond)
	for {
		count, pruneErr := idempotency.PruneExpired(context.Background(), pruneAt)
		if pruneErr != nil {
			t.Fatalf("PruneExpired(prune Task marker) error = %v", pruneErr)
		}
		if count == 0 {
			break
		}
	}
	staleDispatchValue, err := testbackupruntime.EncodeBackupRecoveryPointPruneDispatchRecord(dispatch)
	if err != nil {
		t.Fatal(err)
	}
	staleDispatch, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: testbackupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.TaskID)}},
		[]testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.TaskID),
			Value: staleDispatchValue,
		}},
	)
	clear(staleDispatchValue)
	if err != nil || !staleDispatch.Succeeded {
		t.Fatalf("seed stale terminal prune dispatch = %#v, %v", staleDispatch, err)
	}
	if count, pruneErr := tasks.PruneExpiredTasks(
		context.Background(), pruneAt,
	); !errors.Is(pruneErr, errs.New(errs.KindInternal, "")) || count != 0 {
		t.Fatalf("PruneExpiredTasks(stale prune dispatch) = %d, %v", count, pruneErr)
	}
	removedDispatch, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{
			Key:         testbackupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.TaskID),
			ModRevision: staleDispatch.Revision,
		}},
		[]testkeyvalue.Mutation{
			{
				Type: testkeyvalue.MutationDelete,
				Key:  testbackupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.TaskID),
			},
		},
	)
	if err != nil || !removedDispatch.Succeeded {
		t.Fatalf("remove stale terminal prune dispatch = %#v, %v", removedDispatch, err)
	}
	if count, pruneErr := tasks.PruneExpiredTasks(
		context.Background(), pruneAt,
	); pruneErr != nil || count != 1 {
		t.Fatalf("PruneExpiredTasks(prune source) = %d, %v", count, pruneErr)
	}
	if sourceEntry := mustOptionalKey(t, store, testtaskjournal.TaskStorageKey(dispatch.TaskID)); sourceEntry != nil {
		t.Fatalf("retained source prune Task = %#v", sourceEntry)
	}
}
