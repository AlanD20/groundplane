package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintRequirementGateFixture struct {
	store      *memoryTaskStore
	repository *TaskRepository
	task       TaskRecord
	attach     AttachRecord
	gate       BlueprintRequirementGate
	projection EnvironmentComposeProjection
}

func newBlueprintRequirementGateFixture(
	t *testing.T,
	status core.AttachStatus,
	seedGate bool,
) blueprintRequirementGateFixture {
	t.Helper()
	now := time.Date(2026, 9, 2, 16, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, now, 1)
	project := ProjectRecord{
		ID: ids.NewAt(ids.KindProject, now, 2), TenantID: tenantID, Kind: ProjectKindTenant,
	}
	environment := EnvironmentRecord{ID: ids.NewAt(ids.KindEnvironment, now, 3), ProjectID: project.ID}
	task := environmentBlueprintTestTask(t, project, environment, 10)
	producerTaskID := ids.NewAt(ids.KindTask, now, 20)
	attachID := ids.NewAt(ids.KindAttach, now, 21)
	attach, err := NewPendingAttachRecord(
		attachID,
		environment.ID,
		"api-db",
		ids.NewAt(ids.KindProject, now, 22),
		ids.NewAt(ids.KindEnvironment, now, 23),
		ids.NewAt(ids.KindService, now, 24),
		ids.NewAt(ids.KindNetwork, now, 25),
		ids.NewAt(ids.KindService, now, 26),
		attachID,
		nil,
		nil,
		producerTaskID,
		now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord() error = %v", err)
	}
	attach.Status = status
	attachValue, err := encodeAttachRecord(attach)
	if err != nil {
		t.Fatalf("encodeAttachRecord() error = %v", err)
	}
	store := newMemoryTaskStore()
	seedTaskRepositoryValue(t, store, attachKey(attach.ID), attachValue)
	requirements := core.BlueprintRequirements{
		Authored: []core.Requirement{{
			Target:    core.RequirementTarget{Kind: core.RequirementTargetBackingAttach, Name: attach.Name},
			Condition: core.RequirementReady,
			Phases:    []core.RequirementPhase{core.RequirementPhaseDeploy},
		}},
		Resolved: []core.ResolvedRequirement{{
			Target: core.ResolvedRequirementTarget{
				Kind: core.RequirementTargetBackingAttach, Name: attach.Name,
				ID: attach.ID, TaskID: attach.TaskID, Revision: store.currentRevision(),
			},
			Condition: core.RequirementReady,
			Phases:    []core.RequirementPhase{core.RequirementPhaseDeploy},
		}},
		ResolutionRevision: store.currentRevision(),
	}
	dag, err := core.BuildBlueprintRequirementDAG(
		task.ID, requirements, []string{task.Steps[0].ID}, nil,
	)
	if err != nil {
		t.Fatalf("BuildBlueprintRequirementDAG() error = %v", err)
	}
	if err := dag.Validate(); err != nil {
		t.Fatalf("built DAG validation = %v; DAG = %#v", err, dag)
	}
	gate, err := NewBlueprintRequirementGate(task, requirements.ResolutionRevision, dag)
	if err != nil {
		t.Fatalf("NewBlueprintRequirementGate() error = %v", err)
	}
	task.Params[TaskBlueprintRequirementGateSHA256Param] = gate.DAGDigest
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	seedBlueprintRequirementTask(t, store, task)
	if seedGate {
		gateValue, encodeErr := encodeBlueprintRequirementGate(gate)
		if encodeErr != nil {
			t.Fatalf("encodeBlueprintRequirementGate() error = %v", encodeErr)
		}
		seedTaskRepositoryValue(t, store, blueprintRequirementGateKey(task.ID), gateValue)
	}
	return blueprintRequirementGateFixture{
		store: store, repository: repository, task: task, attach: attach, gate: gate,
		projection: EnvironmentComposeProjection{
			EnvironmentID: environment.ID, RevisionID: task.ID, BlueprintRequirements: requirements,
		},
	}
}

func seedBlueprintRequirementTask(t *testing.T, store *memoryTaskStore, task TaskRecord) {
	t.Helper()
	marker := pendingTaskMarker(task)
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := encodeTaskReference(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	markerKey, err := idempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	markerValue, err := encodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatal(err)
	}
	ownerKeys, err := taskOwnerIndexKeys(task.Owner, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: MutationPut, Key: markerKey, Value: markerValue},
	}
	for _, key := range ownerKeys {
		mutations = append(mutations, Mutation{Type: MutationPut, Key: key, Value: []byte(task.ID)})
	}
	result, err := store.Transact(context.Background(), nil, mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed Blueprint Task transaction = %#v, %v", result, err)
	}
}

func TestBlueprintRequirementGateClaimSuccessAndUnmet(t *testing.T) {
	for _, test := range []struct {
		name  string
		state core.AttachStatus
		want  bool
	}{
		{name: "ready", state: core.AttachReady, want: true},
		{name: "unmet", state: core.AttachPending, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBlueprintRequirementGateFixture(t, test.state, true)
			assignment, found, err := fixture.repository.ClaimNextTask(
				context.Background(),
				ids.NewAt(ids.KindAgent, fixture.task.CreatedAt, 80),
				1,
				fixture.task.CreatedAt.Add(time.Second),
			)
			if err != nil || found != test.want {
				t.Fatalf("ClaimNextTask() = %#v, %t, %v; want found %t", assignment, found, err, test.want)
			}
			if found && assignment.Task.Record.ID != fixture.task.ID {
				t.Fatalf("claimed Task = %q, want %q", assignment.Task.Record.ID, fixture.task.ID)
			}
		})
	}
}

func TestBlueprintRequirementGateClaimFailsClosedWhenMarkerHasNoGate(t *testing.T) {
	fixture := newBlueprintRequirementGateFixture(t, core.AttachReady, false)
	_, found, err := fixture.repository.ClaimNextTask(
		context.Background(),
		ids.NewAt(ids.KindAgent, fixture.task.CreatedAt, 81),
		1,
		fixture.task.CreatedAt.Add(time.Second),
	)
	if err == nil || found || !isKind(err, errs.KindInternal) {
		t.Fatalf("ClaimNextTask() found/error = %t/%v, want false/internal", found, err)
	}
}

type blueprintRequirementGateClaimRaceStore struct {
	*memoryTaskStore
	targetKey   string
	replacement []byte
	armed       bool
}

func (store *blueprintRequirementGateClaimRaceStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	if store.armed {
		for _, condition := range conditions {
			if condition.Key == store.targetKey {
				store.armed = false
				if _, err := store.memoryTaskStore.Transact(ctx, nil, []Mutation{{
					Type: MutationPut, Key: store.targetKey, Value: store.replacement,
				}}); err != nil {
					return TransactionResult{}, err
				}
				break
			}
		}
	}
	return store.memoryTaskStore.Transact(ctx, conditions, mutations)
}

func TestBlueprintRequirementGateClaimRetriesAgainstTargetRace(t *testing.T) {
	fixture := newBlueprintRequirementGateFixture(t, core.AttachReady, true)
	pending := fixture.attach
	pending.Status = core.AttachPending
	pendingValue, err := encodeAttachRecord(pending)
	if err != nil {
		t.Fatal(err)
	}
	tracing := &blueprintRequirementGateClaimRaceStore{
		memoryTaskStore: fixture.store,
		targetKey:       attachKey(fixture.attach.ID),
		replacement:     pendingValue,
		armed:           true,
	}
	repository, err := newTaskRepository(tracing)
	if err != nil {
		t.Fatal(err)
	}
	_, found, err := repository.ClaimNextTask(
		context.Background(),
		ids.NewAt(ids.KindAgent, fixture.task.CreatedAt, 82),
		1,
		fixture.task.CreatedAt.Add(time.Second),
	)
	if err != nil || found || tracing.armed {
		t.Fatalf("ClaimNextTask(race) found/error/armed = %t/%v/%t", found, err, tracing.armed)
	}
	current, err := repository.GetTask(context.Background(), fixture.task.ID)
	if err != nil || current.Record.Status != TaskStatusPending {
		t.Fatalf("Task after target race = %#v, %v", current, err)
	}
}

func TestBlueprintRequirementGatePublicationRejectsResolvedTargetRace(t *testing.T) {
	fixture := newBlueprintRequirementGateFixture(t, core.AttachReady, false)
	prepared, err := prepareBlueprintRequirementGatePublication(
		fixture.gate, fixture.task, fixture.projection, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.clear()
	changed := fixture.attach
	changed.Status = core.AttachPending
	changedValue, err := encodeAttachRecord(changed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Transact(context.Background(), nil, []Mutation{{
		Type: MutationPut, Key: attachKey(changed.ID), Value: changedValue,
	}}); err != nil {
		t.Fatal(err)
	}
	result, err := fixture.store.Transact(context.Background(), prepared.conditions, prepared.mutations)
	if err != nil || result.Succeeded {
		t.Fatalf("gate publication race transaction = %#v, %v", result, err)
	}
	if err := prepared.classify(result.FailureReads); err == nil || !isKind(err, errs.KindStateConflict) {
		t.Fatalf("gate publication race classification = %v, want state conflict", err)
	}
}

func TestEnvironmentBlueprintComponentClassifierPreservesRequirementConflict(t *testing.T) {
	// Rationale: Component publication extends the requirement-aware compare
	// classifier; it must not reset the chain and hide a raced requirement.
	t.Parallel()
	requirementClassified := false
	requirementClassifier := func(_ int64, values []*KeyValue) error {
		requirementClassified = true
		if len(values) != 1 {
			return errs.New(errs.KindInternal, "requirement compare evidence is incomplete")
		}
		return errs.New(errs.KindStateConflict, "Blueprint requirement target raced")
	}
	componentPublication := preparedComponentTaskPublication{
		conditions: []Condition{{Key: "/test/component-publication"}},
	}
	classified := classifyEnvironmentBlueprintComponentPublication(
		requirementClassifier,
		componentPublication,
	)
	err := classified(0, []*KeyValue{nil, nil})
	if !requirementClassified || !isKind(err, errs.KindStateConflict) {
		t.Fatalf(
			"Component publication classification called/error = %t/%v, want true/state conflict",
			requirementClassified,
			err,
		)
	}
}

func TestBlueprintRequirementGateRetryAndReplayPreserveOriginalRoot(t *testing.T) {
	fixture := newBlueprintRequirementGateFixture(t, core.AttachReady, true)
	failed := terminalBlueprintRequirementTask(t, fixture.repository, fixture.store, fixture.task)
	retryAt := failed.UpdatedAt.Add(time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 90)
	marker := pendingRetryMarker(failed, retryID, retryAt, "requirement-gate-retry-one")
	result, err := fixture.repository.RetryTask(
		context.Background(), failed.ID, retryID, TaskActorOperator, marker,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertRequirementGateRetryOutcome(t, result, IdempotencyKnownApplied)
	firstGate, firstRevision := readBlueprintRequirementGateForTest(t, fixture.store, retryID)
	if firstGate.RetryOf != failed.ID || firstGate.DAG.RootTaskID != fixture.task.ID ||
		firstGate.DAGDigest != fixture.gate.DAGDigest {
		t.Fatalf("first retry gate = %#v", firstGate)
	}
	replay, err := fixture.repository.RetryTask(
		context.Background(), failed.ID, retryID, TaskActorOperator, marker,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertRequirementGateRetryOutcome(t, replay, IdempotencyKnownExisting)
	_, replayRevision := readBlueprintRequirementGateForTest(t, fixture.store, retryID)
	if replayRevision != firstRevision {
		t.Fatalf("replayed gate revision = %d, want %d", replayRevision, firstRevision)
	}

	firstRetry, err := fixture.repository.GetTask(context.Background(), retryID)
	if err != nil {
		t.Fatal(err)
	}
	firstFailed := terminalBlueprintRequirementTask(t, fixture.repository, fixture.store, firstRetry.Record)
	secondAt := firstFailed.UpdatedAt.Add(time.Second)
	secondID := ids.NewAt(ids.KindTask, secondAt, 91)
	secondMarker := pendingRetryMarker(firstFailed, secondID, secondAt, "requirement-gate-retry-two")
	second, err := fixture.repository.RetryTask(
		context.Background(), firstFailed.ID, secondID, TaskActorOperator, secondMarker,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertRequirementGateRetryOutcome(t, second, IdempotencyKnownApplied)
	secondGate, _ := readBlueprintRequirementGateForTest(t, fixture.store, secondID)
	if secondGate.RetryOf != firstFailed.ID || secondGate.DAG.RootTaskID != fixture.task.ID ||
		secondGate.DAGDigest != fixture.gate.DAGDigest {
		t.Fatalf("second retry gate = %#v", secondGate)
	}
}

func terminalBlueprintRequirementTask(
	t *testing.T,
	repository *TaskRepository,
	store *memoryTaskStore,
	task TaskRecord,
) TaskRecord {
	t.Helper()
	current, err := repository.GetTask(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	running, err := transitionTaskStatus(
		current.Record, TaskStatusPending, TaskStatusRunning, current.Record.UpdatedAt.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := transitionTaskStatus(
		running, TaskStatusRunning, TaskStatusFailed, running.UpdatedAt.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	value, err := encodeTaskRecord(failed)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Transact(context.Background(), []Condition{{
		Key: taskKey(task.ID), ModRevision: current.Revision,
	}}, []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: value},
		{Type: MutationDelete, Key: taskActiveOperationKey(task.OperationID)},
		{Type: MutationDelete, Key: taskQueueKey(task.Executor, task.ID)},
	})
	if err != nil || !result.Succeeded {
		t.Fatalf("terminal Task transaction = %#v, %v", result, err)
	}
	return failed
}

func assertRequirementGateRetryOutcome(
	t *testing.T,
	result IdempotencyTransactionResult,
	want IdempotencyKnownOutcome,
) {
	t.Helper()
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != want {
		t.Fatalf("retry outcome/conflict/error = %v/%v/%v, want %v", outcome, conflict, err, want)
	}
}

func readBlueprintRequirementGateForTest(
	t *testing.T,
	store *memoryTaskStore,
	taskID string,
) (BlueprintRequirementGate, int64) {
	t.Helper()
	read, err := store.Get(context.Background(), blueprintRequirementGateKey(taskID))
	if err != nil || read.Entry == nil {
		t.Fatalf("gate read = %#v, %v", read, err)
	}
	gate, err := decodeBlueprintRequirementGate(read.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	return gate, read.Entry.ModRevision
}
