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

type blueprintRequirementGateTestStore interface {
	Transact(context.Context, []Condition, []Mutation) (TransactionResult, error)
}

// Rationale: only the exact acknowledged prerequisite may carry a pending Blueprint gate across an
// Environment epoch advance; unrelated mutation must remain permanent conflict evidence.
func TestBlueprintRequirementGateClaimEpochAllowsOnlyExactPrerequisiteAcknowledgements(t *testing.T) {
	for _, test := range []struct {
		name           string
		unrelatedEpoch bool
	}{
		{name: "exact prerequisite"},
		{name: "unrelated epoch before acknowledgement", unrelatedEpoch: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newAttachTestStore()
			scope := seedAttachScope(t, ctx, store)
			attaches, err := NewAttachRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			tasks, err := newTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			attach, facts := testPendingAttach(t, scope, 920, "epoch-gate-db", nil)
			createTestAttach(t, ctx, attaches, scope, attach, &facts)
			agentID := ids.NewAt(ids.KindAgent, attach.CreatedAt, 921)
			claimedAttach, found, err := tasks.ClaimNextTask(
				ctx, agentID, 1, attach.CreatedAt.Add(time.Second),
			)
			if err != nil || !found || claimedAttach.Task.Record.ID != attach.TaskID {
				t.Fatalf("claim Attach = %#v, %t, %v", claimedAttach, found, err)
			}
			provisioning, err := attaches.GetAttach(ctx, attach.ID)
			if err != nil || provisioning.Record.Status != core.AttachProvisioning {
				t.Fatalf("provisioning Attach = %#v, %v", provisioning, err)
			}

			blueprintTask, publicationRevision := publishBlueprintRequirementCandidateForAttach(
				t, store, scope, provisioning, core.RequirementReady,
			)
			if test.unrelatedEpoch {
				epochValue, encodeErr := encodeEnvironmentMutationEpochRecord(EnvironmentMutationEpochRecord{
					EnvironmentID: scope.Environment.Record.ID,
				})
				if encodeErr != nil {
					t.Fatal(encodeErr)
				}
				advanced, advanceErr := store.Transact(ctx, nil, []Mutation{{
					Type: MutationPut, Key: environmentMutationEpochKey(scope.Environment.Record.ID), Value: epochValue,
				}})
				if advanceErr != nil || !advanced.Succeeded {
					t.Fatalf("advance unrelated epoch = %#v, %v", advanced, advanceErr)
				}
			}

			terminal, err := tasks.AcknowledgeTask(
				ctx,
				agentID,
				1,
				attach.TaskID,
				claimedAttach.Assignment.Record.AssignmentID,
				TaskStatusCompleted,
				completedComposeTaskResult(),
				attach.CreatedAt.Add(2*time.Second),
			)
			if err != nil || terminal.Record.Status != TaskStatusCompleted {
				t.Fatalf("acknowledge Attach = %#v, %v", terminal, err)
			}
			gateAndEpoch, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
				blueprintRequirementGateKey(blueprintTask.ID),
				environmentMutationEpochKey(scope.Environment.Record.ID),
			}})
			if err != nil || gateAndEpoch.Values[0] == nil || gateAndEpoch.Values[1] == nil ||
				gateAndEpoch.Values[1].ModRevision != terminal.Revision {
				t.Fatalf("gate/epoch after Attach acknowledgement = %#v, %v", gateAndEpoch, err)
			}

			claimedBlueprint, found, claimErr := tasks.ClaimNextTask(
				ctx, agentID, 2, blueprintTask.CreatedAt.Add(time.Second),
			)
			if test.unrelatedEpoch {
				if claimErr == nil || found || !isKind(claimErr, errs.KindStateConflict) {
					t.Fatalf("claim Blueprint after unrelated epoch = %#v, %t, %v", claimedBlueprint, found, claimErr)
				}
				if gateAndEpoch.Values[0].ModRevision != publicationRevision {
					t.Fatalf(
						"gate revision after unrelated mutation = %d, want publication %d",
						gateAndEpoch.Values[0].ModRevision,
						publicationRevision,
					)
				}
				pending, getErr := tasks.GetTask(ctx, blueprintTask.ID)
				if getErr != nil || pending.Record.Status != TaskStatusPending {
					t.Fatalf("Blueprint after rejected claim = %#v, %v", pending, getErr)
				}
				return
			}
			if claimErr != nil || !found || claimedBlueprint.Task.Record.ID != blueprintTask.ID {
				t.Fatalf("claim Blueprint after exact prerequisite = %#v, %t, %v", claimedBlueprint, found, claimErr)
			}
			if gateAndEpoch.Values[0].ModRevision != terminal.Revision {
				t.Fatalf(
					"gate revision after exact prerequisite = %d, want terminal %d",
					gateAndEpoch.Values[0].ModRevision,
					terminal.Revision,
				)
			}
			writerAndEpoch, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
				taskMaterializationWriterKey(scope.Environment.Record.ID),
				environmentMutationEpochKey(scope.Environment.Record.ID),
			}})
			if err != nil || writerAndEpoch.Values[0] == nil || writerAndEpoch.Values[1] == nil ||
				writerAndEpoch.Values[0].ModRevision != claimedBlueprint.Assignment.Revision ||
				writerAndEpoch.Values[1].ModRevision != claimedBlueprint.Assignment.Revision {
				t.Fatalf("Blueprint writer/epoch transfer = %#v, %v", writerAndEpoch, err)
			}
		})
	}
}

// Rationale: a failed or aborted prerequisite Attach still exists, but it must not carry gate authority
// across its terminal epoch advance and make an exists-gated Blueprint claimable.
func TestBlueprintRequirementGateNonSuccessPrerequisiteNeverRefreshesEpoch(t *testing.T) {
	for _, terminalStatus := range []TaskStatus{TaskStatusFailed, TaskStatusAborted} {
		t.Run(string(terminalStatus), func(t *testing.T) {
			ctx := context.Background()
			store := newAttachTestStore()
			scope := seedAttachScope(t, ctx, store)
			attaches, err := NewAttachRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			tasks, err := newTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			attach, facts := testPendingAttach(t, scope, 940, "non-success-gate-db", nil)
			created := createTestAttach(t, ctx, attaches, scope, attach, &facts)
			agentID := ids.NewAt(ids.KindAgent, attach.CreatedAt, 941)
			var assignmentID string
			prerequisite := created
			if terminalStatus == TaskStatusFailed {
				claim, found, claimErr := tasks.ClaimNextTask(
					ctx, agentID, 1, attach.CreatedAt.Add(time.Second),
				)
				if claimErr != nil || !found || claim.Task.Record.ID != attach.TaskID {
					t.Fatalf("claim Attach = %#v, %t, %v", claim, found, claimErr)
				}
				assignmentID = claim.Assignment.Record.AssignmentID
				prerequisite, err = attaches.GetAttach(ctx, attach.ID)
				if err != nil || prerequisite.Record.Status != core.AttachProvisioning {
					t.Fatalf("provisioning Attach = %#v, %v", prerequisite, err)
				}
			}
			blueprintTask, publicationRevision := publishBlueprintRequirementCandidateForAttach(
				t, store, scope, prerequisite, core.RequirementExists,
			)

			var terminal Versioned[TaskRecord]
			if terminalStatus == TaskStatusAborted {
				terminal, err = tasks.AbortPendingTask(
					ctx, attach.TaskID, attach.CreatedAt.Add(2*time.Second),
				)
			} else {
				terminal, err = tasks.AcknowledgeTask(
					ctx,
					agentID,
					1,
					attach.TaskID,
					assignmentID,
					TaskStatusFailed,
					TaskResultRecord{
						Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone,
						FailedStepID:           attachTaskStepIDForTest(t, tasks, attach.TaskID),
						ReconciliationRequired: true,
					},
					attach.CreatedAt.Add(2*time.Second),
				)
			}
			if err != nil || terminal.Record.Status != terminalStatus {
				t.Fatalf("terminalize Attach = %#v, %v", terminal, err)
			}
			gateAndEpoch, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
				blueprintRequirementGateKey(blueprintTask.ID),
				environmentMutationEpochKey(scope.Environment.Record.ID),
			}})
			if err != nil || gateAndEpoch.Values[0] == nil || gateAndEpoch.Values[1] == nil ||
				gateAndEpoch.Values[0].ModRevision != publicationRevision ||
				gateAndEpoch.Values[1].ModRevision != terminal.Revision {
				t.Fatalf("non-success gate/epoch = %#v, %v", gateAndEpoch, err)
			}
			claimed, found, claimErr := tasks.ClaimNextTask(
				ctx, agentID, 2, blueprintTask.CreatedAt.Add(time.Second),
			)
			if claimErr == nil || found || !isKind(claimErr, errs.KindStateConflict) {
				t.Fatalf("claim Blueprint after non-success = %#v, %t, %v", claimed, found, claimErr)
			}
			pending, err := tasks.GetTask(ctx, blueprintTask.ID)
			if err != nil || pending.Record.Status != TaskStatusPending {
				t.Fatalf("Blueprint after non-success = %#v, %v", pending, err)
			}
		})
	}
}

func attachTaskStepIDForTest(t *testing.T, tasks *TaskRepository, taskID string) string {
	t.Helper()
	task, err := tasks.GetTask(context.Background(), taskID)
	if err != nil || len(task.Record.Steps) == 0 {
		t.Fatalf("get Attach Task step = %#v, %v", task, err)
	}
	return task.Record.Steps[0].ID
}

func publishBlueprintRequirementCandidateForAttach(
	t *testing.T,
	store blueprintRequirementGateTestStore,
	scope AttachCreateScope,
	attach Versioned[AttachRecord],
	condition core.RequirementCondition,
) (TaskRecord, int64) {
	t.Helper()
	task := environmentBlueprintTestTask(t, scope.Project.Record, scope.Environment.Record, 930)
	task.RenderGeneration = int32(scope.ComposeProjection.Record.RenderGeneration + 1)
	publicationID := ids.NewULID()
	task.Params[TaskReleasePublicationParam] = publicationID
	requirements := core.BlueprintRequirements{
		Authored: []core.Requirement{{
			Target: core.RequirementTarget{
				Kind: core.RequirementTargetBackingAttach, Name: attach.Record.Name,
			},
			Condition: condition,
			Phases:    []core.RequirementPhase{core.RequirementPhaseDeploy},
		}},
		Resolved: []core.ResolvedRequirement{{
			Target: core.ResolvedRequirementTarget{
				Kind: core.RequirementTargetBackingAttach, Name: attach.Record.Name,
				ID: attach.Record.ID, TaskID: attach.Record.TaskID, Revision: attach.Revision,
			},
			Condition: condition,
			Phases:    []core.RequirementPhase{core.RequirementPhaseDeploy},
		}},
		ResolutionRevision: attach.ReadRevision,
	}
	dag, err := core.BuildBlueprintRequirementDAG(
		task.ID, requirements, []string{task.Steps[0].ID}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	gate, err := NewBlueprintRequirementGate(task, requirements.ResolutionRevision, dag)
	if err != nil {
		t.Fatal(err)
	}
	task.Params[TaskBlueprintRequirementGateSHA256Param] = gate.DAGDigest
	seedBlueprintRequirementTask(t, store, task)
	gateValue, err := encodeBlueprintRequirementGate(gate)
	if err != nil {
		t.Fatal(err)
	}
	markerValue, err := encodeReleaseRecord("release-publication", ReleasePublicationMarker{
		PublicationID:  publicationID,
		OperationID:    task.OperationID,
		ManifestDigest: gate.DAGDigest,
		PublishedAt:    task.CreatedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	headValue, err := encodeTaskReference(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	epochValue, err := encodeEnvironmentMutationEpochRecord(EnvironmentMutationEpochRecord{
		EnvironmentID: scope.Environment.Record.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	published, err := store.Transact(context.Background(), nil, []Mutation{
		{Type: MutationPut, Key: releasePublicationKey(publicationID), Value: markerValue},
		{Type: MutationPut, Key: environmentBlueprintHeadKey(scope.Environment.Record.ID), Value: headValue},
		{Type: MutationPut, Key: blueprintRequirementGateKey(task.ID), Value: gateValue},
		{Type: MutationPut, Key: environmentMutationEpochKey(scope.Environment.Record.ID), Value: epochValue},
	})
	if err != nil || !published.Succeeded {
		t.Fatalf("publish Blueprint candidate = %#v, %v", published, err)
	}
	return task, published.Revision
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

func seedBlueprintRequirementTask(t *testing.T, store blueprintRequirementGateTestStore, task TaskRecord) {
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
	_, classified, err := composeEnvironmentBlueprintComponentPublication(
		[]Condition{{Key: "/test/requirement"}},
		requirementClassifier,
		componentPublication,
	)
	if err != nil {
		t.Fatal(err)
	}
	err = classified(0, []*KeyValue{nil, nil})
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
