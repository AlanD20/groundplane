package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestTaskAssignmentCodecIsStrict(t *testing.T) {
	t.Parallel()

	now := taskJournalTime()
	record := testtaskassignments.TaskAssignmentRecord{
		AssignmentID: ids.NewAt(ids.KindAssignment, now, 3),
		TaskID:       ids.NewAt(ids.KindTask, now, 1), Executor: testtaskjournal.TaskExecutorAgent,
		AgentID:         ids.NewAt(ids.KindAgent, now, 2),
		AgentGeneration: 7, ClaimedTaskRevision: 41, AssignedAt: now, Deadline: now.Add(time.Minute),
		RecoveryDeadline: now.Add(
			2 * time.Minute,
		), ExecutionMode: testtaskassignments.TaskExecutionModeForward, ExecutionEpoch: 1,
	}
	value, err := testtaskassignments.EncodeTaskAssignment(record)
	if err != nil {
		t.Fatalf("encodeTaskAssignment() error = %v", err)
	}
	decoded, err := testtaskassignments.DecodeTaskAssignment(value)
	if err != nil || decoded != record {
		t.Fatalf("decodeTaskAssignment() = %#v, %v", decoded, err)
	}
	duplicate := bytes.Replace(value, []byte(`"schema":3`), []byte(`"schema":3,"schema":3`), 1)
	if _, err := testtaskassignments.DecodeTaskAssignment(duplicate); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("decodeTaskAssignment(duplicate) error = %v, want internal", err)
	}
	unknown := bytes.Replace(value, []byte(`"schema":3`), []byte(`"schema":3,"extra":true`), 1)
	if _, err := testtaskassignments.DecodeTaskAssignment(unknown); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("decodeTaskAssignment(unknown) error = %v, want internal", err)
	}
	missingID := bytes.Replace(
		value,
		[]byte(`"assignment_id":"`+record.AssignmentID+`",`),
		nil,
		1,
	)
	if _, err := testtaskassignments.DecodeTaskAssignment(missingID); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("decodeTaskAssignment(missing assignment id) error = %v, want internal", err)
	}
	malformedID := bytes.Replace(value, []byte(record.AssignmentID), []byte(record.TaskID), 1)
	if _, err := testtaskassignments.DecodeTaskAssignment(malformedID); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("decodeTaskAssignment(malformed assignment id) error = %v, want internal", err)
	}
	controller := record
	controller.Executor = testtaskjournal.TaskExecutorController
	controller.AgentID = ""
	controller.AgentGeneration = 0
	controllerValue, err := testtaskassignments.EncodeTaskAssignment(controller)
	if err != nil {
		t.Fatalf("encodeTaskAssignment(controller) error = %v", err)
	}
	decodedController, err := testtaskassignments.DecodeTaskAssignment(controllerValue)
	if err != nil || decodedController != controller {
		t.Fatalf("decodeTaskAssignment(controller) = %#v, %v", decodedController, err)
	}
}

// Rationale: CoreDNS terminal replay must distinguish every persisted proof
// field, including exact byte content and nilness.
func TestTaskResultsEqualComparesDNSResolverEvidenceExactly(t *testing.T) {
	t.Parallel()

	observedAt := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	evidence := &testtaskjournal.TaskDNSResolverObservationEvidence{
		ComponentID:             "component",
		ServiceID:               "service",
		ArtifactID:              "artifact",
		ArtifactSHA256:          "artifact-sha",
		RenderGeneration:        1,
		ImageReference:          "coredns/coredns@sha256:image",
		VerifiedImageDigest:     "image-digest",
		ImageConfigDigest:       "config-digest",
		ListenEndpoint:          "127.0.0.1:53",
		ReloadSHA512:            "reload-sha",
		ObservedAt:              observedAt,
		StaticQueryPresent:      true,
		StaticQueryName:         "static.example.",
		StaticQueryIPv4:         "192.0.2.1",
		StaticQuerySucceeded:    true,
		RecursiveQuerySucceeded: true,
		ForwarderQueryCount:     2,
		ForwarderSuccessCount:   2,
		ProofSHA256:             "proof-sha",
		CanonicalEvidence:       []byte("canonical-proof"),
	}
	left := testtaskjournal.TaskResultRecord{
		Kind:                            testtaskjournal.TaskResultCompose,
		DNSResolverCandidateObservation: evidence,
	}
	equivalent := testtaskjournal.CloneTaskResult(&left)
	changedBytes := testtaskjournal.CloneTaskResult(&left)
	changedBytes.DNSResolverCandidateObservation.CanonicalEvidence = []byte("different-proof")
	changedTimestamp := testtaskjournal.CloneTaskResult(&left)
	changedTimestamp.DNSResolverCandidateObservation.ObservedAt = observedAt.Add(time.Second)
	emptyBytes := testtaskjournal.CloneTaskResult(&left)
	emptyBytes.DNSResolverCandidateObservation.CanonicalEvidence = []byte{}
	nilEvidence := testtaskjournal.TaskResultRecord{Kind: testtaskjournal.TaskResultCompose}

	tests := []struct {
		name  string
		right testtaskjournal.TaskResultRecord
		want  bool
	}{
		{name: "equivalent evidence", right: *equivalent, want: true},
		{name: "changed proof bytes", right: *changedBytes, want: false},
		{name: "changed observed timestamp", right: *changedTimestamp, want: false},
		{name: "empty proof bytes", right: *emptyBytes, want: false},
		{name: "missing evidence", right: nilEvidence, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := testtaskjournal.TaskResultsEqual(left, test.right); got != test.want {
				t.Fatalf("taskResultsEqual() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestTaskRepositoryCreatesAndReplaysAtomicTask(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	task := validTaskRecord(taskJournalTime())
	marker := pendingTaskMarker(task)

	result, err := repository.CreateTask(ctx, task, marker)
	if err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("CreateTask() outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskStorageKey(task.ID), true)
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskOperationIndexKey(task.OperationID, task.ID), true)
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskActiveOperationKey(task.OperationID), true)
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskQueueKey(task.Executor, task.ID), true)
	markerKey, keyErr := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if keyErr != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", keyErr)
	}
	assertTaskLifecycleValue(t, store, markerKey, true)

	persisted, err := repository.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask() error = %v", err)
	}
	if persisted.Record.idempotencyMarker == nil ||
		*persisted.Record.idempotencyMarker != marker.Locator ||
		persisted.Record.IdempotencyKey != marker.Locator.Key {
		t.Fatalf("persisted Task marker locator = %#v", persisted.Record.idempotencyMarker)
	}

	replay, err := repository.CreateTask(ctx, task, marker)
	if err != nil {
		t.Fatalf("CreateTask(replay) error = %v", err)
	}
	replayOutcome, existing, replayConflict, replayErr := replay.Classify()
	if replayErr != nil || replayConflict != nil || replayOutcome != IdempotencyKnownExisting ||
		existing.Kind != testidempotency.IdempotencyMarkerTask || existing.State != testidempotency.IdempotencyMarkerPending ||
		existing.TaskID != task.ID {
		t.Fatalf(
			"CreateTask(replay) outcome/marker/conflict/error = %v/%#v/%v/%v",
			replayOutcome,
			existing,
			replayConflict,
			replayErr,
		)
	}
}

func TestTaskRepositoryGenericCreationRejectsNonPlatformOwner(t *testing.T) {
	// Rationale: the generic publisher has no hierarchy evidence and therefore may publish only platform/operator Tasks.
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	task := validTaskRecord(taskJournalTime())
	task.Owner = testtaskjournal.TaskOwner{
		WorkspaceType: testtaskjournal.TaskWorkspaceTenant,
		TenantID:      ids.NewAt(ids.KindTenant, task.CreatedAt, 590),
	}

	if _, err := repository.CreateTask(ctx, task, pendingTaskMarker(task)); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("CreateTask(non-platform owner) error = %v, want validation_failed", err)
	}
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskStorageKey(task.ID), false)
}

func TestTaskRepositoryOrdinaryRetryRejectsSystemActor(t *testing.T) {
	// Rationale: only a typed Controller-parent initiation may authorize a
	// system retry; the ordinary entry point is operator-only.
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	source := validTaskRecord(taskJournalTime())
	createLifecycleTask(t, repository, source)
	finishedAt := source.CreatedAt.Add(time.Second)
	if _, err := repository.AbortPendingTask(ctx, source.ID, finishedAt); err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	retryID := ids.NewAt(ids.KindTask, finishedAt.Add(time.Second), 591)
	marker := pendingRetryMarker(source, retryID, finishedAt.Add(time.Second), "system-retry-key-0002")
	if _, err := repository.RetryTask(ctx, source.ID, retryID, testtaskjournal.TaskActorSystem, marker); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("RetryTask(system actor) error = %v, want validation_failed", err)
	}
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskStorageKey(retryID), false)
}

func TestTaskRepositoryNormalizesEqualAndRegressedLifecycleTimestamps(t *testing.T) {
	// Rationale: Controller timestamps must advance monotonically even when its clock repeats or moves backward.
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	task := validTaskRecord(taskJournalTime())
	task.Executor = testtaskjournal.TaskExecutorController
	createLifecycleTask(t, repository, task)

	claim, found, err := repository.ClaimNextControllerTask(ctx, task.UpdatedAt)
	if err != nil || !found {
		t.Fatalf("ClaimNextControllerTask(equal timestamp) = %#v, %v, %v", claim, found, err)
	}
	wantStartedAt := task.UpdatedAt.Add(time.Nanosecond)
	if claim.Task.Record.StartedAt == nil || !claim.Task.Record.StartedAt.Equal(wantStartedAt) ||
		!claim.Task.Record.UpdatedAt.Equal(wantStartedAt) ||
		!claim.Assignment.Record.AssignedAt.Equal(wantStartedAt) {
		t.Fatalf("normalized claim = %#v", claim)
	}

	terminal, err := repository.AcknowledgeControllerTask(
		ctx,
		task.ID, testtaskjournal.TaskStatusCompleted, task.CreatedAt,
	)
	if err != nil {
		t.Fatalf("AcknowledgeControllerTask(regressed timestamp) error = %v", err)
	}
	wantFinishedAt := wantStartedAt.Add(time.Nanosecond)
	if terminal.Record.FinishedAt == nil || !terminal.Record.FinishedAt.Equal(wantFinishedAt) ||
		!terminal.Record.UpdatedAt.Equal(wantFinishedAt) {
		t.Fatalf("normalized terminal Task = %#v", terminal.Record)
	}
}

func TestTaskRepositoryRetriesTerminalTaskAtomically(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	source := validTaskRecord(taskJournalTime())
	createLifecycleTask(t, repository, source)
	terminalAt := source.CreatedAt.Add(time.Second)
	if _, err := repository.AbortPendingTask(ctx, source.ID, terminalAt); err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}

	retryID := ids.NewAt(ids.KindTask, terminalAt.Add(time.Second), 601)
	marker := pendingRetryMarker(source, retryID, terminalAt.Add(time.Second), "retry-request-key-0001")
	result, err := repository.RetryTask(ctx, source.ID, retryID, testtaskjournal.TaskActorOperator, marker)
	if err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("RetryTask() outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	persisted, err := repository.GetTask(ctx, retryID)
	if err != nil {
		t.Fatalf("GetTask(retry) error = %v", err)
	}
	if persisted.Record.RetryOf != source.ID || persisted.Record.OperationID != source.OperationID ||
		persisted.Record.IdempotencyKey != source.IdempotencyKey ||
		persisted.Record.Executor != source.Executor ||
		persisted.Record.Owner != source.Owner || persisted.Record.Actor != testtaskjournal.TaskActorOperator ||
		persisted.Record.PlanID != source.PlanID || persisted.Record.PlanHash != source.PlanHash ||
		persisted.Record.Status != testtaskjournal.TaskStatusPending || persisted.Record.idempotencyMarker == nil ||
		*persisted.Record.idempotencyMarker != marker.Locator {
		t.Fatalf("persisted retry = %#v", persisted.Record)
	}
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskOperationIndexKey(source.OperationID, retryID), true)
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskActiveOperationKey(source.OperationID), true)
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskQueueKey(source.Executor, retryID), true)

	replayID := ids.NewAt(ids.KindTask, terminalAt.Add(2*time.Second), 602)
	replayMarker := pendingRetryMarker(source, replayID, terminalAt.Add(2*time.Second), marker.Locator.Key)
	replay, err := repository.RetryTask(ctx, source.ID, replayID, testtaskjournal.TaskActorOperator, replayMarker)
	if err != nil {
		t.Fatalf("RetryTask(replay) error = %v", err)
	}
	replayOutcome, existing, replayConflict, replayErr := replay.Classify()
	if replayErr != nil || replayConflict != nil || replayOutcome != IdempotencyKnownExisting ||
		existing.TaskID != retryID || existing.State != testidempotency.IdempotencyMarkerPending {
		t.Fatalf(
			"RetryTask(replay) outcome/marker/conflict/error = %v/%#v/%v/%v",
			replayOutcome,
			existing,
			replayConflict,
			replayErr,
		)
	}
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskStorageKey(replayID), false)

	conflictID := ids.NewAt(ids.KindTask, terminalAt.Add(3*time.Second), 603)
	conflictMarker := pendingRetryMarker(
		source,
		conflictID,
		terminalAt.Add(3*time.Second),
		"second-retry-key-0001",
	)
	conflicted, err := repository.RetryTask(
		ctx,
		source.ID,
		conflictID,
		testtaskjournal.TaskActorOperator,
		conflictMarker,
	)
	if err != nil {
		t.Fatalf("RetryTask(active retry) error = %v", err)
	}
	conflictOutcome, _, domainConflict, conflictErr := conflicted.Classify()
	if conflictErr != nil || conflictOutcome != IdempotencyKnownConflict ||
		!errors.Is(domainConflict, errs.New(errs.KindTaskRetryInFlight, "")) {
		t.Fatalf(
			"RetryTask(active retry) outcome/conflict/error = %v/%v/%v",
			conflictOutcome,
			domainConflict,
			conflictErr,
		)
	}
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskStorageKey(conflictID), false)
}

func TestTaskRepositoryClaimsFIFOAndAcknowledgesTerminalState(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	first := validTaskRecord(taskJournalTime())
	second := validTaskRecord(taskJournalTime().Add(time.Second))
	createLifecycleTask(t, repository, first)
	createLifecycleTask(t, repository, second)
	agentID := ids.NewAt(ids.KindAgent, first.CreatedAt, 91)

	claim, found, err := repository.ClaimNextTask(
		ctx,
		agentID,
		3,
		second.CreatedAt.Add(time.Second),
	)
	if err != nil || !found {
		t.Fatalf("ClaimNextTask() = %#v, %v, %v", claim, found, err)
	}
	if claim.Task.Record.ID != first.ID || claim.Task.Record.Status != testtaskjournal.TaskStatusRunning ||
		claim.Assignment.Record.TaskID != first.ID ||
		ids.Validate(ids.KindAssignment, claim.Assignment.Record.AssignmentID) != nil ||
		claim.Assignment.Record.AgentGeneration != 3 ||
		claim.Assignment.Record.ClaimedTaskRevision <= 0 ||
		claim.Assignment.Record.ExecutionMode != testtaskassignments.TaskExecutionModeForward ||
		claim.Assignment.Record.ExecutionEpoch != 1 ||
		!claim.Assignment.Record.RecoveryDeadline.Equal(
			claim.Assignment.Record.Deadline.Add(time.Duration(first.TimeoutSeconds)*time.Second),
		) {
		t.Fatalf("ClaimNextTask() = %#v", claim)
	}
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskQueueKey(first.Executor, first.ID), false)
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskAssignmentKey(agentID, first.ID), true)
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskAssignmentIndexKey(first.ID), true)
	assertTaskLifecycleValue(
		t,
		store,
		testtaskjournal.TaskTimeoutIndexKey(first.ID, claim.Assignment.Record.Deadline),
		true,
	)
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskQueueKey(second.Executor, second.ID), true)
	recovered, err := repository.ListAgentAssignments(ctx, agentID, 3, 4)
	if err != nil || len(recovered) != 1 ||
		recovered[0].Task.Record.ID != first.ID ||
		recovered[0].Assignment.Record != claim.Assignment.Record {
		t.Fatalf("ListAgentAssignments() = %#v, %v", recovered, err)
	}

	terminalAt := second.CreatedAt.Add(2 * time.Second)
	terminal, err := repository.AcknowledgeTask(
		ctx,
		agentID,
		3,
		first.ID, taskAssignmentIDForTest(t, repository,

			first.ID), testtaskjournal.TaskStatusCompleted, completedComposeTaskResult(),
		terminalAt)

	if err != nil {
		t.Fatalf("AcknowledgeTask() error = %v", err)
	}
	if terminal.Record.Status != testtaskjournal.TaskStatusCompleted || terminal.Record.FinishedAt == nil ||
		!terminal.Record.FinishedAt.Equal(terminalAt) || terminal.Record.Result == nil {
		t.Fatalf("AcknowledgeTask() = %#v", terminal)
	}
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskAssignmentKey(agentID, first.ID), false)
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskAssignmentIndexKey(first.ID), false)
	assertTaskLifecycleValue(
		t,
		store,
		testtaskjournal.TaskTimeoutIndexKey(first.ID, claim.Assignment.Record.Deadline),
		false,
	)
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskActiveOperationKey(first.OperationID), false)
	marker := pendingTaskMarker(first)
	markerKey, keyErr := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if keyErr != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", keyErr)
	}
	retentionKey, keyErr := testidempotency.IdempotencyRetentionKey(
		markerKey,
		terminalAt.Add(testidempotency.MarkerRetention),
	)
	if keyErr != nil {
		t.Fatalf("idempotencyRetentionKey() error = %v", keyErr)
	}
	assertTaskLifecycleValue(t, store, retentionKey, true)
	assertTaskLifecycleValue(
		t,
		store,
		testtaskjournal.TaskRetentionIndexKey(first.ID, terminalAt.Add(testtaskjournal.TaskRetention)),
		true,
	)
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	evidence, err := idempotency.Read(ctx, marker.Locator)
	if err != nil {
		t.Fatalf("Read(marker) error = %v", err)
	}
	storedMarker, err := evidence.Marker()
	if err != nil || storedMarker.State != testidempotency.IdempotencyMarkerCompleted ||
		!storedMarker.TerminalAt.Equal(terminalAt) {
		t.Fatalf("terminal marker = %#v, %v", storedMarker, err)
	}

	replay, err := repository.AcknowledgeTask(
		ctx,
		agentID,
		3,
		first.ID, taskAssignmentIDForTest(t, repository,

			first.ID), testtaskjournal.TaskStatusCompleted, completedComposeTaskResult(),
		terminalAt.Add(time.Second))

	if err != nil || replay.Record.Status != testtaskjournal.TaskStatusCompleted ||
		replay.Revision != terminal.Revision {
		t.Fatalf("AcknowledgeTask(replay) = %#v, %v", replay, err)
	}
	mismatched := completedComposeTaskResult()
	mismatched.Projects = []testtaskjournal.TaskObservedProjectSummary{{
		ProjectName: "gp-platform", ObservedAt: terminalAt,
	}}
	if _, err := repository.AcknowledgeTask(
		ctx,
		agentID,
		3,
		first.ID,
		taskAssignmentIDForTest(t, repository,
			first.ID), testtaskjournal.TaskStatusCompleted, mismatched,
		terminalAt.Add(2*time.Second),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("AcknowledgeTask(mismatched replay) error = %v, want state conflict", err)
	}
}

// Rationale: a retry is a new immutable Task attempt and therefore must never
// inherit the terminal source Task's assignment identity.
func TestTaskRetryClaimAllocatesFreshAssignmentID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	source := validTaskRecord(taskJournalTime())
	createLifecycleTask(t, repository, source)
	agentID := ids.NewAt(ids.KindAgent, source.CreatedAt, 801)
	sourceClaim, found, err := repository.ClaimNextTask(
		ctx, agentID, 1, source.CreatedAt.Add(time.Second),
	)
	if err != nil || !found {
		t.Fatalf("ClaimNextTask(source) = %#v, %t, %v", sourceClaim, found, err)
	}
	failedResult := completedComposeTaskResult()
	failedResult.ExitCode = 1
	terminal, err := repository.AcknowledgeTask(
		ctx,
		agentID,
		1,
		source.ID, taskAssignmentIDForTest(t, repository,

			source.ID), testtaskjournal.TaskStatusFailed, failedResult,
		source.CreatedAt.Add(2*time.Second))

	if err != nil {
		t.Fatalf("AcknowledgeTask(source) error = %v", err)
	}
	retryAt := source.CreatedAt.Add(3 * time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 802)
	marker := pendingRetryMarker(terminal.Record, retryID, retryAt, "assignment-retry-key-0001")
	result, err := repository.RetryTask(ctx, source.ID, retryID, testtaskjournal.TaskActorOperator, marker)
	if err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("RetryTask() outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	retryClaim, found, err := repository.ClaimNextTask(ctx, agentID, 1, retryAt.Add(time.Second))
	if err != nil || !found {
		t.Fatalf("ClaimNextTask(retry) = %#v, %t, %v", retryClaim, found, err)
	}
	if retryClaim.Assignment.Record.AssignmentID == sourceClaim.Assignment.Record.AssignmentID {
		t.Fatalf("retry reused assignment id %q", retryClaim.Assignment.Record.AssignmentID)
	}
}

// Rationale: a committed claim whose response is lost must retain exactly one
// assignment identity for lookup and reconnect instead of allocating again.
func TestTaskClaimUnknownOutcomeRecoversCommittedAssignmentID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	task := validTaskRecord(taskJournalTime())
	createLifecycleTask(t, repository, task)
	agentID := ids.NewAt(ids.KindAgent, task.CreatedAt, 811)
	unknown := errs.New(errs.KindStorageUnavailable, "unknown Task claim outcome")
	store.failAfterCommit(unknown)
	if _, _, err := repository.ClaimNextTask(
		ctx, agentID, 5, task.CreatedAt.Add(time.Second),
	); !errors.Is(err, unknown) {
		t.Fatalf("ClaimNextTask(unknown) error = %v", err)
	}
	recovered, err := repository.GetTaskAssignment(ctx, task.ID)
	if err != nil || ids.Validate(ids.KindAssignment, recovered.Assignment.Record.AssignmentID) != nil {
		t.Fatalf("GetTaskAssignment(after unknown) = %#v, %v", recovered, err)
	}
	reconnected, err := repository.ListAgentAssignments(ctx, agentID, 5, 1)
	if err != nil || len(reconnected) != 1 ||
		reconnected[0].Assignment.Record.AssignmentID != recovered.Assignment.Record.AssignmentID {
		t.Fatalf("ListAgentAssignments(after unknown) = %#v, %v", reconnected, err)
	}
	if another, found, err := repository.ClaimNextTask(
		ctx, agentID, 5, task.CreatedAt.Add(2*time.Second),
	); err != nil || found || another.Assignment.Record.AssignmentID != "" {
		t.Fatalf("ClaimNextTask(after unknown) = %#v, %t, %v", another, found, err)
	}
}

func TestTaskRepositoryTimesOutExactAgentGenerationAssignments(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	first := validTaskRecord(taskJournalTime())
	second := validTaskRecord(taskJournalTime().Add(time.Second))
	createLifecycleTask(t, repository, first)
	createLifecycleTask(t, repository, second)
	agentID := ids.NewAt(ids.KindAgent, first.CreatedAt, 701)
	if _, found, err := repository.ClaimNextTask(
		ctx,
		agentID,
		9,
		second.CreatedAt.Add(time.Second),
	); err != nil ||
		!found {
		t.Fatalf("ClaimNextTask(first) found/error = %v/%v", found, err)
	}
	if _, found, err := repository.ClaimNextTask(
		ctx,
		agentID,
		9,
		second.CreatedAt.Add(2*time.Second),
	); err != nil ||
		!found {
		t.Fatalf("ClaimNextTask(second) found/error = %v/%v", found, err)
	}

	terminalAt := second.CreatedAt.Add(time.Duration(second.TimeoutSeconds+3) * time.Second)
	timedOut, err := repository.TimeoutAgentAssignments(ctx, agentID, 9, 2, terminalAt)
	if err != nil || timedOut != 2 {
		t.Fatalf("TimeoutAgentAssignments() count/error = %d/%v", timedOut, err)
	}
	for _, taskID := range []string{first.ID, second.ID} {
		task, getErr := repository.GetTask(ctx, taskID)
		if getErr != nil {
			t.Fatalf("GetTask(%s) error = %v", taskID, getErr)
		}
		if task.Record.Status != testtaskjournal.TaskStatusTimedOut || task.Record.FinishedAt == nil ||
			!task.Record.FinishedAt.Equal(terminalAt) || task.Record.Result == nil ||
			!task.Record.Result.ReconciliationRequired {
			t.Fatalf("timed-out Task %s = %#v", taskID, task.Record)
		}
	}
	remaining, err := repository.ListAgentAssignments(ctx, agentID, 9, 2)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("ListAgentAssignments(after timeout) = %#v, %v", remaining, err)
	}
}

func TestTaskRepositoryIsolatesControllerAndAgentExecutionClaims(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime()
	controllerTask := validTaskRecord(now)
	controllerTask.Executor = testtaskjournal.TaskExecutorController
	agentTask := validTaskRecord(now.Add(time.Second))
	createLifecycleTask(t, repository, controllerTask)
	createLifecycleTask(t, repository, agentTask)

	agentID := ids.NewAt(ids.KindAgent, now, 701)
	agentClaim, found, err := repository.ClaimNextTask(ctx, agentID, 4, now.Add(2*time.Second))
	if err != nil || !found || agentClaim.Task.Record.ID != agentTask.ID ||
		agentClaim.Assignment.Record.Executor != testtaskjournal.TaskExecutorAgent {
		t.Fatalf("ClaimNextTask() = %#v, %v, %v", agentClaim, found, err)
	}
	controllerClaim, found, err := repository.ClaimNextControllerTask(ctx, now.Add(2*time.Second))
	if err != nil || !found || controllerClaim.Task.Record.ID != controllerTask.ID ||
		controllerClaim.Assignment.Record.Executor != testtaskjournal.TaskExecutorController ||
		controllerClaim.Assignment.Record.AgentID != "" || controllerClaim.Assignment.Record.AgentGeneration != 0 {
		t.Fatalf("ClaimNextControllerTask() = %#v, %v, %v", controllerClaim, found, err)
	}
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskAssignmentKey(agentID, agentTask.ID), true)
	assertTaskLifecycleValue(t, store, testtaskjournal.ControllerTaskClaimKey(controllerTask.ID), true)
	recovered, err := repository.ListControllerTaskClaims(ctx)
	if err != nil || len(recovered) != 1 ||
		recovered[0].Task.Record.ID != controllerTask.ID ||
		recovered[0].Assignment.Record != controllerClaim.Assignment.Record {
		t.Fatalf("ListControllerTaskClaims() = %#v, %v", recovered, err)
	}

	terminalAt := now.Add(3 * time.Second)
	terminal, err := repository.AcknowledgeControllerTask(
		ctx,
		controllerTask.ID, testtaskjournal.TaskStatusCompleted, terminalAt,
	)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusCompleted || terminal.Record.Result != nil {
		t.Fatalf("AcknowledgeControllerTask() = %#v, %v", terminal, err)
	}
	assertTaskLifecycleValue(t, store, testtaskjournal.ControllerTaskClaimKey(controllerTask.ID), false)
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskAssignmentIndexKey(controllerTask.ID), false)
	assertTaskLifecycleValue(
		t,
		store,
		testtaskjournal.TaskTimeoutIndexKey(controllerTask.ID, controllerClaim.Assignment.Record.Deadline),
		false,
	)
	replayed, err := repository.AcknowledgeControllerTask(
		ctx,
		controllerTask.ID, testtaskjournal.TaskStatusCompleted, terminalAt,
	)
	if err != nil || replayed.Record.Status != testtaskjournal.TaskStatusCompleted || replayed.Record.Result != nil {
		t.Fatalf("AcknowledgeControllerTask(replay) = %#v, %v", replayed, err)
	}
	recovered, err = repository.ListControllerTaskClaims(ctx)
	if err != nil || len(recovered) != 0 {
		t.Fatalf("ListControllerTaskClaims(terminal) = %#v, %v", recovered, err)
	}
}

func TestTaskRepositoryRejectsStaleGenerationAndAbortsPendingTask(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	runningTask := validTaskRecord(taskJournalTime())
	createLifecycleTask(t, repository, runningTask)
	agentID := ids.NewAt(ids.KindAgent, runningTask.CreatedAt, 101)
	if _, found, err := repository.ClaimNextTask(
		ctx,
		agentID,
		8,
		runningTask.CreatedAt.Add(time.Second),
	); err != nil || !found {
		t.Fatalf("ClaimNextTask() found/error = %v/%v", found, err)
	}
	if _, err := repository.AcknowledgeTask(
		ctx,
		agentID,
		9,
		runningTask.ID, taskAssignmentIDForTest(t, repository,

			runningTask.ID), testtaskjournal.TaskStatusCompleted, completedComposeTaskResult(),
		runningTask.CreatedAt.Add(2*time.Second)); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("AcknowledgeTask(stale generation) error = %v, want state.conflict", err)
	}
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskAssignmentKey(agentID, runningTask.ID), true)

	pendingTask := validTaskRecord(taskJournalTime().Add(10 * time.Second))
	createLifecycleTask(t, repository, pendingTask)
	terminalAt := pendingTask.CreatedAt.Add(time.Second)
	aborted, err := repository.AbortPendingTask(ctx, pendingTask.ID, terminalAt)
	if err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	if aborted.Record.Status != testtaskjournal.TaskStatusAborted {
		t.Fatalf("AbortPendingTask() = %#v", aborted)
	}
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskQueueKey(pendingTask.Executor, pendingTask.ID), false)
	assertTaskLifecycleValue(t, store, testtaskjournal.TaskActiveOperationKey(pendingTask.OperationID), false)
	marker := pendingTaskMarker(pendingTask)
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	evidence, err := idempotency.Read(ctx, marker.Locator)
	if err != nil {
		t.Fatalf("Read(aborted marker) error = %v", err)
	}
	storedMarker, err := evidence.Marker()
	if err != nil || storedMarker.State != testidempotency.IdempotencyMarkerFailed {
		t.Fatalf("aborted marker = %#v, %v", storedMarker, err)
	}
}

func createLifecycleTask(t *testing.T, repository *TaskRepository, task TaskRecord) {
	t.Helper()
	result, err := repository.CreateTask(context.Background(), task, pendingTaskMarker(task))
	if err != nil {
		t.Fatalf("CreateTask(%s) error = %v", task.ID, err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf(
			"CreateTask(%s) outcome/conflict/error = %v/%v/%v",
			task.ID,
			outcome,
			conflict,
			classifyErr,
		)
	}
}

func pendingTaskMarker(task TaskRecord) testidempotency.IdempotencyMarker {
	ciphertext := []byte("protected-task-intent")
	digest := sha256.Sum256(ciphertext)
	body, _ := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: task.ID})
	return testidempotency.IdempotencyMarker{
		Kind: testidempotency.IdempotencyMarkerTask, State: testidempotency.IdempotencyMarkerPending,
		Locator: testidempotency.IdempotencyLocator{
			ScopeKind: testidempotency.IdempotencyScopeEnvironment,
			ScopeID:   ids.NewAt(ids.KindEnvironment, task.CreatedAt, 501),
			Method:    http.MethodPost,
			Route:     "/environments/{environment}/tasks",
			Key:       task.IdempotencyKey,
		},
		Intent: testidempotency.ProtectedIntentRecord{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
		},
		Response: testidempotency.IdempotencyResponse{
			Status: http.StatusAccepted, ContentKind: "application/json", Body: body,
		},
		TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
}

func pendingRetryMarker(
	source TaskRecord,
	retryID string,
	createdAt time.Time,
	key string,
) testidempotency.IdempotencyMarker {
	marker := pendingTaskMarker(source)
	marker.Locator.Method = http.MethodPost
	marker.Locator.Route = "/tasks/{id}/retry"
	marker.Locator.Key = key
	marker.TaskID = retryID
	marker.CreatedAt = createdAt
	marker.UpdatedAt = createdAt
	marker.Response.Body, _ = json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: retryID})
	return marker
}

func assertTaskLifecycleValue(t *testing.T, store *memoryTaskStore, key string, want bool) {
	t.Helper()
	result, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%s) error = %v", key, err)
	}
	if got := result.Entry != nil; got != want {
		t.Fatalf("Get(%s) present = %v, want %v", key, got, want)
	}
}
