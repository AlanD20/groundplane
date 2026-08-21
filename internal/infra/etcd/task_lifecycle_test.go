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
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestTaskAssignmentCodecIsStrict(t *testing.T) {
	t.Parallel()

	now := taskJournalTime()
	record := TaskAssignmentRecord{
		TaskID: ids.NewAt(ids.KindTask, now, 1), AgentID: ids.NewAt(ids.KindAgent, now, 2),
		AgentGeneration: 7, ClaimedTaskRevision: 41, AssignedAt: now,
	}
	value, err := encodeTaskAssignment(record)
	if err != nil {
		t.Fatalf("encodeTaskAssignment() error = %v", err)
	}
	decoded, err := decodeTaskAssignment(value)
	if err != nil || decoded != record {
		t.Fatalf("decodeTaskAssignment() = %#v, %v", decoded, err)
	}
	duplicate := bytes.Replace(value, []byte(`"schema":1`), []byte(`"schema":1,"schema":1`), 1)
	if _, err := decodeTaskAssignment(duplicate); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("decodeTaskAssignment(duplicate) error = %v, want internal", err)
	}
	unknown := bytes.Replace(value, []byte(`"schema":1`), []byte(`"schema":1,"extra":true`), 1)
	if _, err := decodeTaskAssignment(unknown); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("decodeTaskAssignment(unknown) error = %v, want internal", err)
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
	assertTaskLifecycleValue(t, store, taskKey(task.ID), true)
	assertTaskLifecycleValue(t, store, taskOperationIndexKey(task.OperationID, task.ID), true)
	assertTaskLifecycleValue(t, store, taskActiveOperationKey(task.OperationID), true)
	assertTaskLifecycleValue(t, store, taskQueueKey(task.ID), true)
	markerKey, keyErr := idempotencyMarkerKey(marker.Locator)
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
		existing.Kind != IdempotencyMarkerTask || existing.State != IdempotencyMarkerPending ||
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
	if claim.Task.Record.ID != first.ID || claim.Task.Record.Status != TaskStatusRunning ||
		claim.Assignment.Record.TaskID != first.ID ||
		claim.Assignment.Record.AgentGeneration != 3 ||
		claim.Assignment.Record.ClaimedTaskRevision <= 0 {
		t.Fatalf("ClaimNextTask() = %#v", claim)
	}
	assertTaskLifecycleValue(t, store, taskQueueKey(first.ID), false)
	assertTaskLifecycleValue(t, store, taskAssignmentKey(agentID, first.ID), true)
	assertTaskLifecycleValue(t, store, taskQueueKey(second.ID), true)

	terminalAt := second.CreatedAt.Add(2 * time.Second)
	terminal, err := repository.AcknowledgeTask(
		ctx,
		agentID,
		3,
		first.ID,
		TaskStatusCompleted,
		terminalAt,
	)
	if err != nil {
		t.Fatalf("AcknowledgeTask() error = %v", err)
	}
	if terminal.Record.Status != TaskStatusCompleted || terminal.Record.TerminalAt == nil ||
		!terminal.Record.TerminalAt.Equal(terminalAt) {
		t.Fatalf("AcknowledgeTask() = %#v", terminal)
	}
	assertTaskLifecycleValue(t, store, taskAssignmentKey(agentID, first.ID), false)
	assertTaskLifecycleValue(t, store, taskActiveOperationKey(first.OperationID), false)
	marker := pendingTaskMarker(first)
	markerKey, keyErr := idempotencyMarkerKey(marker.Locator)
	if keyErr != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", keyErr)
	}
	retentionKey, keyErr := idempotencyRetentionKey(markerKey, terminalAt.Add(markerRetention))
	if keyErr != nil {
		t.Fatalf("idempotencyRetentionKey() error = %v", keyErr)
	}
	assertTaskLifecycleValue(t, store, retentionKey, true)
	idempotency, err := newIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	evidence, err := idempotency.Read(ctx, marker.Locator)
	if err != nil {
		t.Fatalf("Read(marker) error = %v", err)
	}
	storedMarker, err := evidence.Marker()
	if err != nil || storedMarker.State != IdempotencyMarkerCompleted ||
		!storedMarker.TerminalAt.Equal(terminalAt) {
		t.Fatalf("terminal marker = %#v, %v", storedMarker, err)
	}

	replay, err := repository.AcknowledgeTask(
		ctx,
		agentID,
		3,
		first.ID,
		TaskStatusCompleted,
		terminalAt.Add(time.Second),
	)
	if err != nil || replay.Record.Status != TaskStatusCompleted ||
		replay.Revision != terminal.Revision {
		t.Fatalf("AcknowledgeTask(replay) = %#v, %v", replay, err)
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
		runningTask.ID,
		TaskStatusCompleted,
		runningTask.CreatedAt.Add(2*time.Second),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("AcknowledgeTask(stale generation) error = %v, want state.conflict", err)
	}
	assertTaskLifecycleValue(t, store, taskAssignmentKey(agentID, runningTask.ID), true)

	pendingTask := validTaskRecord(taskJournalTime().Add(10 * time.Second))
	createLifecycleTask(t, repository, pendingTask)
	terminalAt := pendingTask.CreatedAt.Add(time.Second)
	aborted, err := repository.AbortPendingTask(ctx, pendingTask.ID, terminalAt)
	if err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	if aborted.Record.Status != TaskStatusAborted {
		t.Fatalf("AbortPendingTask() = %#v", aborted)
	}
	assertTaskLifecycleValue(t, store, taskQueueKey(pendingTask.ID), false)
	assertTaskLifecycleValue(t, store, taskActiveOperationKey(pendingTask.OperationID), false)
	marker := pendingTaskMarker(pendingTask)
	idempotency, err := newIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	evidence, err := idempotency.Read(ctx, marker.Locator)
	if err != nil {
		t.Fatalf("Read(aborted marker) error = %v", err)
	}
	storedMarker, err := evidence.Marker()
	if err != nil || storedMarker.State != IdempotencyMarkerFailed {
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

func pendingTaskMarker(task TaskRecord) IdempotencyMarker {
	ciphertext := []byte("protected-task-intent")
	digest := sha256.Sum256(ciphertext)
	body, _ := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: task.ID})
	return IdempotencyMarker{
		Kind: IdempotencyMarkerTask, State: IdempotencyMarkerPending,
		Locator: IdempotencyLocator{
			ScopeKind: IdempotencyScopeEnvironment,
			ScopeID:   ids.NewAt(ids.KindEnvironment, task.CreatedAt, 501),
			Method:    http.MethodPost,
			Route:     "/environments/{environment}/tasks",
			Key:       task.IdempotencyKey,
		},
		Intent: ProtectedIntentRecord{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
		},
		Response: IdempotencyResponse{
			Status: http.StatusAccepted, ContentKind: "application/json", Body: body,
		},
		TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
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
