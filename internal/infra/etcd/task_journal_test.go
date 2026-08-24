package etcd

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestTaskRecordCodecPreservesRestartSafeJournalState(t *testing.T) {
	// Rationale: a Controller restart must resume at the persisted next event
	// sequence rather than reuse a sequence or lose task lifecycle metadata.
	now := taskJournalTime()
	task := validTaskRecord(now)
	first, err := prepareTaskEvent(task, taskEventInput(task.ID, 1, TaskEventStateRunning), nil, now.Add(time.Second))
	if err != nil {
		t.Fatalf("prepareTaskEvent(first) error = %v", err)
	}
	encoded, err := encodeTaskRecord(first.Task)
	if err != nil {
		t.Fatalf("encodeTaskRecord() error = %v", err)
	}
	restored, err := decodeTaskRecord(encoded)
	if err != nil {
		t.Fatalf("decodeTaskRecord() error = %v", err)
	}
	second, err := prepareTaskEvent(
		restored,
		taskEventInput(task.ID, 2, TaskEventStateRunning),
		nil,
		now.Add(2*time.Second),
	)
	if err != nil {
		t.Fatalf("prepareTaskEvent(second) error = %v", err)
	}
	if first.Sequence != 1 || second.Sequence != 2 || second.Task.NextEventSequence != 3 ||
		second.Task.EventCount != 2 {
		t.Fatalf("journal summaries = first %#v, second %#v", first, second)
	}
	if first.Task.Status != TaskStatusPending || second.Task.Status != TaskStatusPending {
		t.Fatalf("step events changed task lifecycle: first=%s second=%s", first.Task.Status, second.Task.Status)
	}
	reencoded, err := encodeTaskRecord(restored)
	if err != nil {
		t.Fatalf("re-encode restored task: %v", err)
	}
	if string(reencoded) != string(encoded) {
		t.Fatalf("task codec is not canonical:\nfirst  %s\nsecond %s", encoded, reencoded)
	}
}

func TestTaskEventDeduplicationReturnsSequenceAndRejectsPayloadMismatch(t *testing.T) {
	// Rationale: an Agent may resend after losing the stream response. The same
	// identity must be replay-safe, while identity reuse cannot hide new output.
	now := taskJournalTime()
	task := validTaskRecord(now)
	input := taskEventInput(task.ID, 1, TaskEventStateRunning)
	prepared, err := prepareTaskEvent(task, input, nil, now.Add(time.Second))
	if err != nil {
		t.Fatalf("prepareTaskEvent() error = %v", err)
	}
	duplicate, err := prepareTaskEvent(prepared.Task, input, &prepared.Dedup, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("prepareTaskEvent(duplicate) error = %v", err)
	}
	if !duplicate.Duplicate || duplicate.Sequence != prepared.Sequence ||
		duplicate.Task.NextEventSequence != prepared.Task.NextEventSequence {
		t.Fatalf("duplicate result = %#v", duplicate)
	}

	mismatch := input
	mismatch.Payload = json.RawMessage(`{"message":"different"}`)
	_, err = prepareTaskEvent(prepared.Task, mismatch, &prepared.Dedup, now.Add(2*time.Second))
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("prepareTaskEvent(mismatch) error = %v, want internal", err)
	}
}

func TestTaskEventAcceptsTerminalReplayButRejectsNewTerminalIdentity(t *testing.T) {
	// Rationale: an acknowledgement may race an Agent reconnect. The exact
	// committed identity remains replayable, but a terminal Task cannot grow a
	// new activity history after its lifecycle is closed.
	now := taskJournalTime()
	task := validTaskRecord(now)
	input := taskEventInput(task.ID, 1, TaskEventStateRunning)
	prepared, err := prepareTaskEvent(task, input, nil, now.Add(time.Second))
	if err != nil {
		t.Fatalf("prepareTaskEvent(first) error = %v", err)
	}
	running, err := transitionTaskStatus(
		prepared.Task,
		TaskStatusPending,
		TaskStatusRunning,
		now.Add(2*time.Second),
	)
	if err != nil {
		t.Fatalf("transitionTaskStatus(running) error = %v", err)
	}
	terminal, err := transitionTaskStatus(
		running,
		TaskStatusRunning,
		TaskStatusCompleted,
		now.Add(3*time.Second),
	)
	if err != nil {
		t.Fatalf("transitionTaskStatus(terminal) error = %v", err)
	}
	replay, err := prepareTaskEvent(terminal, input, &prepared.Dedup, now.Add(4*time.Second))
	if err != nil {
		t.Fatalf("prepareTaskEvent(replay) error = %v", err)
	}
	if !replay.Duplicate || replay.Sequence != prepared.Sequence {
		t.Fatalf("prepareTaskEvent(replay) = %#v", replay)
	}
	newIdentity := taskEventInput(task.ID, 2, TaskEventStateRunning)
	if _, err := prepareTaskEvent(
		terminal,
		newIdentity,
		nil,
		now.Add(4*time.Second),
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("prepareTaskEvent(new terminal identity) error = %v, want internal", err)
	}
}

func TestTaskEventRequiresADeclaredTaskStep(t *testing.T) {
	// Rationale: accepting an Agent-supplied step id that is absent from the
	// immutable procedure would create activity that cannot be correlated with
	// the dispatched plan after a reconnect.
	now := taskJournalTime()
	task := validTaskRecord(now)
	input := taskEventInput(task.ID, 1, TaskEventStateRunning)
	input.Identity.StepID = ids.NewAt(ids.KindStep, now, 6)
	_, err := prepareTaskEvent(task, input, nil, now.Add(time.Second))
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("prepareTaskEvent(unknown step) error = %v, want validation.failed", err)
	}
}

func TestTaskStepEventDoesNotCompleteTheTaskLifecycle(t *testing.T) {
	// Rationale: TaskEvent state belongs to one step. Only the exact terminal
	// Agent acknowledgement may complete the whole multi-step Task.
	now := taskJournalTime()
	task := validTaskRecord(now)
	running, err := transitionTaskStatus(task, TaskStatusPending, TaskStatusRunning, now.Add(time.Second))
	if err != nil {
		t.Fatalf("transitionTaskStatus(running) error = %v", err)
	}
	prepared, err := prepareTaskEvent(
		running,
		taskEventInput(task.ID, 1, TaskEventStateCompleted),
		nil,
		now.Add(2*time.Second),
	)
	if err != nil {
		t.Fatalf("prepareTaskEvent(completed step) error = %v", err)
	}
	if prepared.Event.State != TaskEventStateCompleted || prepared.Task.Status != TaskStatusRunning {
		t.Fatalf("event/task states = %s/%s, want completed/running", prepared.Event.State, prepared.Task.Status)
	}
}

func TestTaskEventOrderingAndLimitsAreDeterministic(t *testing.T) {
	// Rationale: event keys sort lexically by their Controller-assigned sequence;
	// the summary must stop before uint64 or the accepted journal cap can drift.
	now := taskJournalTime()
	task := validTaskRecord(now)
	for ordinal := uint64(1); ordinal <= 3; ordinal++ {
		prepared, err := prepareTaskEvent(
			task,
			taskEventInput(task.ID, ordinal, TaskEventStateRunning),
			nil,
			now.Add(time.Duration(ordinal)*time.Second),
		)
		if err != nil {
			t.Fatalf("prepareTaskEvent(%d) error = %v", ordinal, err)
		}
		if prepared.Sequence != ordinal {
			t.Fatalf("sequence = %d, want %d", prepared.Sequence, ordinal)
		}
		task = prepared.Task
	}
	task.EventCount = MaximumTaskEvents
	task.NextEventSequence = MaximumTaskEvents + 1
	_, err := prepareTaskEvent(
		task,
		taskEventInput(task.ID, MaximumTaskEvents+1, TaskEventStateRunning),
		nil,
		now.Add(time.Hour),
	)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("prepareTaskEvent(limit) error = %v, want validation.failed", err)
	}
}

func TestTaskEventDurableJSONLimitIncludesEnvelope(t *testing.T) {
	// Rationale: enforcing only the Agent chunk size would let envelope metadata
	// exceed etcd's accepted 32 KiB durable event ceiling.
	now := taskJournalTime()
	task := validTaskRecord(now)
	input := taskEventInput(task.ID, 1, TaskEventStateRunning)
	input.Payload = json.RawMessage(`{"message":"` + strings.Repeat("x", MaximumTaskEventBytes) + `"}`)
	_, err := prepareTaskEvent(task, input, nil, now.Add(time.Second))
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("prepareTaskEvent(oversized) error = %v, want validation.failed", err)
	}
}

func TestTaskStatusTransitionRejectsStaleAndInvalidState(t *testing.T) {
	// Rationale: repository CAS will compare the record revision; this pure state
	// guard independently prevents stale workers and terminal-state regression.
	now := taskJournalTime()
	task := validTaskRecord(now)
	running, err := transitionTaskStatus(task, TaskStatusPending, TaskStatusRunning, now.Add(time.Second))
	if err != nil {
		t.Fatalf("transitionTaskStatus(running) error = %v", err)
	}
	_, err = transitionTaskStatus(running, TaskStatusPending, TaskStatusRunning, now.Add(2*time.Second))
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("transitionTaskStatus(stale) error = %v, want state.conflict", err)
	}
	completed, err := transitionTaskStatus(running, TaskStatusRunning, TaskStatusCompleted, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("transitionTaskStatus(completed) error = %v", err)
	}
	_, err = transitionTaskStatus(completed, TaskStatusCompleted, TaskStatusRunning, now.Add(3*time.Second))
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("transitionTaskStatus(regression) error = %v, want state.conflict", err)
	}
	if completed.RetainUntil == nil || completed.FinishedAt == nil ||
		!completed.RetainUntil.Equal(completed.FinishedAt.Add(TaskRetention)) ||
		!completed.UpdatedAt.Equal(*completed.FinishedAt) {
		t.Fatalf("terminal retention metadata = %#v", completed)
	}
}

func TestTaskRetryClonesOperationMetadataWithoutAliasing(t *testing.T) {
	// Rationale: retries survive restart as new attempts of the same operation;
	// mutable maps and step slices must not alias the terminal source record.
	now := taskJournalTime()
	source := validTaskRecord(now)
	running, err := transitionTaskStatus(source, TaskStatusPending, TaskStatusRunning, now.Add(time.Second))
	if err != nil {
		t.Fatalf("transitionTaskStatus(running) error = %v", err)
	}
	failed, err := transitionTaskStatus(running, TaskStatusRunning, TaskStatusFailed, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("transitionTaskStatus(failed) error = %v", err)
	}
	retryID := ids.NewAt(ids.KindTask, now.Add(3*time.Second), 3)
	retry, err := cloneRetryTask(failed, retryID, TaskActorOperator, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("cloneRetryTask() error = %v", err)
	}
	if retry.ID == failed.ID || retry.RetryOf != failed.ID || retry.OperationID != failed.OperationID ||
		retry.IdempotencyKey != failed.IdempotencyKey || retry.PlanID != failed.PlanID ||
		retry.PlanHash != failed.PlanHash || retry.RenderGeneration != failed.RenderGeneration ||
		retry.Type != failed.Type || retry.Target != failed.Target || retry.TimeoutSeconds != failed.TimeoutSeconds ||
		retry.Owner != failed.Owner || retry.Actor != TaskActorOperator ||
		!reflect.DeepEqual(retry.Params, failed.Params) || !reflect.DeepEqual(retry.Steps, failed.Steps) {
		t.Fatalf("retry did not preserve operation metadata: source=%#v retry=%#v", failed, retry)
	}
	if retry.Status != TaskStatusPending || retry.EventCount != 0 || retry.NextEventSequence != 1 ||
		retry.StartedAt != nil || retry.FinishedAt != nil || retry.RetainUntil != nil ||
		!retry.UpdatedAt.Equal(retry.CreatedAt) {
		t.Fatalf("retry lifecycle was not reset: %#v", retry)
	}
	retry.Params["name"] = "changed"
	retry.Steps[0].ID = ids.NewAt(ids.KindStep, now, 99)
	if failed.Params["name"] == "changed" || failed.Steps[0].ID == retry.Steps[0].ID {
		t.Fatal("retry metadata aliases its source")
	}
}

func TestTaskAndEventCodecsRejectCorruptDurableRecords(t *testing.T) {
	// Rationale: clean-start decoding must reject every Task journal shape that
	// predates or omits the accepted durable schema.
	// Rationale: persisted records are trusted only after their versioned
	// envelope, stable ids, canonical timestamps, and event digest all agree.
	now := taskJournalTime()
	task := validTaskRecord(now)
	encodedTask, err := encodeTaskRecord(task)
	if err != nil {
		t.Fatalf("encodeTaskRecord() error = %v", err)
	}
	corruptTask := strings.Replace(string(encodedTask), `"next_event_sequence":1`, `"next_event_sequence":2`, 1)
	if _, err := decodeTaskRecord([]byte(corruptTask)); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("decodeTaskRecord(corrupt) error = %v, want internal", err)
	}
	ownerless := strings.Replace(string(encodedTask), `"owner":{"workspace_type":"platform"},`, "", 1)
	if _, err := decodeTaskRecord([]byte(ownerless)); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("decodeTaskRecord(ownerless clean-start record) error = %v, want internal", err)
	}
	actorless := strings.Replace(string(encodedTask), `"actor":"operator",`, "", 1)
	if _, err := decodeTaskRecord([]byte(actorless)); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("decodeTaskRecord(actorless clean-start record) error = %v, want internal", err)
	}
	updatedless := strings.Replace(
		string(encodedTask),
		`"updated_at":"`+task.UpdatedAt.Format(time.RFC3339Nano)+`"`,
		`"updated_at":""`,
		1,
	)
	if _, err := decodeTaskRecord([]byte(updatedless)); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("decodeTaskRecord(timestampless clean-start record) error = %v, want internal", err)
	}
	running, err := transitionTaskStatus(task, TaskStatusPending, TaskStatusRunning, now.Add(time.Second))
	if err != nil {
		t.Fatalf("transitionTaskStatus(running) error = %v", err)
	}
	terminal, err := transitionTaskStatus(running, TaskStatusRunning, TaskStatusFailed, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("transitionTaskStatus(terminal) error = %v", err)
	}
	encodedTerminal, err := encodeTaskRecord(terminal)
	if err != nil {
		t.Fatalf("encodeTaskRecord(terminal) error = %v", err)
	}
	oldTerminal := strings.Replace(string(encodedTerminal), `"finished_at":`, `"terminal_at":`, 1)
	if _, err := decodeTaskRecord([]byte(oldTerminal)); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("decodeTaskRecord(old terminal schema) error = %v, want internal", err)
	}

	prepared, err := prepareTaskEvent(
		task,
		taskEventInput(task.ID, 1, TaskEventStateRunning),
		nil,
		now.Add(time.Second),
	)
	if err != nil {
		t.Fatalf("prepareTaskEvent() error = %v", err)
	}
	encodedEvent, err := encodeTaskEventRecord(prepared.Event)
	if err != nil {
		t.Fatalf("encodeTaskEventRecord() error = %v", err)
	}
	corruptEvent := strings.Replace(string(encodedEvent), prepared.Event.PayloadSHA256, strings.Repeat("0", 64), 1)
	if _, err := decodeTaskEventRecord([]byte(corruptEvent)); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("decodeTaskEventRecord(corrupt) error = %v, want internal", err)
	}
}

func TestTaskControllerTimestampsNormalizeEqualityAndRegression(t *testing.T) {
	// Rationale: wall-clock equality and regression must not make otherwise
	// valid Controller transitions fail or move updated_at backwards.
	now := taskJournalTime()
	task := validTaskRecord(now)
	running, err := transitionTaskStatus(task, TaskStatusPending, TaskStatusRunning, now)
	if err != nil {
		t.Fatalf("transitionTaskStatus(equal) error = %v", err)
	}
	wantRunning := now.Add(time.Nanosecond)
	if !running.UpdatedAt.Equal(wantRunning) || running.StartedAt == nil ||
		!running.StartedAt.Equal(wantRunning) {
		t.Fatalf("equal transition timestamps = %#v", running)
	}
	terminal, err := transitionTaskStatus(running, TaskStatusRunning, TaskStatusFailed, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("transitionTaskStatus(regressed) error = %v", err)
	}
	wantTerminal := wantRunning.Add(time.Nanosecond)
	if !terminal.UpdatedAt.Equal(wantTerminal) || terminal.FinishedAt == nil ||
		!terminal.FinishedAt.Equal(wantTerminal) {
		t.Fatalf("regressed transition timestamps = %#v", terminal)
	}

	eventTask := validTaskRecord(now)
	prepared, err := prepareTaskEvent(
		eventTask,
		taskEventInput(eventTask.ID, 1, TaskEventStateRunning),
		nil,
		now.Add(-time.Hour),
	)
	if err != nil {
		t.Fatalf("prepareTaskEvent(regressed) error = %v", err)
	}
	if !prepared.Task.UpdatedAt.Equal(now.Add(time.Nanosecond)) {
		t.Fatalf("event updated_at = %s", prepared.Task.UpdatedAt)
	}
}

func TestTaskJournalRequiresCanonicalUTCTimestamps(t *testing.T) {
	// Rationale: a zero-offset named location is an equivalent instant but not
	// the one canonical representation used for hashing and durable records.
	now := taskJournalTime()
	task := validTaskRecord(now)
	task.CreatedAt = time.Date(2026, time.August, 20, 12, 0, 0, 0, time.FixedZone("zero-offset", 0))
	if _, err := encodeTaskRecord(task); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("encodeTaskRecord(noncanonical UTC) error = %v, want validation.failed", err)
	}

	task = validTaskRecord(now)
	input := taskEventInput(task.ID, 1, TaskEventStateRunning)
	noncanonical := time.Date(2026, time.August, 20, 12, 0, 1, 0, time.FixedZone("zero-offset", 0))
	if _, err := prepareTaskEvent(
		task,
		input,
		nil,
		noncanonical,
	); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("prepareTaskEvent(noncanonical UTC) error = %v, want validation.failed", err)
	}
}

func validTaskRecord(now time.Time) TaskRecord {
	task := newTaskRecord(
		ids.NewAt(ids.KindTask, now, 1),
		ids.NewAt(ids.KindOperation, now, 2),
		PlatformTaskOwner(),
		TaskActorOperator,
		TaskScript,
		ids.NewAt(ids.KindService, now, 4),
		120,
		now,
	)
	task.IdempotencyKey = "0123456789abcdef"
	task.PlanID = ids.NewAt(ids.KindPlan, now, 3)
	task.PlanHash = strings.Repeat("a", 64)
	task.RenderGeneration = 1
	task.Params = map[string]string{"name": "migrate"}
	task.Steps = []TaskStepRecord{{ID: taskJournalStepID()}}
	return task
}

func taskEventInput(taskID string, ordinal uint64, state TaskEventState) TaskEventInput {
	return TaskEventInput{
		Identity: TaskEventIdentity{TaskID: taskID, StepID: taskJournalStepID(), Attempt: 1, Ordinal: ordinal},
		State:    state,
		Payload:  json.RawMessage(`{"message":"progress"}`),
	}
}

func taskJournalStepID() string {
	return ids.NewAt(ids.KindStep, taskJournalTime(), 5)
}

func taskJournalTime() time.Time {
	return time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)
}
