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
	if completed.RetainUntil == nil || completed.TerminalAt == nil ||
		!completed.RetainUntil.Equal(completed.TerminalAt.Add(TaskRetention)) {
		t.Fatalf("terminal retention metadata = %#v", completed)
	}
}

func TestTaskRetryClonesOperationMetadataWithoutAliasing(t *testing.T) {
	// Rationale: retries survive restart as new attempts of the same operation;
	// mutable maps and steps must not alias the terminal source record.
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
	retry, err := cloneRetryTask(failed, retryID, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("cloneRetryTask() error = %v", err)
	}
	if retry.ID == failed.ID || retry.RetryOf != failed.ID || retry.OperationID != failed.OperationID ||
		retry.IdempotencyKey != failed.IdempotencyKey || retry.PlanHash != failed.PlanHash ||
		retry.Type != failed.Type || retry.Target != failed.Target || retry.TimeoutSeconds != failed.TimeoutSeconds ||
		!reflect.DeepEqual(retry.Params, failed.Params) || !reflect.DeepEqual(retry.Steps, failed.Steps) {
		t.Fatalf("retry did not preserve operation metadata: source=%#v retry=%#v", failed, retry)
	}
	if retry.Status != TaskStatusPending || retry.EventCount != 0 || retry.NextEventSequence != 1 ||
		retry.StartedAt != nil || retry.TerminalAt != nil || retry.RetainUntil != nil {
		t.Fatalf("retry lifecycle was not reset: %#v", retry)
	}
	retry.Params["name"] = "changed"
	retry.Steps[0].Params["script"] = "changed"
	if failed.Params["name"] == "changed" || failed.Steps[0].Params["script"] == "changed" {
		t.Fatal("retry metadata aliases its source")
	}
}

func TestTaskAndEventCodecsRejectCorruptDurableRecords(t *testing.T) {
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
		TaskScript,
		ids.NewAt(ids.KindService, now, 4),
		120,
		now,
	)
	task.IdempotencyKey = "0123456789abcdef"
	task.PlanHash = strings.Repeat("a", 64)
	task.Params = map[string]string{"name": "migrate"}
	task.Steps = []TaskStepRecord{{
		ID: taskJournalStepID(), Op: "run_script", Params: map[string]string{"script": "migrate"},
	}}
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
