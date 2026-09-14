package etcd

import (
	"bytes"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Rationale: the running set is execution authority, not desired state. Storage,
// status changes and retries must retain it, including an explicitly empty set.
func TestEntryTaskRuntimeSurvivesJournalAndRetry(t *testing.T) {
	for _, running := range [][]string{{}, {ids.New(ids.KindService)}} {
		task := entryRuntimeJournalTask(t, running)
		if len(running) != 0 {
			task.EntryRuntime.Updates = []EntryRuntimeUpdate{{ServiceID: running[0], PreviousRevision: 11,
				CurrentArtifactID: ids.New(ids.KindConfig), RetainedPriorArtifactID: ids.New(ids.KindConfig)}}
		}
		encoded, err := encodeTaskRecord(task)
		if err != nil {
			t.Fatal(err)
		}
		restored, err := decodeTaskRecord(encoded)
		if err != nil || restored.EntryRuntime == nil || restored.EntryRuntime.RunningServiceIDs == nil ||
			!slices.Equal(restored.EntryRuntime.RunningServiceIDs, running) ||
			!slices.Equal(restored.EntryRuntime.Updates, task.EntryRuntime.Updates) {
			t.Fatal("stored Entry running set changed", err)
		}
		reencoded, err := encodeTaskRecord(restored)
		if err != nil || !bytes.Equal(encoded, reencoded) {
			t.Fatal("Entry runtime codec is not canonical", err)
		}
		failed, err := transitionTaskStatus(restored, TaskStatusPending, TaskStatusRunning, task.CreatedAt.Add(1))
		if err != nil {
			t.Fatal(err)
		}
		failed, err = transitionTaskStatus(failed, TaskStatusRunning, TaskStatusFailed, task.CreatedAt.Add(2))
		if err != nil {
			t.Fatal(err)
		}
		retry, err := cloneRetryTask(failed, ids.New(ids.KindTask), TaskActorOperator, task.CreatedAt.Add(3))
		if err != nil || retry.EntryRuntime == nil || retry.EntryRuntime.RunningServiceIDs == nil ||
			!slices.Equal(retry.EntryRuntime.RunningServiceIDs, running) ||
			!slices.Equal(retry.EntryRuntime.Updates, task.EntryRuntime.Updates) {
			t.Fatal("retry lost Entry running set", err)
		}
		if epoch, err := EntryRuntimeEpochRevision(retry); err != nil || epoch != 7 {
			t.Fatal("retry changed capture epoch", err)
		}
		if len(running) > 0 {
			retry.EntryRuntime.RunningServiceIDs[0] = ids.New(ids.KindService)
			failed.EntryRuntime.RunningServiceIDs[0] = ids.New(ids.KindService)
			retry.EntryRuntime.Updates[0].CurrentArtifactID = ids.New(ids.KindConfig)
			failed.EntryRuntime.Updates[0].CurrentArtifactID = ids.New(ids.KindConfig)
			if restored.EntryRuntime.RunningServiceIDs[0] != running[0] ||
				retry.EntryRuntime.RunningServiceIDs[0] == failed.EntryRuntime.RunningServiceIDs[0] ||
				restored.EntryRuntime.Updates[0].CurrentArtifactID != task.EntryRuntime.Updates[0].CurrentArtifactID ||
				retry.EntryRuntime.Updates[0].CurrentArtifactID == failed.EntryRuntime.Updates[0].CurrentArtifactID {
				t.Fatal("Entry capture aliases a prior Task")
			}
		}
	}
}

// Rationale: absent historical capture is readable, but cannot authorize new
// execution. Malformed or foreign capture must fail at the durable boundary.
func TestEntryTaskRuntimeRejectsMissingAndMalformedAuthority(t *testing.T) {
	first, second := ids.New(ids.KindService), ids.New(ids.KindService)
	ordered := []string{first, second}
	slices.Sort(ordered)
	for _, test := range []struct {
		name   string
		change func(*TaskRecord)
	}{
		{"missing set", func(task *TaskRecord) { task.EntryRuntime.RunningServiceIDs = nil }},
		{"missing updates", func(task *TaskRecord) { task.EntryRuntime.Updates = nil }},
		{"foreign kind", func(task *TaskRecord) { task.EntryRuntime.RunningServiceIDs = []string{task.ID} }},
		{"duplicate", func(task *TaskRecord) { task.EntryRuntime.RunningServiceIDs = []string{first, first} }},
		{"unsorted", func(task *TaskRecord) { task.EntryRuntime.RunningServiceIDs = []string{ordered[1], ordered[0]} }},
		{"foreign task", func(task *TaskRecord) { task.Type = TaskCreate }},
		{"foreign executor", func(task *TaskRecord) { task.Executor = TaskExecutorController }},
	} {
		t.Run(test.name, func(t *testing.T) {
			task := entryRuntimeJournalTask(t, []string{})
			test.change(&task)
			if _, err := encodeTaskRecord(task); err == nil {
				t.Fatal("malformed capture was encoded")
			}
			encoded, err := encodeEnvelope("task", taskRecordToData(task))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeTaskRecord(encoded); err == nil {
				t.Fatal("malformed stored capture was accepted")
			}
		})
	}
	task := entryRuntimeJournalTask(t, []string{})
	task.EntryRuntime = nil
	encoded, err := encodeTaskRecord(task)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := decodeTaskRecord(encoded)
	if err != nil || restored.EntryRuntime != nil {
		t.Fatal("historical capture was invented", err)
	}
	if _, err := EntryRuntimeEpochRevision(restored); err == nil {
		t.Fatal("historical Task authorized execution without capture")
	}
}

func entryRuntimeJournalTask(t *testing.T, running []string) TaskRecord {
	t.Helper()
	task := validTaskRecord(taskJournalTime())
	task.Target = ids.New(ids.KindEnvironment)
	task.Params = map[string]string{TaskResourceKindParam: TaskResourceEntry, TaskEntryRuntimeEpochParam: "7"}
	task.EntryRuntime = &EntryTaskRuntime{
		RunningServiceIDs: slices.Clone(running), Updates: []EntryRuntimeUpdate{},
	}
	return task
}
