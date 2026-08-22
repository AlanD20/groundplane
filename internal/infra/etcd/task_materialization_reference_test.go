package etcd

import (
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: restart and retry must preserve an exact, non-aliased source
// union for every materialization without persisting any value bytes.
func TestTaskMaterializationReferencesRoundTripAndClone(t *testing.T) {
	task := taskWithMaterializationReferences()
	encoded, err := encodeTaskRecord(task)
	if err != nil {
		t.Fatalf("encodeTaskRecord() error = %v", err)
	}
	restored, err := decodeTaskRecord(encoded)
	if err != nil {
		t.Fatalf("decodeTaskRecord() error = %v", err)
	}
	if !reflect.DeepEqual(restored.Materializations, task.Materializations) {
		t.Fatalf("restored materializations = %#v", restored.Materializations)
	}

	failed, err := transitionTaskStatus(task, TaskStatusPending, TaskStatusRunning, task.CreatedAt.Add(1))
	if err != nil {
		t.Fatalf("transitionTaskStatus(running) error = %v", err)
	}
	failed, err = transitionTaskStatus(failed, TaskStatusRunning, TaskStatusFailed, task.CreatedAt.Add(2))
	if err != nil {
		t.Fatalf("transitionTaskStatus(failed) error = %v", err)
	}
	retry, err := cloneRetryTask(
		failed,
		ids.NewAt(ids.KindTask, task.CreatedAt, 31),
		task.CreatedAt.Add(3),
	)
	if err != nil {
		t.Fatalf("cloneRetryTask() error = %v", err)
	}
	retry.Materializations[2].Source.GeneratedEnvironment.Values[0].Name = "MUTATED"
	if failed.Materializations[2].Source.GeneratedEnvironment.Values[0].Name != "APP_ENV" {
		t.Fatal("retry materialization references alias the source task")
	}
}

// Rationale: durable source decoding must fail closed instead of selecting a
// fallback repository or allowing one source to authorize another step.
func TestTaskMaterializationReferencesRejectConfusedShapes(t *testing.T) {
	tests := map[string]func(*TaskRecord){
		"unknown step": func(task *TaskRecord) {
			task.Materializations[0].StepID = ids.NewAt(ids.KindStep, task.CreatedAt, 30)
		},
		"different Environment": func(task *TaskRecord) {
			task.Materializations[0].EnvironmentID = ids.NewAt(ids.KindEnvironment, task.CreatedAt, 30)
		},
		"multiple union members": func(task *TaskRecord) {
			task.Materializations[0].Source.EntryValue = &TaskEntryValueReference{
				EntryID:           ids.NewAt(ids.KindEnvEntry, task.CreatedAt, 30),
				ValueGenerationID: ids.NewAt(ids.KindConfig, task.CreatedAt, 31),
				Storage:           TaskEntryValueStoragePlain,
			}
		},
		"unsorted generated values": func(task *TaskRecord) {
			values := task.Materializations[2].Source.GeneratedEnvironment.Values
			values[0], values[1] = values[1], values[0]
		},
		"untyped Entry storage": func(task *TaskRecord) {
			task.Materializations[1].Source.EntryValue.Storage = ""
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			task := taskWithMaterializationReferences()
			mutate(&task)
			if _, err := encodeTaskRecord(task); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("encodeTaskRecord() error = %v, want validation.failed", err)
			}
		})
	}
}

func taskWithMaterializationReferences() TaskRecord {
	now := taskJournalTime()
	task := validTaskRecord(now)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 20)
	stepIDs := []string{
		taskJournalStepID(),
		ids.NewAt(ids.KindStep, now, 6),
		ids.NewAt(ids.KindStep, now, 7),
	}
	sort.Strings(stepIDs)
	task.Type = TaskUpdate
	task.Target = environmentID
	task.Params = map[string]string{TaskMaterializationEnvironmentParam: environmentID}
	task.Steps = []TaskStepRecord{{ID: stepIDs[0]}, {ID: stepIDs[1]}, {ID: stepIDs[2]}}
	plain := TaskEntryValueReference{
		EntryID:           ids.NewAt(ids.KindEnvEntry, now, 21),
		ValueGenerationID: ids.NewAt(ids.KindConfig, now, 22),
		Storage:           TaskEntryValueStoragePlain,
	}
	secret := TaskEntryValueReference{
		EntryID:           ids.NewAt(ids.KindEnvEntry, now, 23),
		ValueGenerationID: ids.NewAt(ids.KindConfig, now, 24),
		Storage:           TaskEntryValueStorageSecret,
	}
	task.Materializations = []TaskMaterializationRecord{
		{
			StepID: stepIDs[0], EnvironmentID: environmentID,
			Source: TaskMaterializationSource{
				Kind: TaskMaterializationSourceBlueprintFile,
				BlueprintFile: &TaskBlueprintFileValueReference{
					RevisionID: ids.NewAt(ids.KindTask, now, 25), Path: "config/app.yaml",
				},
			},
		},
		{
			StepID: stepIDs[1], EnvironmentID: environmentID,
			Source: TaskMaterializationSource{Kind: TaskMaterializationSourceEntryValue, EntryValue: &plain},
		},
		{
			StepID: stepIDs[2], EnvironmentID: environmentID,
			Source: TaskMaterializationSource{
				Kind: TaskMaterializationSourceGeneratedEnvironment,
				GeneratedEnvironment: &TaskGeneratedEnvironmentValueReference{
					FormatVersion: generatedEnvironmentFormatVersion,
					Values: []TaskGeneratedEnvironmentEntryReference{
						{Name: "APP_ENV", Value: plain},
						{Name: "TOKEN", Value: secret},
					},
				},
			},
		},
	}
	return task
}
