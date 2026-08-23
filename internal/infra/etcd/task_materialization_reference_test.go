package etcd

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

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
	retry.Materializations[3].Source.GeneratedEnvironment.Values[0].Name = "MUTATED"
	if failed.Materializations[3].Source.GeneratedEnvironment.Values[0].Name != "APP_ENV" {
		t.Fatal("retry materialization references alias the source task")
	}
	retry.Materializations[1].Source.ComponentFile.Path = "components/mutated/config"
	if failed.Materializations[1].Source.ComponentFile.Path != "components/caddy/Caddyfile" {
		t.Fatal("retry Component file reference aliases the source task")
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
			values := task.Materializations[3].Source.GeneratedEnvironment.Values
			values[0], values[1] = values[1], values[0]
		},
		"untyped Entry storage": func(task *TaskRecord) {
			task.Materializations[2].Source.EntryValue.Storage = ""
		},
		"invalid Component identity": func(task *TaskRecord) {
			task.Materializations[1].Source.ComponentFile.ComponentID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		},
		"duplicate materialization id": func(task *TaskRecord) {
			task.Materializations[1].MaterializationID = task.Materializations[0].MaterializationID
		},
		"invalid digest": func(task *TaskRecord) {
			task.Materializations[0].SHA256 = "not-a-digest"
		},
		"unsafe output policy": func(task *TaskRecord) {
			task.Materializations[0].Mode = 0o600
		},
		"source output mismatch": func(task *TaskRecord) {
			task.Materializations[0].OutputKind = TaskMaterializationOutputSecretFile
			task.Materializations[0].Mode = 0o600
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
		ids.NewAt(ids.KindStep, now, 8),
	}
	sort.Strings(stepIDs)
	task.Type = TaskUpdate
	task.Target = environmentID
	task.Params = map[string]string{TaskMaterializationEnvironmentParam: environmentID}
	task.Steps = []TaskStepRecord{{ID: stepIDs[0]}, {ID: stepIDs[1]}, {ID: stepIDs[2]}, {ID: stepIDs[3]}}
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
			StepID: stepIDs[0], MaterializationID: ids.NewAt(ids.KindConfig, now, 40),
			EnvironmentID: environmentID, Destination: "config/app.yaml",
			OutputKind: TaskMaterializationOutputPlainFile, Mode: 0o444, Length: 12,
			SHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Source: TaskMaterializationSource{
				Kind: TaskMaterializationSourceBlueprintFile,
				BlueprintFile: &TaskBlueprintFileValueReference{
					RevisionID: ids.NewAt(ids.KindTask, now, 25), Path: "config/app.yaml",
				},
			},
		},
		{
			StepID: stepIDs[1], MaterializationID: ids.NewAt(ids.KindConfig, now, 41),
			EnvironmentID: environmentID, Destination: "components/caddy/Caddyfile",
			OutputKind: TaskMaterializationOutputPlainFile, Mode: 0o444, Length: 13,
			SHA256: "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Source: TaskMaterializationSource{
				Kind: TaskMaterializationSourceComponentFile,
				ComponentFile: &TaskComponentFileValueReference{
					RevisionID:  ids.NewAt(ids.KindTask, now, 26),
					ComponentID: ids.NewAt(ids.KindComponent, now, 27),
					Path:        "components/caddy/Caddyfile",
				},
			},
		},
		{
			StepID: stepIDs[2], MaterializationID: ids.NewAt(ids.KindConfig, now, 42),
			EnvironmentID: environmentID, Destination: "config/plain.txt",
			OutputKind: TaskMaterializationOutputPlainFile, UID: 1000, GID: 1000, Mode: 0o444, Length: 14,
			SHA256: "2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Source: TaskMaterializationSource{Kind: TaskMaterializationSourceEntryValue, EntryValue: &plain},
		},
		{
			StepID: stepIDs[3], MaterializationID: ids.NewAt(ids.KindConfig, now, 43),
			EnvironmentID: environmentID, Destination: "secrets/.env." + environmentID + ".app",
			ServiceID: ids.NewAt(ids.KindService, now, 44), ServiceName: "app",
			OutputKind: TaskMaterializationOutputGeneratedEnvironment,
			Mode:       0o600, Length: 15,
			SHA256: "3123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
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

// Rationale: durable removal intent must authorize only an empty typed source
// bound to one of the three corresponding removal output policies.
func TestTaskMaterializationRemovalReferenceIsClosed(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 13, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	stepID := ids.NewAt(ids.KindStep, now, 2)
	emptyDigest := sha256.Sum256(nil)
	reference := TaskMaterializationRecord{
		StepID:            stepID,
		MaterializationID: ids.NewAt(ids.KindConfig, now, 3),
		EnvironmentID:     environmentID,
		Destination:       "config/app.yaml",
		OutputKind:        TaskMaterializationOutputRemovePlainFile,
		UID:               1000,
		GID:               1000,
		Mode:              0o444,
		SHA256: hex.EncodeToString(
			emptyDigest[:],
		),
		Source: TaskMaterializationSource{Kind: TaskMaterializationSourceRemoval},
	}
	if err := validateTaskMaterializationReferences(
		[]TaskMaterializationRecord{reference}, []TaskStepRecord{{ID: stepID}}, environmentID, true, 7,
	); err != nil {
		t.Fatalf("validateTaskMaterializationReferences(removal) error = %v", err)
	}
	reference.Source.EntryValue = &TaskEntryValueReference{}
	if err := validateTaskMaterializationReferences(
		[]TaskMaterializationRecord{reference}, []TaskStepRecord{{ID: stepID}}, environmentID, true, 7,
	); err == nil {
		t.Fatal("validateTaskMaterializationReferences accepted removal with a value source")
	}
}
