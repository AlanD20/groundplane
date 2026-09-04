package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: materializing Route and Entry removals target their stable child
// ids while still serializing writes against the owning Environment.
func TestTaskMaterializationEnvironmentAcceptsClosedRemovalTargets(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	for name, target := range map[string]string{
		"route": ids.NewAt(ids.KindRoute, now, 2),
		"entry": ids.NewAt(ids.KindEnvEntry, now, 3),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			task := TaskRecord{
				Executor: TaskExecutorAgent, Type: TaskRemove, Target: target,
				Params: map[string]string{TaskMaterializationEnvironmentParam: environmentID},
			}
			got, materializes, err := taskMaterializationEnvironment(task)
			if err != nil || !materializes || got != environmentID {
				t.Fatalf("taskMaterializationEnvironment() = %q/%t/%v", got, materializes, err)
			}
		})
	}
	for name, taskType := range map[string]TaskType{
		"volume add": TaskCreate, "volume edit": TaskUpdate, "volume remove": TaskRemove,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			task := TaskRecord{
				Executor: TaskExecutorAgent, Type: taskType, Target: ids.NewAt(ids.KindVolume, now, 4),
				Params: map[string]string{
					TaskMaterializationEnvironmentParam: environmentID,
					TaskResourceKindParam:               TaskResourceVolume,
				},
			}
			got, materializes, err := taskMaterializationEnvironment(task)
			if err != nil || !materializes || got != environmentID {
				t.Fatalf("taskMaterializationEnvironment() = %q/%t/%v", got, materializes, err)
			}
		})
	}
	serviceTask := TaskRecord{
		Executor: TaskExecutorAgent, Type: TaskRemove, Target: ids.NewAt(ids.KindService, now, 5),
		Params: map[string]string{TaskMaterializationEnvironmentParam: environmentID},
	}
	if _, _, err := taskMaterializationEnvironment(serviceTask); !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("taskMaterializationEnvironment(Service removal) error = %v", err)
	}
}

func TestAttachDetachWriterClaimHasNoBlueprintAppliedAuthority(t *testing.T) {
	// Rationale: Attach and Detach share only the generic Environment writer
	// fence and must not read or CAS the Blueprint applied projection.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 10)
	for index, taskType := range []TaskType{TaskAttach, TaskDetach} {
		task := TaskRecord{
			ID: ids.NewAt(ids.KindTask, now, int64(index+11)), Executor: TaskExecutorAgent,
			Type: taskType, RenderGeneration: 7,
			Params: map[string]string{TaskMutationEnvironmentParam: environmentID},
		}
		writer, conditions, err := repository.prepareTaskMaterializationWriter(ctx, task, environmentID, 0)
		if err != nil || len(conditions) != 0 || writer.BlueprintAppliedPredecessor != nil {
			t.Fatalf("prepare %s writer claim = %#v, %#v, %v", taskType, writer, conditions, err)
		}
		if validateTaskMaterializationWriterForTask(writer, task, environmentID) != nil {
			t.Fatalf("validate %s writer claim failed", taskType)
		}
		unexpected := taskMaterializationWriter(task, environmentID, &taskMaterializationAppliedPredecessor{})
		if validateTaskMaterializationWriterForTask(unexpected, task, environmentID) == nil {
			t.Fatalf("%s writer accepted unexpected Blueprint authority", taskType)
		}
	}
	blueprint := TaskRecord{
		ID: ids.NewAt(ids.KindTask, now, 13), Executor: TaskExecutorAgent,
		Type: TaskUpdate, RenderGeneration: 1,
		Owner:  TaskOwner{EnvironmentID: environmentID},
		Params: map[string]string{TaskReleasePublicationParam: ids.NewULID()},
	}
	if validateTaskMaterializationWriterForTask(
		taskMaterializationWriter(blueprint, environmentID, nil), blueprint, environmentID,
	) == nil {
		t.Fatal("Blueprint candidate writer accepted missing applied authority")
	}
}
