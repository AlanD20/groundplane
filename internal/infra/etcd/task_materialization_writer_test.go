package etcd

import (
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
