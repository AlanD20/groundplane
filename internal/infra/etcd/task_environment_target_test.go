package etcd

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ENT-08/SVC-15: metadata deletion and file acknowledgement are distinct
// effects in the same Environment, not conflicting owners. A foreign target
// must still fail before either effect advances the execution epoch.
func TestTaskEnvironmentTargetAcceptsSameOwnerEffectsOnly(t *testing.T) {
	t.Parallel()
	task := configurationTaskFixture()
	for _, foreign := range []bool{false, true} {
		task.Params[TaskMaterializationEnvironmentParam] = task.Owner.EnvironmentID
		task.Params[TaskEntryEnvironmentParam] = task.Owner.EnvironmentID
		if foreign {
			task.Params[TaskEntryEnvironmentParam] = ids.New(ids.KindEnvironment)
		}
		target, present, err := ordinaryTaskEnvironmentMutationTarget(task, true, false, true, false, false)
		if foreign {
			if !errors.Is(err, errs.New(errs.KindInternal, "")) || present || target != "" {
				t.Fatalf("foreign owner accepted: %q/%v/%v", target, present, err)
			}
		} else if err != nil || !present || target != task.Owner.EnvironmentID {
			t.Fatalf("same owner effects rejected: %q/%v/%v", target, present, err)
		}
	}
}

// ROUTE-03/SVC-15: explicit Route file writers require their exact Environment
// owner and supported mutation kind; a Route label alone grants no file access.
func TestRouteFileWriterRequiresExactMutationOwner(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"create", "edit", "foreign owner", "foreign parameter", "wrong target", "wrong kind", "unsupported action"} {
		t.Run(scenario, func(t *testing.T) {
			task := configurationTaskFixture()
			task.Type, task.Target = TaskCreate, ids.New(ids.KindRoute)
			task.Params[TaskResourceKindParam] = TaskResourceRoute
			task.Params[TaskMaterializationEnvironmentParam] = task.Owner.EnvironmentID
			task.Params[TaskRouteEnvironmentParam] = task.Owner.EnvironmentID
			switch scenario {
			case "edit":
				task.Type = TaskUpdate
			case "foreign owner":
				task.Owner.EnvironmentID = ids.New(ids.KindEnvironment)
			case "foreign parameter":
				task.Params[TaskRouteEnvironmentParam] = ids.New(ids.KindEnvironment)
			case "wrong target":
				task.Target = ids.New(ids.KindService)
			case "wrong kind":
				task.Params[TaskResourceKindParam] = TaskResourceVolume
			case "unsupported action":
				task.Type = TaskDeploy
			}
			_, present, err := taskMaterializationEnvironment(task)
			if scenario == "create" || scenario == "edit" {
				if err != nil || !present {
					t.Fatalf("Route mutation rejected: %v", err)
				}
			} else if err == nil || present {
				t.Fatal("invalid Route authority accepted")
			}
		})
	}
}
