package etcd

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SVC-05: the first Service Deploy acknowledges an empty configuration without
// becoming an ordinary file writer. Its Release already owns the Environment fence.
func TestFirstReleaseConfigurationAcknowledgementDoesNotInventFileWriter(t *testing.T) {
	_, store, _, _ := backupPolicyRepositoryTestHierarchy(t)
	task := configurationTaskFixture()
	task.Type, task.Target = testtaskjournal.TaskDeploy, ids.New(ids.KindService)
	task.Materializations = nil
	delete(task.Params, testtaskjournal.TaskMaterializationEnvironmentParam)
	prepared, err := prepareRuntimeConfigurationTask(context.Background(), store, task, store.revision)
	if err != nil {
		t.Fatal(err)
	}
	prepared.Status = testtaskjournal.TaskStatusCompleted
	change, err := prepareRuntimeConfigurationAcknowledgement(prepared)
	if err != nil || !change.applies {
		t.Fatalf("first empty configuration acknowledgement = %#v, %v", change, err)
	}
	defer clearTaskMaterializationProjectionChange(change)
	if target, applies, err := ordinaryTaskEnvironmentMutationTarget(prepared, change.applies, false, false, false, false); err != nil ||
		applies ||
		target != "" {
		t.Fatalf("Release invented ordinary file-writer ownership: %q, %v, %v", target, applies, err)
	}
	prepared.Params[testtaskjournal.TaskMaterializationEnvironmentParam] = ""
	if _, _, err := ordinaryTaskEnvironmentMutationTarget(prepared, true, false, false, false, false); err == nil {
		t.Fatal("explicit empty file-writer identity was accepted")
	}
}

// ENT-08/SVC-15: metadata deletion and file acknowledgement are distinct
// effects in the same Environment, not conflicting owners. A foreign target
// must still fail before either effect advances the execution epoch.
func TestTaskEnvironmentTargetAcceptsSameOwnerEffectsOnly(t *testing.T) {
	t.Parallel()
	task := configurationTaskFixture()
	for _, foreign := range []bool{false, true} {
		task.Params[testtaskjournal.TaskMaterializationEnvironmentParam] = task.Owner.EnvironmentID
		task.Params[testtaskjournal.TaskEntryEnvironmentParam] = task.Owner.EnvironmentID
		if foreign {
			task.Params[testtaskjournal.TaskEntryEnvironmentParam] = ids.New(ids.KindEnvironment)
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
			task.Type, task.Target = testtaskjournal.TaskCreate, ids.New(ids.KindRoute)
			task.Params[testtaskjournal.TaskResourceKindParam] = testtaskjournal.TaskResourceRoute
			task.Params[testtaskjournal.TaskMaterializationEnvironmentParam] = task.Owner.EnvironmentID
			task.Params[testtaskjournal.TaskRouteEnvironmentParam] = task.Owner.EnvironmentID
			switch scenario {
			case "edit":
				task.Type = testtaskjournal.TaskUpdate
			case "foreign owner":
				task.Owner.EnvironmentID = ids.New(ids.KindEnvironment)
			case "foreign parameter":
				task.Params[testtaskjournal.TaskRouteEnvironmentParam] = ids.New(ids.KindEnvironment)
			case "wrong target":
				task.Target = ids.New(ids.KindService)
			case "wrong kind":
				task.Params[testtaskjournal.TaskResourceKindParam] = testtaskjournal.TaskResourceVolume
			case "unsupported action":
				task.Type = testtaskjournal.TaskDeploy
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
