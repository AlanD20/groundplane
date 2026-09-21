package etcd

import (
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Multiple terminal effects may share one Environment owner. Every declared
// target must agree before its mutation epoch can advance.
func ordinaryTaskEnvironmentMutationTarget(
	task TaskRecord,
	materializationChange bool,
	attachChange bool,
	entryChange bool,
	serviceChange bool,
	connectorChange bool,
) (string, bool, error) {
	targets := make([]string, 0, 1)
	if materializationChange {
		targets = append(targets, task.Params[taskjournal.TaskMaterializationEnvironmentParam])
	}
	if attachChange {
		targets = append(targets, task.Params[taskjournal.TaskMutationEnvironmentParam])
	}
	if entryChange {
		targets = append(targets, task.Params[TaskEntryEnvironmentParam])
	}
	if serviceChange {
		targets = append(targets, task.Params[TaskServiceEnvironmentParam])
	}
	if task.Params[TaskZoneRemovalOperationParam] != "" {
		targets = append(targets, task.Params[TaskZoneEnvironmentParam])
	}
	if connectorChange {
		targets = append(targets, task.Params[TaskConnectorEnvironmentParam])
	}
	if len(targets) == 0 {
		return "", false, nil
	}
	if len(slices.Compact(targets)) != 1 {
		return "", false, errs.New(errs.KindInternal, "task has conflicting environment mutation targets")
	}
	environmentID := targets[0]
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || task.Owner.EnvironmentID != environmentID {
		return "", false, errs.New(errs.KindInternal, "task environment mutation ownership is corrupt")
	}
	return environmentID, true, nil
}
