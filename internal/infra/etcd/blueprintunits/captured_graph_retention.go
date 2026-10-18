package blueprintunits

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareGraphCollection joins final Task collection, after individual owners
// retire their inputs and work. It never removes desired or applied resource
// state, which must remain usable after the Task history expires.
func PrepareGraphCollection(task workqueue.TaskSnapshot) (workqueue.Change, error) {
	if task.Task.Action != workexecution.ActionApply || task.Task.Scope.EnvironmentID == "" ||
		task.Task.TargetID != task.Task.Scope.EnvironmentID {
		return workqueue.Change{}, invalidRecord()
	}
	change, err := workqueue.PrepareTaskCollection(task)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{
		Type: keyvalue.MutationDelete, Key: CapturedGraphKey(task.Task.Scope.EnvironmentID, task.Task.ID),
	})
	return change, nil
}
