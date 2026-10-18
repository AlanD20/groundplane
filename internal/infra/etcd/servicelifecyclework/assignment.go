package servicelifecyclework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareAssignment compares the still-owned runtime intent before reserving a
// host slot. The same intent is compared again before each effect boundary.
func (repository *Repository) PrepareAssignment(ctx context.Context, view workqueue.View) (Input, workqueue.Change, error) {
	captured, guard, err := repository.Load(ctx, view)
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	if captured.Runtime == nil {
		return Input{}, workqueue.Change{}, conflict("Service lifecycle has no captured host runtime")
	}
	conditions, _, err := services.PrepareLifecycleRuntime(ctx, repository.store, captured.ServiceID,
		captured.Scope.EnvironmentID, captured.TaskID, captured.Action, view.ReadRevision, false)
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	conditions = append(conditions, guard, keyvalue.Condition{Key: workqueue.TaskKey(captured.TaskID), ModRevision: view.Task.Revision})
	pins, err := repository.hookPinGuard(ctx, captured, view.ReadRevision)
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	conditions = append(conditions, pins...)
	return captured, workqueue.Change{Conditions: conditions}, nil
}
