package componentwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareRouteCapture binds one native Task, its executable input and retained
// Secret membership to the request replay record. The initiating operation joins
// desired-state publication, configuration sources and the Environment epoch
// comparisons in the same transaction; this method never commits that transaction.
func (repository *InputRepository) PrepareRouteCapture(
	ctx context.Context,
	task workexecution.Task,
	item workexecution.Item,
	inputID string,
	input *agentpb.ComponentWorkInput,
	capture Capture,
	secrets tasksecretpins.Prepared,
	guards []keyvalue.Condition,
	at time.Time,
) (workexecution.Item, workqueue.Change, error) {
	if at.IsZero() || at.Location() != time.UTC || at.Before(task.CreatedAt) ||
		!at.Before(
			task.Deadline,
		) || !at.Before(item.Deadline) || item.CreatedAt.Before(task.CreatedAt) || item.CreatedAt.After(task.UpdatedAt) ||
		!item.UpdatedAt.Equal(item.CreatedAt) || len(item.Dependencies) != 0 ||
		(capture.Secrets == nil) != secrets.IsZero() {
		return workexecution.Item{}, workqueue.Change{}, conflict(
			"Route publication requires a fresh captured single action",
		)
	}
	activation := workqueue.Change{}
	if !secrets.IsZero() {
		binding, err := secrets.Binding()
		if err != nil {
			return workexecution.Item{}, workqueue.Change{}, err
		}
		if binding != *capture.Secrets {
			return workexecution.Item{}, workqueue.Change{}, conflict(
				"Route Secret preparation differs from captured membership",
			)
		}
		fragment, err := tasksecretpins.Activation(secrets)
		if err != nil {
			return workexecution.Item{}, workqueue.Change{}, err
		}
		defer fragment.Clear()
		activation.Conditions, activation.Mutations, err = tasksecretpins.EtcdFragment(fragment)
		if err != nil {
			return workexecution.Item{}, workqueue.Change{}, err
		}
	}
	defer keyvalue.ClearMutationValues(activation.Mutations)
	reference, staged, err := repository.stageRoute(ctx, task, item, inputID, input, capture, guards)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	keep := false
	defer func() {
		if !keep {
			keyvalue.ClearMutationValues(staged.Mutations)
		}
	}()
	ready, err := workexecution.PrepareInput(item, reference, item.CreatedAt)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	staged.Conditions = append(staged.Conditions, activation.Conditions...)
	for _, mutation := range activation.Mutations {
		mutation.Value = append([]byte(nil), mutation.Value...)
		staged.Mutations = append(staged.Mutations, mutation)
	}
	keep = true
	return ready, staged, nil
}
