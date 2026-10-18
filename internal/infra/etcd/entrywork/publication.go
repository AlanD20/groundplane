package entrywork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PreparePublication binds one native Task, its executable input and retained
// Secret membership to the request replay record. The initiating operation joins
// desired-state publication, configuration sources and the Environment epoch
// comparisons in the same transaction; this method never commits that transaction.
func (repository *InputRepository) PreparePublication(ctx context.Context, task workexecution.Task, item workexecution.Item,
	inputID string, input *agentpb.EntryConfigurationWorkInput, capture Capture, secrets tasksecretpins.Prepared,
	marker idempotency.IdempotencyMarker, guards []keyvalue.Condition, at time.Time,
) (workexecution.Item, workqueue.Change, error) {
	if at.IsZero() || at.Location() != time.UTC || at.Before(task.CreatedAt) ||
		!at.Before(task.Deadline) || !at.Before(item.Deadline) || !item.CreatedAt.Equal(task.CreatedAt) ||
		!item.UpdatedAt.Equal(item.CreatedAt) || len(item.Dependencies) != 0 ||
		(capture.Secrets == nil) != secrets.IsZero() {
		return workexecution.Item{}, workqueue.Change{}, conflict("Entry publication requires a fresh captured single action")
	}
	activation := workqueue.Change{}
	if !secrets.IsZero() {
		binding, err := secrets.Binding()
		if err != nil {
			return workexecution.Item{}, workqueue.Change{}, err
		}
		if binding != *capture.Secrets {
			return workexecution.Item{}, workqueue.Change{}, conflict("Entry Secret preparation differs from captured membership")
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
	markerKey, err := idempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	markerValue, err := idempotency.EncodeIdempotencyMarker(marker)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	defer clear(markerValue)
	reference, staged, err := repository.Stage(ctx, task, item, inputID, input, capture, guards)
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
	queue, err := workqueue.PrepareSingleTask(task, ready, marker)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	staged.Conditions = append(staged.Conditions, queue.Conditions...)
	staged.Conditions = append(staged.Conditions, activation.Conditions...)
	staged.Conditions = append(staged.Conditions, keyvalue.Condition{Key: markerKey})
	staged.Mutations = append(staged.Mutations, queue.Mutations...)
	for _, mutation := range activation.Mutations {
		mutation.Value = append([]byte(nil), mutation.Value...)
		staged.Mutations = append(staged.Mutations, mutation)
	}
	staged.Mutations = append(staged.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: markerKey, Value: append([]byte(nil), markerValue...)})
	keep = true
	return ready, staged, nil
}
