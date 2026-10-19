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

// PreparePublication joins the captured input root, exact dependency receipts,
// queue readiness and parent counters. The caller adds its resource comparisons
// and configuration/source retention writes to the same transaction.
func (repository *InputRepository) PreparePublication(ctx context.Context, view workqueue.View,
	inputID string, input *agentpb.ComponentWorkInput, capture Capture, secrets tasksecretpins.Prepared,
	dependencies []workqueue.ItemSnapshot, at time.Time,
) (workexecution.Input, workqueue.Change, error) {
	if !at.Before(view.Task.Task.Deadline) || !at.Before(view.Item.Item.Deadline) {
		return workexecution.Input{}, workqueue.Change{}, conflict("Component input preparation exceeded its deadline")
	}
	conditions, err := workqueue.DependencyConditions(view.Item.Item, dependencies)
	if err != nil {
		return workexecution.Input{}, workqueue.Change{}, err
	}
	for _, dependency := range dependencies {
		if dependency.Revision > view.ReadRevision {
			return workexecution.Input{}, workqueue.Change{}, conflict(
				"Component dependency is newer than the captured preparation snapshot",
			)
		}
	}
	activation := workqueue.Change{}
	if (capture.Secrets == nil) != secrets.IsZero() {
		return workexecution.Input{}, workqueue.Change{}, conflict(
			"Component input must publish its captured Secret retention",
		)
	}
	if !secrets.IsZero() {
		binding, err := secrets.Binding()
		if err != nil {
			return workexecution.Input{}, workqueue.Change{}, err
		}
		if binding != *capture.Secrets {
			return workexecution.Input{}, workqueue.Change{}, conflict(
				"Component Secret preparation differs from captured membership",
			)
		}
		fragment, err := tasksecretpins.Activation(secrets)
		if err != nil {
			return workexecution.Input{}, workqueue.Change{}, err
		}
		defer fragment.Clear()
		activation.Conditions, activation.Mutations, err = tasksecretpins.EtcdFragment(fragment)
		if err != nil {
			return workexecution.Input{}, workqueue.Change{}, err
		}
	}
	defer keyvalue.ClearMutationValues(activation.Mutations)
	reference, change, err := repository.Stage(ctx, view, inputID, input, capture)
	if err != nil {
		return workexecution.Input{}, workqueue.Change{}, err
	}
	ready, err := workqueue.PrepareInput(view.Task, view.Item, reference, at)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workexecution.Input{}, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, conditions...)
	change.Conditions = append(change.Conditions, ready.Conditions...)
	change.Mutations = append(change.Mutations, ready.Mutations...)
	change.Conditions = append(change.Conditions, activation.Conditions...)
	for _, mutation := range activation.Mutations {
		mutation.Value = append([]byte(nil), mutation.Value...)
		change.Mutations = append(change.Mutations, mutation)
	}
	return reference, change, nil
}
