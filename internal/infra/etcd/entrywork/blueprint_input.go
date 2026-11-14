package entrywork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareBlueprintInput activates the exact file sources with an existing
// waiting item. No new Task, desired revision or independent execution is made.
func (repository *InputRepository) PrepareBlueprintInput(ctx context.Context, view workqueue.View,
	input *agentpb.EntryConfigurationWorkInput, capture Capture, secrets tasksecretpins.Prepared,
	guards []keyvalue.Condition, at time.Time,
) (_ workqueue.Change, resultErr error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Action != workexecution.ActionApply || !task.PublicationComplete || task.Stop != nil ||
		task.FinishedAt != nil || task.CollectionStartedAt != nil || capture.BlueprintRemoval == nil ||
		view.Task.Revision <= 0 || view.Item.Revision <= 0 || item.Input != nil || item.ForwardRevoked ||
		item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Phase != workexecution.Waiting ||
		item.Wait == nil || item.Wait.Reason != workexecution.WaitInput || len(guards) == 0 ||
		at.IsZero() || at.Location() != time.UTC || !at.Before(task.Deadline) || !at.Before(item.Deadline) ||
		(capture.Secrets == nil) != secrets.IsZero() {
		return workqueue.Change{}, conflict("Entry omission input requires its accepted waiting Apply work")
	}
	guards = append(append([]keyvalue.Condition(nil), guards...),
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision})
	activation := workqueue.Change{}
	if !secrets.IsZero() {
		binding, err := secrets.Binding()
		if err != nil {
			return workqueue.Change{}, err
		}
		if binding != *capture.Secrets {
			return workqueue.Change{}, conflict("Entry cleanup Secret pins changed during preparation")
		}
		fragment, err := tasksecretpins.Activation(secrets)
		if err != nil {
			return workqueue.Change{}, err
		}
		activation.Conditions, activation.Mutations, err = tasksecretpins.EtcdFragment(fragment)
		fragment.Clear()
		if err != nil {
			return workqueue.Change{}, err
		}
	}
	defer func() {
		if resultErr != nil {
			keyvalue.ClearMutationValues(activation.Mutations)
		}
	}()
	reference, staged, err := repository.Stage(ctx, task, item,
		ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "entry-removal-input"), input, capture, guards)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer func() {
		if resultErr != nil {
			keyvalue.ClearMutationValues(staged.Mutations)
		}
	}()
	ready, err := workqueue.PrepareInput(view.Task, view.Item, reference, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	ready.Conditions = append(ready.Conditions, staged.Conditions...)
	ready.Conditions = append(ready.Conditions, activation.Conditions...)
	ready.Mutations = append(ready.Mutations, staged.Mutations...)
	ready.Mutations = append(ready.Mutations, activation.Mutations...)
	return ready, nil
}
