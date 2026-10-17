package networkwork

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareBlueprintInput binds executable Network input to an existing waiting
// work item. The coordinator supplies captured-graph, desired-head and applied
// state comparisons and commits this change once; no child Task is published.
func (repository *InputRepository) PrepareBlueprintInput(ctx context.Context, view workqueue.View,
	input *agentpb.NetworkWorkInput, dependencies []workqueue.ItemSnapshot,
	authority []keyvalue.Condition, at time.Time,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || input == nil ||
		view.Task.Revision <= 0 || view.Item.Revision <= 0 ||
		view.ReadRevision < view.Task.Revision || view.ReadRevision < view.Item.Revision ||
		task.Action != workexecution.ActionApply || !task.PublicationComplete ||
		task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		item.TaskID != task.ID || item.Operation != workexecution.Network ||
		item.Placement.Executor != workexecution.Agent || item.ForwardRevoked ||
		item.Phase != workexecution.Waiting || item.Wait == nil || item.Wait.Reason != workexecution.WaitInput ||
		item.Input != nil || item.AttemptNumber != 0 || len(authority) == 0 ||
		at.IsZero() || !at.Before(task.Deadline) || !at.Before(item.Deadline) ||
		input.EnvironmentId != task.Scope.EnvironmentID || input.NetworkId != item.TargetID ||
		hex.EncodeToString(input.IntentSha256) != task.Intent.SHA256 ||
		(input.Action != agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_ENSURE &&
			input.Action != agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_REMOVE) {
		return workqueue.Change{}, conflict("Blueprint Network input requires its waiting captured work")
	}
	dependencyGuards, err := workqueue.DependencyConditions(item, dependencies)
	if err != nil {
		return workqueue.Change{}, err
	}
	guards := append([]keyvalue.Condition(nil), authority...)
	guards = append(guards, dependencyGuards...)
	guards = append(guards,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
	)
	inputID := ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "network-input")
	reference, captured, err := repository.Stage(ctx, task.ID, item.ID, inputID, input, guards)
	if err != nil {
		return workqueue.Change{}, err
	}
	ready, err := workqueue.PrepareInput(view.Task, view.Item, reference, at)
	if err != nil {
		keyvalue.ZeroMutationBytes(captured.Mutations)
		return workqueue.Change{}, err
	}
	desired, err := idempotency.EncodeTaskReference(task.ID)
	if err != nil {
		keyvalue.ZeroMutationBytes(captured.Mutations)
		keyvalue.ZeroMutationBytes(ready.Mutations)
		return workqueue.Change{}, err
	}
	ready.Conditions = append(ready.Conditions, captured.Conditions...)
	ready.Conditions = append(ready.Conditions, keyvalue.Condition{Key: DesiredKey(item.ID)})
	ready.Mutations = append(ready.Mutations, captured.Mutations...)
	ready.Mutations = append(ready.Mutations, keyvalue.Mutation{
		Type: keyvalue.MutationPut, Key: DesiredKey(item.ID), Value: desired,
	})
	return ready, nil
}

func DesiredKey(workID string) string { return InputPrefix(workID) + "desired" }
