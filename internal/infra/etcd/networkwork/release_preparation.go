package networkwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareReleaseEnsure stages only the exact bridge selected by a direct
// Release. Existing roots are immutable replay, not a reason to reread desired
// state. The caller joins a new root with a native Task revision advance, keeping
// graph publication fenced until every captured input is durably present.
func (repository *InputRepository) PrepareReleaseEnsure(ctx context.Context, task workqueue.TaskSnapshot,
	item workexecution.Item, input *agentpb.NetworkWorkInput, desiredRevision string,
	owner []keyvalue.Condition, revision int64,
) (workqueue.Change, bool, error) {
	if task.Revision <= 0 || revision < task.Revision || task.Task.PublicationComplete || task.Task.FinishedAt != nil ||
		task.Task.Action != workexecution.ActionDeploy && task.Task.Action != workexecution.ActionRollback ||
		item.TaskID != task.Task.ID || item.Operation != workexecution.Network || item.Input == nil ||
		item.Phase != workexecution.Ready || item.AttemptNumber != 0 || item.ForwardRevoked ||
		item.Placement.Executor != workexecution.Agent || len(owner) == 0 ||
		input == nil || input.Action != agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_ENSURE ||
		input.EnvironmentId != task.Task.Scope.EnvironmentID || input.NetworkId != item.TargetID {
		return workqueue.Change{}, false, conflict("Release bridge preparation requires its accepted unpublished graph")
	}
	if err := item.Validate(); err != nil {
		return workqueue.Change{}, false, err
	}
	desired, err := idempotency.EncodeTaskReference(desiredRevision)
	if err != nil {
		return workqueue.Change{}, false, err
	}
	_, raw, expected, err := executionplan.CaptureNetworkWorkInput(item.Input.ID, input)
	clear(raw)
	if err != nil || expected != *item.Input {
		if err != nil {
			return workqueue.Change{}, false, err
		}
		return workqueue.Change{}, false, conflict("Release bridge input differs from its captured reference")
	}
	keys := []string{InputKey(item.ID), DesiredKey(item.ID)}
	read, err := repository.read(ctx, keys, revision)
	if err != nil {
		return workqueue.Change{}, false, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] != nil || read.Values[1] != nil {
		if read.Values[0] == nil || read.Values[1] == nil {
			return workqueue.Change{}, false, conflict("Release bridge preparation is only partially published")
		}
		captured, err := idempotency.DecodeTaskReference(read.Values[1].Value)
		if err != nil || captured != desiredRevision {
			return workqueue.Change{}, false, conflict("Release bridge desired source changed during graph preparation")
		}
		_, conditions, err := repository.Load(ctx, workqueue.View{Task: task,
			Item: workqueue.ItemSnapshot{Item: item}, ReadRevision: revision})
		if err != nil {
			return workqueue.Change{}, false, err
		}
		conditions = append(conditions, keyvalue.Condition{Key: keys[1], ModRevision: read.Values[1].ModRevision})
		return workqueue.Change{Conditions: conditions}, true, nil
	}
	reference, change, err := repository.Stage(ctx, task.Task.ID, item.ID, item.Input.ID, input, owner)
	if err != nil {
		return workqueue.Change{}, false, err
	}
	if reference != *item.Input {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, false, conflict("Release bridge staging changed its captured input")
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: keys[1]})
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: keys[1], Value: desired},
	)
	return change, false, nil
}
