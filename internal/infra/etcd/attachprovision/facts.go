package attachprovision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachoutputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareFacts stores one encrypted result per exact attempt. Replay preserves
// the first ciphertext and timestamp, even though encryption produces new bytes.
// Receiving facts accounts for a previously authorized effect; it grants no new
// command and is permitted after Stop while that attempt still owns its effects.
func (repository *InputRepository) PrepareFacts(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.ProvisionInput, output blueprintattachoutputs.Generation,
) (workqueue.Change, error) {
	attempt, task, item := view.Attempt.Attempt, view.Work.Task.Task, view.Work.Item.Item
	if blueprintattachoutputs.Validate(output) != nil || !workexecution.SameAssignment(input.Attempt, attempt) ||
		attempt.StartedAt == nil || input.Provision.GetCustomAttach() == nil ||
		output.EnvironmentID != task.Scope.EnvironmentID || output.TaskID != task.ID || output.WorkItemID != item.ID ||
		output.AttemptID != attempt.ID || output.Executor != attempt.Executor || output.Input != attempt.Input ||
		output.AttachID != item.TargetID || output.CreatedAt.Before(*attempt.StartedAt) || task.CollectionStartedAt != nil {
		return workqueue.Change{}, checkpointConflict("Attach facts do not match their captured execution")
	}
	source, err := repository.LoadCaptured(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer source.Clear()
	if source.Generation.Record.OperationID != output.OperationID || source.Generation.Record.Hook.Attach == nil ||
		source.Generation.Record.Provision != nil || source.Generation.Record.SettlesInController() {
		return workqueue.Change{}, checkpointConflict("Attach facts do not belong to a captured Custom hook")
	}
	progress, guards, err := repository.LoadProgress(ctx, view, input)
	if err != nil {
		return workqueue.Change{}, err
	}
	if progress == nil || progress.Sequence == 0 || progress.Sequence > 2 {
		return workqueue.Change{}, checkpointConflict("Attach facts have no acknowledged hook permission")
	}
	key := blueprintattachoutputs.Key(output.AttachID, attempt.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 {
		return workqueue.Change{}, checkpointConflict("Attach fact publication read is incomplete")
	}
	change := workqueue.Change{Conditions: append(source.Conditions, guards...)}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision})
	if saved := read.Values[0]; saved != nil {
		if saved.Key != key || saved.ModRevision <= 0 || saved.ModRevision > view.Work.ReadRevision {
			return workqueue.Change{}, checkpointConflict("Attach fact publication has invalid storage ownership")
		}
		existing, err := blueprintattachoutputs.Decode(saved.Value)
		if err != nil {
			return workqueue.Change{}, err
		}
		defer blueprintattachoutputs.Clear(&existing)
		if existing.EnvironmentID != output.EnvironmentID || existing.TaskID != output.TaskID ||
			existing.WorkItemID != output.WorkItemID || existing.AttemptID != output.AttemptID ||
			existing.Executor != output.Executor || existing.Input != output.Input || existing.AttachID != output.AttachID ||
			existing.OperationID != output.OperationID || existing.ResultSHA256 != output.ResultSHA256 {
			return workqueue.Change{}, checkpointConflict("Attach attempt already published different hook facts")
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: saved.ModRevision})
		change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: append([]byte(nil), saved.Value...)}}
		return change, nil
	}
	if !view.CurrentExecutor || !view.ActiveAttempt || task.FinishedAt != nil || attempt.FinishedAt != nil ||
		!progress.InFlight || progress.Completed != 0 || output.CreatedAt.Before(progress.UpdatedAt) {
		return workqueue.Change{}, checkpointConflict("Attach facts have no active, authorized hook execution")
	}
	raw, err := blueprintattachoutputs.Encode(output)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: raw}}
	return change, nil
}
