package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *EnvironmentBlueprintRepository) AbandonVolumeWorkInput(ctx context.Context,
	accepted workqueue.TaskSnapshot, workID string,
) (bool, error) {
	draft, revision, err := repository.readNativePreparationDraft(ctx, accepted.Task.ID)
	if err != nil {
		return false, err
	}
	if revision > 0 {
		if accepted.Task.AcceptedIntent == nil || draft.Kind != "volume" ||
			draft.Intent != *accepted.Task.AcceptedIntent {
			return false, errs.New(errs.KindStateConflict, "Volume cleanup does not own its native draft")
		}
		workID = draft.WorkID
	}
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return false, err
	}
	current, _, err := queue.ReadTask(ctx, accepted.Task.ID, 0)
	if err != nil {
		return false, err
	}
	if current.Task.Intent != (workexecution.Input{}) {
		return false, nil
	}
	if current.Task.ExpectedItems != 0 || current.Task.AcceptedIntent == nil || accepted.Task.AcceptedIntent == nil ||
		*current.Task.AcceptedIntent != *accepted.Task.AcceptedIntent {
		return false, errs.New(errs.KindStateConflict, "Volume preparation no longer owns its accepted input")
	}
	// Advance the pure Task revision while abandoning inert captures. Other
	// compilers using the old acceptance guard cannot promote deleted chunks.
	raw, err := workqueue.EncodeTask(current.Task)
	if err != nil {
		return false, err
	}
	defer clear(raw)
	result, err := queue.Commit(ctx, workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: workqueue.TaskKey(current.Task.ID), ModRevision: current.Revision},
			{Key: volumework.InputKey(workID)}},
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: workqueue.TaskKey(current.Task.ID), Value: raw},
			{Type: keyvalue.MutationDelete, Key: volumework.InputPrefix(workID), Prefix: true},
		}})
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return false, err
	}
	if !result.Succeeded {
		return false, nil
	}
	return result.Succeeded, nil
}
