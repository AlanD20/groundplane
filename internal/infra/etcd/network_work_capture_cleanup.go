package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *EnvironmentBlueprintRepository) AbandonNetworkWorkInput(ctx context.Context,
	accepted workqueue.TaskSnapshot, workID string, detaches []string,
) (bool, error) {
	draft, revision, err := repository.readNativePreparationDraft(ctx, accepted.Task.ID)
	if err != nil {
		return false, err
	}
	if revision > 0 {
		if accepted.Task.AcceptedIntent == nil || draft.Kind != "network" ||
			draft.Intent != *accepted.Task.AcceptedIntent {
			return false, errs.New(errs.KindStateConflict, "Zone cleanup does not own its native draft")
		}
		workID, detaches = draft.WorkID, draft.Detaches
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
		return false, errs.New(errs.KindStateConflict, "Zone preparation no longer owns its accepted input")
	}
	// Advance the pure Task revision while abandoning inert captures. Other
	// compilers using the old acceptance guard cannot promote deleted chunks.
	raw, err := workqueue.EncodeTask(current.Task)
	if err != nil {
		return false, err
	}
	defer clear(raw)
	conditions := []keyvalue.Condition{
		{Key: workqueue.TaskKey(current.Task.ID), ModRevision: current.Revision},
		{Key: networkwork.InputKey(workID)},
	}
	if revision > 0 {
		conditions = append(
			conditions,
			keyvalue.Condition{Key: nativePreparationDraftKey(current.Task.ID), ModRevision: revision},
		)
	}
	result, err := queue.Commit(ctx, workqueue.Change{
		Conditions: conditions,
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: workqueue.TaskKey(current.Task.ID), Value: raw},
			{Type: keyvalue.MutationDelete, Key: networkwork.InputPrefix(workID), Prefix: true},
		}})
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return false, err
	}
	if !result.Succeeded {
		return false, nil
	}
	current.Revision = result.Revision
	start := int(draft.Cleaned)
	end := min(start+8, len(detaches))
	for _, detachID := range detaches[start:end] {
		cleaned, err := queue.Commit(ctx, workqueue.Change{Conditions: []keyvalue.Condition{
			{Key: workqueue.TaskKey(current.Task.ID), ModRevision: current.Revision},
			{Key: attachremoval.WorkCaptureKey(detachID)}, {Key: workqueue.ItemKey(detachID)}},
			Mutations: []keyvalue.Mutation{
				{Type: keyvalue.MutationDelete, Key: attachremoval.DetachInputPrefix(detachID), Prefix: true},
			}})
		keyvalue.ClearValues(cleaned.FailureReads)
		if err != nil {
			return false, err
		}
		if !cleaned.Succeeded {
			return false, nil
		}
	}
	if revision > 0 && end > start {
		draft.Cleaned = uint32(end)
		progress, err := recordcodec.Encode("native-preparation-draft", draft)
		if err != nil {
			return false, err
		}
		defer clear(progress)
		saved, err := queue.Commit(ctx, workqueue.Change{Conditions: []keyvalue.Condition{
			{Key: workqueue.TaskKey(current.Task.ID), ModRevision: current.Revision},
			{Key: nativePreparationDraftKey(current.Task.ID), ModRevision: revision}},
			Mutations: []keyvalue.Mutation{
				{Type: keyvalue.MutationPut, Key: nativePreparationDraftKey(current.Task.ID), Value: progress},
			}})
		keyvalue.ClearValues(saved.FailureReads)
		if err != nil {
			return false, err
		}
		if !saved.Succeeded {
			return false, nil
		}
	}
	if end < len(detaches) {
		return false, nil
	}
	cleaned, err := queue.Commit(ctx, workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.TaskKey(current.Task.ID), ModRevision: current.Revision}},
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationDelete, Key: networkwork.CascadeGraphPrefix(current.Task.ID), Prefix: true},
		}})
	keyvalue.ClearValues(cleaned.FailureReads)
	if err != nil {
		return false, err
	}
	return cleaned.Succeeded, nil
}
