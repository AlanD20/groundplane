package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// SettleRecoveredServiceRemoval preserves resource state selected before the
// failed removal. That state was never retired: successful restoration proves
// the captured runtime again, rather than creating a new Release or publishing
// the removal's candidate Blueprint. Cleanup has its own successful result;
// workqueue retains the original forward failure on the visible work item.
func (repository *ServiceRepository) SettleRecoveredServiceRemoval(ctx context.Context,
	view workqueue.ResumeView, delivery *agentpb.ServiceLifecycleWorkInput,
	authorization []keyvalue.Condition, at time.Time,
) error {
	if repository == nil || repository.store == nil || len(authorization) == 0 ||
		view.Work.Task.Task.Action != workexecution.ActionRemove &&
			view.Work.Task.Task.Action != workexecution.ActionApply {
		return errs.New(errs.KindStateConflict, "service removal restoration requires authenticated removal settlement")
	}
	proof, result, err := repository.PrepareRecoveredServiceRemoval(ctx, view, delivery, at)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(proof.Mutations)
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return err
	}
	request, err := queue.ReadCompletionAt(ctx, view)
	if err != nil {
		return err
	}
	request.At = at
	request.Result = result
	completion, err := workqueue.PrepareCompletion(request)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(completion.Mutations)
	committed, err := queue.Commit(ctx, workqueue.Change{Conditions: authorization}, proof, completion)
	keyvalue.ClearValues(committed.FailureReads)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "Service removal recovery state changed; reload the same attempt")
	}
	return nil
}

// PrepareRecoveredServiceRemoval joins the retained restoration proof and fence
// release. Its caller supplies either current-Agent authentication or replacement-
// Agent interruption guards, and commits queue settlement in the same transaction.
func (repository *ServiceRepository) PrepareRecoveredServiceRemoval(ctx context.Context, view workqueue.ResumeView,
	delivery *agentpb.ServiceLifecycleWorkInput, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	if view.Work.Task.Task.Action != workexecution.ActionRemove &&
		view.Work.Task.Task.Action != workexecution.ActionApply {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(
			errs.KindStateConflict,
			"service removal restoration requires removal settlement",
		)
	}
	inputs, err := serviceremovalwork.NewRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	input, checkpoint, proof, err := inputs.PrepareRecoveryProof(ctx, view, delivery, at)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	proof.Mutations = append(proof.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete,
		Key: deletions.TombstoneKey(string(deletions.DeletionTargetService), input.ServiceID)})
	return proof, workexecution.Completion{
		Outcome:        workexecution.Succeeded,
		EffectsSettled: true,
		Evidence:       checkpoint.Evidence,
	}, nil
}
