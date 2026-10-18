package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SettleRecoveredScript commits cleanup evidence, execution settlement and claim
// release together. The forward outcome remains failed; no exit code is invented.
func (repository *ScriptRepository) SettleRecoveredScript(ctx context.Context, view workqueue.ResumeView,
	raw []byte, reference workexecution.Input, authorization []keyvalue.Condition, at time.Time,
) error {
	if len(authorization) == 0 || !view.CurrentExecutor || !view.ActiveAttempt || view.ExecutorRevision <= 0 ||
		view.Attempt.Attempt.StartedAt == nil {
		return errs.New(errs.KindStateConflict, "Script recovery settlement requires its active started executor")
	}
	input, err := repository.LoadScriptRecoveryInput(ctx, view)
	if err != nil {
		return err
	}
	proof, err := scriptexecution.DecodeRecoveryProof(input, raw, reference)
	if err != nil {
		return err
	}
	if at.Before(proof.ObservedAt) {
		return errs.New(errs.KindStateConflict, "Script recovery settlement predates its proof")
	}
	loaded, err := repository.LoadRetainedScriptWork(ctx, view.Work)
	if err != nil {
		return err
	}
	defer loaded.Execution.Clear()
	next, err := scriptexecutions.SettleRecovery(loaded.Execution, scriptexecutions.ScriptRecoverySettlement{
		AttemptID: input.Attempt.ID, Evidence: reference, Cleanup: proof.Cleanup,
		ObservedAt: proof.ObservedAt, SettledAt: at,
	})
	if err != nil {
		return err
	}
	value, err := recordcodec.Encode("script-execution", next)
	if err != nil {
		return err
	}
	defer clear(value)
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return err
	}
	request, err := queue.ReadCompletionAt(ctx, view)
	if err != nil {
		return err
	}
	request.At = at
	request.Result = workexecution.Completion{Outcome: workexecution.Succeeded, Evidence: reference, EffectsSettled: true}
	change, err := workqueue.PrepareCompletion(request)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	proofKey := scriptexecutions.RecoveryProofKey(input.ExecutionID, input.Attempt.ID)
	conditions := append(loaded.Conditions, authorization...)
	conditions = append(conditions,
		keyvalue.Condition{Key: proofKey},
		keyvalue.Condition{Key: workqueue.ExecutorKey(input.Attempt.Executor), ModRevision: view.ExecutorRevision},
		keyvalue.Condition{Key: workqueue.CheckpointKey(input.Attempt.ID), ModRevision: view.Checkpoint.Revision})
	result, err := queue.Commit(ctx, workqueue.Change{Conditions: conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: proofKey, Value: raw},
		{Type: keyvalue.MutationPut, Key: scriptexecutions.ScriptExecutionKey(input.ExecutionID), Value: value},
	}}, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Script recovery changed before settlement; reload the same attempt")
	}
	return nil
}
