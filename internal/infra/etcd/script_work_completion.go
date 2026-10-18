package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SettleCompletedScriptWork consumes the acknowledged cleanup checkpoint. Worker
// return values cannot override a saved nonzero exit or skip cleanup proof.
func (repository *ScriptRepository) SettleCompletedScriptWork(ctx context.Context, view workqueue.ResumeView,
	authorization []keyvalue.Condition, at time.Time,
) error {
	if !view.CurrentExecutor {
		return errs.New(errs.KindStateConflict, "Script completion requires its current executor")
	}
	return repository.settleProvenScriptWork(ctx, view, authorization, at)
}

func (repository *ScriptRepository) settleProvenScriptWork(ctx context.Context, view workqueue.ResumeView,
	authorization []keyvalue.Condition, at time.Time,
) error {
	if len(authorization) == 0 || !view.ActiveAttempt || view.ExecutorRevision <= 0 ||
		view.Attempt.Attempt.Direction != workexecution.Forward || view.Attempt.Attempt.StartedAt == nil ||
		view.Checkpoint.Record == nil || view.Checkpoint.Revision <= 0 {
		return errs.New(errs.KindStateConflict, "Script completion requires its active executor and cleanup checkpoint")
	}
	loaded, err := repository.LoadRetainedScriptWork(ctx, view.Work)
	if err != nil {
		return err
	}
	defer loaded.Execution.Clear()
	execution, attempt, cursor := loaded.Execution, view.Attempt.Attempt, *view.Checkpoint.Record
	if execution.State != scriptexecutions.ScriptExecutionCleanupProven || execution.Outcome == nil || execution.Cleanup == nil ||
		execution.AttemptID != attempt.ID || execution.Executor == nil || *execution.Executor != attempt.Executor ||
		execution.ReconciliationRequired || cursor.AttemptID != attempt.ID ||
		!execution.UpdatedAt.Equal(cursor.RecordedAt) || execution.LastCheckpointSHA256 != cursor.Evidence.SHA256 ||
		at.Before(execution.UpdatedAt) {
		return errs.New(errs.KindStateConflict, "Script completion lacks acknowledged outcome and cleanup evidence")
	}
	evidenceKey := scriptexecutions.CheckpointEvidenceKey(execution.ID, attempt.ID, cursor.Sequence)
	value, err := scriptexecutions.ScriptExecutionValueAt(ctx, repository.store, evidenceKey, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	defer clear(value.Value)
	digest := sha256.Sum256(value.Value)
	proof, err := recordcodec.Decode[scriptexecutions.ScriptCheckpointInput](value.Value, "script-checkpoint-evidence")
	if err != nil {
		return err
	}
	evidenceID, err := scriptexecutions.CheckpointEvidenceID(execution.CurrentTaskID, execution.ID, attempt.ID, cursor.Sequence)
	if err != nil {
		return err
	}
	proof.PayloadSHA256 = hex.EncodeToString(digest[:])
	if scriptexecutions.ValidateScriptCheckpointInput(proof) != nil || cursor.Evidence.ID != evidenceID ||
		cursor.Evidence.SHA256 != proof.PayloadSHA256 || proof.TaskID != attempt.TaskID || proof.WorkItemID != attempt.WorkItemID ||
		proof.AttemptID != attempt.ID || proof.Executor != attempt.Executor || proof.ExecutionID != execution.ID ||
		proof.OperationID != execution.OperationID || proof.Input != execution.Input || proof.Sequence != cursor.Sequence ||
		proof.StageID != cursor.StageID || !proof.At.Equal(cursor.RecordedAt) ||
		proof.State != scriptexecutions.ScriptExecutionCleanupProven || proof.Evidence.Cleanup == nil ||
		*proof.Evidence.Cleanup != *execution.Cleanup {
		return errs.New(errs.KindStateConflict, "Script cleanup proof differs from the saved attempt checkpoint")
	}
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return err
	}
	var request workqueue.Completion
	if view.CurrentExecutor {
		request, err = queue.ReadCompletionAt(ctx, view)
	} else {
		request, err = queue.ReadInterruptedScriptAt(ctx, view)
	}
	if err != nil {
		return err
	}
	outcome := workexecution.Failed
	if execution.Outcome.Reason == scriptexecutions.ScriptOutcomeNormalExit && execution.Outcome.ExitCode != nil && *execution.Outcome.ExitCode == 0 {
		outcome = workexecution.Succeeded
	}
	// A successful process cannot undo cancellation/supersession. A failed
	// process stays failed even when cancellation races its cleanup.
	if outcome == workexecution.Succeeded {
		if view.Work.Task.Task.Stop != nil {
			outcome = view.Work.Task.Task.Stop.Outcome
		} else if view.Work.Item.Item.ForwardRevoked {
			outcome = workexecution.Superseded
		}
	}
	if execution.Outcome.Reason == scriptexecutions.ScriptOutcomeAbort || execution.Outcome.Reason == scriptexecutions.ScriptOutcomeAbortBeforeStart {
		if view.Work.Task.Task.Stop != nil {
			outcome = view.Work.Task.Task.Stop.Outcome
		} else if view.Work.Item.Item.ForwardRevoked {
			outcome = workexecution.Superseded
		}
	}
	request.At = at
	request.Result = workexecution.Completion{Outcome: outcome, Evidence: cursor.Evidence, EffectsSettled: true}
	change, err := workqueue.PrepareCompletion(request)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	conditions := append(loaded.Conditions, authorization...)
	conditions = append(conditions,
		keyvalue.Condition{Key: evidenceKey, ModRevision: value.ModRevision},
		keyvalue.Condition{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	result, err := queue.Commit(ctx, workqueue.Change{Conditions: conditions}, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Script completion changed before saving; reload the same attempt")
	}
	return nil
}
