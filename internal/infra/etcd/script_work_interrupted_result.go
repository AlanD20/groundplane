package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReconcileInterruptedScript records failure without replaying a shell command.
// Only absence of command authorization permits releasing resource protection.
// Otherwise the worker slot is released but claims remain for explicit cleanup.
func (repository *ScriptRepository) ReconcileInterruptedScript(ctx context.Context, view workqueue.ResumeView,
	authorization agentregistration.WorkAuthorization, at time.Time,
) error {
	guards, err := repository.scriptReplacementConditions(ctx, view, authorization)
	if err != nil {
		return err
	}
	return repository.settleInterruptedScript(ctx, view, guards, at)
}

func (repository *ScriptRepository) settleInterruptedScript(
	ctx context.Context,
	view workqueue.ResumeView,
	guards []keyvalue.Condition,
	at time.Time,
) error {
	if view.Work.Item.Item.Operation != workexecution.Script ||
		view.Attempt.Attempt.Direction != workexecution.Forward ||
		len(guards) == 0 {
		return errs.New(errs.KindStateConflict, "Script failure requires its captured forward assignment")
	}
	loaded, err := repository.LoadRetainedScriptWork(ctx, view.Work)
	if err != nil {
		return err
	}
	defer loaded.Execution.Clear()
	execution := loaded.Execution
	if execution.State == scriptexecutions.ScriptExecutionCleanupProven {
		return repository.settleProvenScriptWork(ctx, view, guards, at)
	}
	noEffects := execution.State == scriptexecutions.ScriptExecutionNotStarted && !execution.StartAuthorized &&
		execution.AttemptID == "" && execution.Executor == nil && view.Checkpoint.Record == nil
	if !noEffects && (view.Checkpoint.Record == nil || execution.AttemptID != view.Attempt.Attempt.ID ||
		execution.Executor == nil || *execution.Executor != view.Attempt.Attempt.Executor) {
		return errs.New(errs.KindStateConflict, "interrupted Script evidence does not match its attempt")
	}
	if at.Before(execution.UpdatedAt) {
		return errs.New(errs.KindStateConflict, "Script interruption predates its saved evidence")
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
	raw, err := recordcodec.Encode("script-interruption", struct {
		AttemptID      string                                `json:"attempt_id"`
		ExecutionID    string                                `json:"execution_id"`
		Input          workexecution.Input                   `json:"input"`
		State          scriptexecutions.ScriptExecutionState `json:"state"`
		Checkpoint     *workexecution.Checkpoint             `json:"checkpoint,omitempty"`
		EffectsSettled bool                                  `json:"effects_settled"`
		At             time.Time                             `json:"at"`
	}{view.Attempt.Attempt.ID, execution.ID, execution.Input, execution.State, view.Checkpoint.Record, noEffects, at})
	if err != nil {
		return err
	}
	defer clear(raw)
	digest := sha256.Sum256(raw)
	reference := workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, view.Attempt.Attempt.AssignedAt,
		view.Attempt.Attempt.ID, "script-interruption"), SHA256: hex.EncodeToString(digest[:])}
	outcome := workexecution.Failed
	if noEffects {
		if view.Work.Task.Task.Stop != nil {
			outcome = view.Work.Task.Task.Stop.Outcome
		} else if view.Work.Item.Item.ForwardRevoked {
			outcome = workexecution.Superseded
		}
	}
	request.At, request.Result = at, workexecution.Completion{
		Outcome:        outcome,
		Evidence:       reference,
		EffectsSettled: noEffects,
	}
	change, err := workqueue.PrepareCompletion(request)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	key := scriptexecutions.ScriptExecutionKey(execution.ID) + "/interruptions/" + view.Attempt.Attempt.ID
	conditions := append(loaded.Conditions, guards...)
	conditions = append(
		conditions,
		keyvalue.Condition{Key: key},
		keyvalue.Condition{
			Key:         workqueue.CheckpointKey(view.Attempt.Attempt.ID),
			ModRevision: view.Checkpoint.Revision,
		},
	)
	result, err := queue.Commit(ctx, workqueue.Change{Conditions: conditions,
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: raw}}}, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Script changed before interruption could be recorded")
	}
	return nil
}
