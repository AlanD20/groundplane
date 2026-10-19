package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ScriptRecoverySources struct {
	Forward    workexecution.Attempt
	Ownership  *ScriptWorkOwnership
	Conditions []keyvalue.Condition
}

// LoadScriptRecoverySources follows the execution's exact original attempt,
// never the most recent recovery attempt or a current Script definition.
func (repository *ScriptRepository) LoadScriptRecoverySources(
	ctx context.Context,
	view workqueue.View,
) (ScriptRecoverySources, error) {
	loaded, err := repository.LoadRetainedScriptWork(ctx, view)
	if err != nil {
		return ScriptRecoverySources{}, err
	}
	defer loaded.Execution.Clear()
	execution := loaded.Execution
	if execution.AttemptID == "" || execution.Executor == nil ||
		execution.State == scriptexecutions.ScriptExecutionNotStarted ||
		execution.State == scriptexecutions.ScriptExecutionCleanupProven ||
		execution.Recovery != nil {
		return ScriptRecoverySources{}, errs.New(
			errs.KindTaskNotRetryable,
			"Script has no unresolved captured execution to clean up",
		)
	}
	attemptKey := workqueue.AttemptKey(execution.AttemptID)
	value, err := scriptexecutions.ScriptExecutionValueAt(ctx, repository.store, attemptKey, view.ReadRevision)
	if err != nil {
		return ScriptRecoverySources{}, err
	}
	defer clear(value.Value)
	forward, err := workqueue.DecodeAttempt(value.Value)
	if err != nil || forward.ID != execution.AttemptID || forward.Executor != *execution.Executor ||
		forward.Direction != workexecution.Forward || forward.FinishedAt == nil || forward.Result == nil ||
		forward.Outcome != workexecution.Failed || forward.TaskID != view.Task.Task.ID || forward.WorkItemID != view.Item.Item.ID ||
		forward.Input != execution.Input {
		return ScriptRecoverySources{}, errs.New(
			errs.KindStateConflict,
			"Script cleanup has no matching failed forward attempt",
		)
	}
	evidenceKey := scriptexecutions.ScriptExecutionKey(execution.ID) + "/interruptions/" + forward.ID
	evidence, err := scriptexecutions.ScriptExecutionValueAt(ctx, repository.store, evidenceKey, view.ReadRevision)
	if err != nil {
		return ScriptRecoverySources{}, err
	}
	defer clear(evidence.Value)
	digest := sha256.Sum256(evidence.Value)
	if forward.Result.ID != ids.DeriveAt(ids.KindPlan, forward.AssignedAt, forward.ID, "script-interruption") ||
		forward.Result.SHA256 != hex.EncodeToString(digest[:]) {
		return ScriptRecoverySources{}, errs.New(
			errs.KindStateConflict,
			"Script cleanup failure evidence differs from its retained result",
		)
	}
	ownership, err := capturedScriptOwnership(execution)
	if err != nil {
		return ScriptRecoverySources{}, err
	}
	conditions := append(loaded.Conditions,
		keyvalue.Condition{Key: attemptKey, ModRevision: value.ModRevision},
		keyvalue.Condition{Key: evidenceKey, ModRevision: evidence.ModRevision})
	return ScriptRecoverySources{Forward: forward, Ownership: ownership, Conditions: conditions}, nil
}

func (repository *ScriptRepository) LoadScriptRecoveryInput(
	ctx context.Context,
	view workqueue.ResumeView,
) (scriptexecution.RecoveryInput, error) {
	if !view.ActiveAttempt || !view.CurrentExecutor || view.Attempt.Attempt.Direction != workexecution.Recovery ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return scriptexecution.RecoveryInput{}, errs.New(
			errs.KindStateConflict,
			"Script cleanup does not own its active assignment",
		)
	}
	sources, err := repository.LoadScriptRecoverySources(ctx, view.Work)
	if err != nil {
		return scriptexecution.RecoveryInput{}, err
	}
	input := scriptexecution.RecoveryInput{Attempt: view.Attempt.Attempt, Forward: sources.Forward,
		OperationID: sources.Ownership.OperationID, ExecutionID: sources.Ownership.Run.ScriptExecutionId,
		Projection: sources.Ownership.Projection, Artifacts: sources.Ownership.Cleanup}
	return input, input.Validate()
}
