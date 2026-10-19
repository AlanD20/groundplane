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

// Replacement does not prove that cleanup completed. Preserve the original
// forward result and resource claims; settle only the obsolete recovery slot.
func (repository *ScriptRepository) ReconcileInterruptedScriptRecovery(ctx context.Context, view workqueue.ResumeView,
	authorization agentregistration.WorkAuthorization, at time.Time,
) error {
	guards, err := repository.scriptReplacementConditions(ctx, view, authorization)
	if err != nil {
		return err
	}
	return repository.settleInterruptedScriptRecovery(ctx, view, guards, at)
}

func (repository *ScriptRepository) settleInterruptedScriptRecovery(
	ctx context.Context,
	view workqueue.ResumeView,
	guards []keyvalue.Condition,
	at time.Time,
) error {
	if view.Work.Item.Item.Operation != workexecution.Script ||
		view.Attempt.Attempt.Direction != workexecution.Recovery ||
		len(guards) == 0 ||
		view.Checkpoint.Record != nil ||
		view.Checkpoint.Revision != 0 {
		return errs.New(errs.KindStateConflict, "Script cleanup failure requires its captured recovery assignment")
	}
	sources, err := repository.LoadScriptRecoverySources(ctx, view.Work)
	if err != nil {
		return err
	}
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return err
	}
	var request workqueue.Completion
	if view.CurrentExecutor {
		request, err = queue.ReadCompletionAt(ctx, view)
	} else {
		request, err = queue.ReadInterruptedScriptRecoveryAt(ctx, view)
	}
	if err != nil {
		return err
	}
	attempt := view.Attempt.Attempt
	raw, err := recordcodec.Encode("script-recovery-interruption", struct {
		AttemptID        string              `json:"attempt_id"`
		ForwardAttemptID string              `json:"forward_attempt_id"`
		ExecutionID      string              `json:"execution_id"`
		Input            workexecution.Input `json:"input"`
		At               time.Time           `json:"at"`
	}{attempt.ID, sources.Forward.ID, sources.Ownership.Run.ScriptExecutionId, attempt.Input, at})
	if err != nil {
		return err
	}
	defer clear(raw)
	digest := sha256.Sum256(raw)
	evidence := workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, attempt.AssignedAt, attempt.ID, "script-recovery-interruption"),
		SHA256: hex.EncodeToString(digest[:]),
	}
	request.At, request.Result = at, workexecution.Completion{
		Outcome:        workexecution.Failed,
		Evidence:       evidence,
		EffectsSettled: false,
	}
	change, err := workqueue.PrepareCompletion(request)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	key := scriptexecutions.ScriptExecutionKey(sources.Ownership.Run.ScriptExecutionId) + "/interruptions/" + attempt.ID
	conditions := append(sources.Conditions, guards...)
	conditions = append(
		conditions,
		keyvalue.Condition{Key: key},
		keyvalue.Condition{Key: workqueue.CheckpointKey(attempt.ID)},
	)
	result, err := queue.Commit(ctx, workqueue.Change{Conditions: conditions,
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: raw}}}, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Script cleanup changed before its interruption was recorded")
	}
	return nil
}
