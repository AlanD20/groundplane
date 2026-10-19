package releasework

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// An earlier cleanup attempt may have started this hook. Its newly proved
// quiescence permits settlement, not a replacement command or invented exit.
func prepareInterruptedHook(
	ctx context.Context,
	store Reader,
	view workqueue.ResumeView,
	input releaseexecution.RecoveryInput,
	saved RecoverySnapshot,
	boundary releaseexecution.RecoveryReport,
	proof releaseexecution.FailureHookProof,
	at time.Time,
) (workqueue.Change, error) {
	if saved.Record == nil || saved.Record.Proofs[0] == nil || saved.Record.Boundaries[0] == nil ||
		proof.InterruptedCleanup == nil || *proof.InterruptedCleanup != *saved.Record.Proofs[0] {
		return workqueue.Change{}, invalid()
	}
	hook, err := boundary.Progress.ActiveHook(input)
	if err != nil {
		return workqueue.Change{}, err
	}
	proofBoundary := *saved.Record.Boundaries[0]
	keys := []string{scriptexecutions.ScriptExecutionKey(hook.ExecutionId),
		RecoveryEffectKey(input.Input.Attempt.ID, proofBoundary.Progress.Sequence)}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.Work.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	change := workqueue.Change{}
	for index, key := range keys {
		value := read.Values[index]
		if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.Work.ReadRevision {
			return workqueue.Change{}, invalid()
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: value.ModRevision})
	}
	effect, err := releaseexecution.DecodeRecoveryEffect(
		input,
		proofBoundary,
		read.Values[1].Value,
		*proof.InterruptedCleanup,
	)
	if err != nil || effect.Result.Quiescence == nil {
		return workqueue.Change{}, invalid()
	}
	record, err := recordcodec.Decode[scriptexecutions.ScriptExecutionRecord](read.Values[0].Value, "script-execution")
	reference := workexecution.Input{ID: hook.Input.InputId, SHA256: hex.EncodeToString(hook.Input.Sha256)}
	if err != nil || scriptexecutions.ValidateScriptExecutionRecord(record) != nil || record.ID != hook.ExecutionId ||
		record.ScriptID != hook.ScriptId || record.Input != reference || record.WorkInput == nil || *record.WorkInput != input.Input.Attempt.Input ||
		record.CurrentTaskID != input.Input.Attempt.TaskID || record.WorkItemID != input.Input.Attempt.WorkItemID ||
		record.OperationID != input.Input.Release.OperationId || record.EnvironmentID != input.Input.Release.EnvironmentId ||
		record.ServiceID != input.Input.Release.ServiceId || record.AttemptID == "" || record.AttemptID == input.Input.Attempt.ID || record.Executor == nil {
		return workqueue.Change{}, invalid()
	}
	attemptKey := workqueue.AttemptKey(record.AttemptID)
	prior, err := store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{attemptKey}, Revision: view.Work.ReadRevision},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	if prior == nil || prior.ReadRevision != view.Work.ReadRevision || len(prior.Values) != 1 ||
		prior.Values[0] == nil {
		return workqueue.Change{}, invalid()
	}
	defer keyvalue.ClearValues(prior.Values)
	value := prior.Values[0]
	previous, err := workqueue.DecodeAttempt(value.Value)
	if err != nil || value.Key != attemptKey || value.ModRevision <= 0 || previous.ID != record.AttemptID || previous.Direction != workexecution.Recovery ||
		previous.TaskID != input.Input.Attempt.TaskID || previous.WorkItemID != input.Input.Attempt.WorkItemID ||
		previous.Input != input.Input.Attempt.Input ||
		previous.Executor != *record.Executor ||
		previous.Executor.HostID != input.Input.Attempt.Executor.HostID ||
		previous.FinishedAt == nil ||
		previous.FinishedAt.After(input.Input.Attempt.AssignedAt) ||
		previous.Number >= input.Input.Attempt.Number {
		return workqueue.Change{}, invalid()
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: attemptKey, ModRevision: value.ModRevision})
	var observation *releaseexecution.HookQuiescenceProof
	for index := range effect.Result.Quiescence.Hooks {
		candidate := &effect.Result.Quiescence.Hooks[index]
		if candidate.ExecutionID == hook.ExecutionId && candidate.Input == reference {
			observation = candidate
		}
	}
	if observation == nil || !observation.Absent {
		return workqueue.Change{}, invalid()
	}
	if record.Recovery != nil {
		return change, nil
	}
	next, err := scriptexecutions.SettleRecovery(record, scriptexecutions.ScriptRecoverySettlement{
		AttemptID: input.Input.Attempt.ID, Evidence: *proof.InterruptedCleanup, Cleanup: observation.Cleanup,
		ObservedAt: observation.ObservedAt, SettledAt: at})
	if err != nil {
		return workqueue.Change{}, err
	}
	raw, err := recordcodec.Encode("script-execution", next)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: keys[0], Value: raw}}
	return change, nil
}
