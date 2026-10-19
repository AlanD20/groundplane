package releasework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type RecoveryCompletionProof struct {
	Progress   RecoverySnapshot
	Evidence   [22]releaseexecution.RecoveryEffectEvidence
	Conditions []keyvalue.Condition
}

// LoadRecoveryCompletionProof rechecks every action proof, not merely the last
// status. This retains the original failure and supplies atomic comparisons for
// the operation owner to join with resource/claim settlement.
func LoadRecoveryCompletionProof(
	ctx context.Context,
	store Reader,
	view workqueue.ResumeView,
	input releaseexecution.RecoveryInput,
) (RecoveryCompletionProof, error) {
	if ctx == nil || store == nil || !view.ActiveAttempt || view.ExecutorRevision <= 0 ||
		!workexecution.SameAssignment(input.Input.Attempt, view.Attempt.Attempt) || view.Checkpoint.Record == nil {
		return RecoveryCompletionProof{}, invalid()
	}
	saved, err := LoadRecovery(ctx, store, input, view.Work.ReadRevision)
	if err != nil {
		return RecoveryCompletionProof{}, err
	}
	if saved.Record == nil || saved.Record.Report.Progress.Phase != releaseexecution.RecoveryComplete ||
		saved.Record.Report.Progress.InFlight || saved.Revision != view.Checkpoint.Revision {
		return RecoveryCompletionProof{}, invalid()
	}
	checkpoint, err := saved.Record.Report.Checkpoint(input)
	if err != nil || !releaseexecution.EqualCheckpoint(checkpoint, *view.Checkpoint.Record) {
		return RecoveryCompletionProof{}, invalid()
	}
	keys := make([]string, input.RecoveryActionCount())
	for index, proof := range saved.Record.Proofs[:len(keys)] {
		if proof == nil || saved.Record.Boundaries[index] == nil {
			return RecoveryCompletionProof{}, invalid()
		}
		keys[index] = RecoveryEffectKey(input.Input.Attempt.ID, saved.Record.Boundaries[index].Progress.Sequence)
	}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.Work.ReadRevision})
	if err != nil {
		return RecoveryCompletionProof{}, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != len(keys) {
		return RecoveryCompletionProof{}, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	attempt := input.Input.Attempt
	result := RecoveryCompletionProof{Progress: saved, Conditions: []keyvalue.Condition{
		{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision},
		{Key: RecoveryKey(attempt.ID), ModRevision: saved.Revision},
		{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision},
	}}
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 ||
			value.ModRevision > view.Work.ReadRevision {
			return RecoveryCompletionProof{}, invalid()
		}
		boundary := *saved.Record.Boundaries[index]
		evidence, err := releaseexecution.DecodeRecoveryEffect(
			input,
			boundary,
			value.Value,
			*saved.Record.Proofs[index],
		)
		if err != nil || evidence.RecordedAt.After(saved.Record.Report.Progress.UpdatedAt) ||
			index+1 < len(keys) && evidence.RecordedAt.After(saved.Record.Boundaries[index+1].Progress.UpdatedAt) {
			return RecoveryCompletionProof{}, invalid()
		}
		result.Evidence[index] = evidence
		result.Conditions = append(
			result.Conditions,
			keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision},
		)
	}
	return result, nil
}
