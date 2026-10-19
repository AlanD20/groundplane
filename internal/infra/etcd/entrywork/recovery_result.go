package entrywork

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func RecoveryResultKey(attemptID string) string { return "/v1/entry-work-recovery-result/" + attemptID }

// PrepareRecoveryResult preserves the original applied configuration/runtime
// identities. Only the queue cleanup result changes, in the same transaction.
func (repository *InputRepository) PrepareRecoveryResult(ctx context.Context, view workqueue.ResumeView,
	raw []byte, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	return repository.prepareRecoveryResult(ctx, view, raw, at, false)
}

func (repository *InputRepository) prepareRecoveryResult(
	ctx context.Context,
	view workqueue.ResumeView,
	raw []byte,
	at time.Time,
	interrupted bool,
) (workqueue.Change, workexecution.Completion, error) {
	attempt := view.Attempt.Attempt
	if view.CanSettleResult() == interrupted || !view.ActiveAttempt || view.ExecutorRevision <= 0 ||
		attempt.FinishedAt != nil || !interrupted && attempt.StartedAt == nil || attempt.Direction != workexecution.Recovery ||
		view.Work.Task.Task.CollectionStartedAt != nil || at.IsZero() || at.Location() != time.UTC ||
		at.Before(view.Work.Item.Item.UpdatedAt) {
		return workqueue.Change{}, workexecution.Completion{}, conflict(
			"Entry recovery no longer owns active execution",
		)
	}
	captured, err := repository.LoadRecoveryInput(ctx, view.Work, attempt)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	id, err := entryexecution.RecoveryResultID(attempt.ID)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	digest := sha256.Sum256(raw)
	evidence := workexecution.Input{ID: id, SHA256: hex.EncodeToString(digest[:])}
	report, err := entryexecution.DecodeRecoveryReport(
		attempt,
		captured.Source,
		captured.Input,
		captured.Boundary,
		raw,
		evidence,
	)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if report.ObservedAt.After(at) {
		return workqueue.Change{}, workexecution.Completion{}, conflict(
			"Entry recovery observation is newer than its settlement",
		)
	}
	if interrupted && (report.EffectsStopped || report.FilesRestored || report.ConsumersRestored) {
		return workqueue.Change{}, workexecution.Completion{}, conflict(
			"Interrupted Entry recovery cannot infer host restoration",
		)
	}
	guards := captured.Conditions
	if report.Settled(captured.Input) {
		// Failed forward work never advanced these acknowledgements. A successful
		// restoration must still name exactly that predecessor, not newer state.
		resources, err := repository.AdmissionConditions(ctx, view.Work)
		if err != nil {
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		guards = append(guards, resources...)
	}
	guards = append(guards,
		keyvalue.Condition{Key: RecoveryResultKey(attempt.ID)},
		keyvalue.Condition{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	outcome := workexecution.Failed
	settled := report.Settled(captured.Input)
	if settled {
		outcome = workexecution.Succeeded
	}
	return workqueue.Change{Conditions: guards, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: RecoveryResultKey(attempt.ID), Value: append([]byte(nil), raw...)},
	}}, workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: settled}, nil
}

func (repository *InputRepository) LoadRecoveryResult(ctx context.Context, view workqueue.ResumeView) ([]byte, error) {
	attempt := view.Attempt.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil || attempt.Direction != workexecution.Recovery {
		return nil, conflict("Entry recovery has no retained result")
	}
	captured, err := repository.LoadRecoveryInput(ctx, view.Work, attempt)
	if err != nil {
		return nil, err
	}
	read, err := repository.read(ctx, []string{RecoveryResultKey(attempt.ID)}, view.Work.ReadRevision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, conflict("Entry recovery result is unavailable")
	}
	raw := read.Values[0].Value
	report, err := entryexecution.DecodeRecoveryReport(
		attempt,
		captured.Source,
		captured.Input,
		captured.Boundary,
		raw,
		*attempt.Result,
	)
	if err != nil {
		return nil, err
	}
	if report.ObservedAt.After(*attempt.FinishedAt) ||
		(attempt.Outcome == workexecution.Succeeded) != report.Settled(captured.Input) {
		return nil, conflict("Entry recovery result differs from its recorded outcome")
	}
	return append([]byte(nil), raw...), nil
}
