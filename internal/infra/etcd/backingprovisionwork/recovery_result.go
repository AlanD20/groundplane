package backingprovisionwork

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func RecoveryResultKey(attemptID string) string {
	return "/v1/execution/backing-provision/cleanup-results/" + attemptID
}

func (repository *Repository) PrepareRecoveryResult(ctx context.Context, view workqueue.ResumeView,
	raw []byte, at time.Time,
) (workqueue.Change, workexecution.Input, error) {
	if !view.ActiveAttempt || view.Attempt.Attempt.Direction != workexecution.Recovery || view.Attempt.Attempt.FinishedAt != nil {
		return workqueue.Change{}, workexecution.Input{}, invalid("backing creation cleanup requires its actual started assignment")
	}
	input, _, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	defer clear(input.Wire)
	procedure, err := input.Procedure()
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	layout, err := backingprovision.CaptureLayout(view.Attempt.Attempt, procedure)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	source, err := repository.LoadRecoveryForAttempt(ctx, view.Work, view.Attempt.Attempt)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	report, evidence, err := backingprovision.DecodeRecoveryReport(view.Attempt.Attempt, source.Recovery, layout, raw)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	if report.ObservedAt.After(at) {
		return workqueue.Change{}, workexecution.Input{}, invalid("backing creation cleanup observation is ahead of its settlement time")
	}
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	completion, err := queue.ReadCompletionAt(ctx, view)
	if !view.CanSettleResult() && report.Status == backingprovision.RecoveryInterrupted {
		completion, err = queue.ReadInterruptedAt(ctx, view, workexecution.BackingProvision)
	}
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	settled := report.Status == backingprovision.RecoveryComplete
	outcome := workexecution.Failed
	if settled {
		outcome = workexecution.Succeeded
	}
	completion.At, completion.Result = at, workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: settled}
	change, err := workqueue.PrepareCompletion(completion)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	key := RecoveryResultKey(view.Attempt.Attempt.ID)
	change.Conditions = append(change.Conditions, source.Conditions...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: workqueue.ExecutorKey(view.Attempt.Attempt.Executor), ModRevision: view.ExecutorRevision})
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: bytes.Clone(raw)})
	if settled {
		directories, err := repository.prepareDirectoryState(ctx, view.Work, input, false)
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, workexecution.Input{}, err
		}
		change.Conditions = append(change.Conditions, directories.Conditions...)
		change.Mutations = append(change.Mutations, directories.Mutations...)
	}
	return change, evidence, nil
}

func (repository *Repository) ReadRecoveryResult(ctx context.Context, view workqueue.ResumeView) ([]byte, error) {
	attempt := view.Attempt.Attempt
	if attempt.Direction != workexecution.Recovery || attempt.FinishedAt == nil || attempt.Result == nil {
		return nil, invalid("backing creation cleanup has no terminal receipt")
	}
	input, _, err := repository.Load(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	defer clear(input.Wire)
	procedure, err := input.Procedure()
	if err != nil {
		return nil, err
	}
	layout, err := backingprovision.CaptureLayout(attempt, procedure)
	if err != nil {
		return nil, err
	}
	source, err := repository.LoadRecoveryForAttempt(ctx, view.Work, attempt)
	if err != nil {
		return nil, err
	}
	key := RecoveryResultKey(attempt.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, invalid("backing creation cleanup lost its saved result")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.Work.ReadRevision {
		return nil, invalid("backing creation cleanup result has an invalid revision")
	}
	report, evidence, err := backingprovision.DecodeRecoveryReport(attempt, source.Recovery, layout, value.Value)
	outcome := workexecution.Failed
	if report.Status == backingprovision.RecoveryComplete {
		outcome = workexecution.Succeeded
	}
	if err != nil || evidence != *attempt.Result || attempt.Outcome != outcome {
		return nil, invalid("backing creation cleanup result differs from its terminal receipt")
	}
	return bytes.Clone(value.Value), nil
}
