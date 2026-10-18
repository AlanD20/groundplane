package resolverwork

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func RecoveryResultKey(attemptID string) string {
	return "/v1/execution/resolver/cleanup-results/" + attemptID
}

func (repository *Repository) PrepareRecoveryResult(ctx context.Context, view workqueue.ResumeView,
	raw []byte, at time.Time,
) (workqueue.Change, workexecution.Input, error) {
	attempt := view.Attempt.Attempt
	if !view.ActiveAttempt || attempt.Direction != workexecution.Recovery || attempt.FinishedAt != nil {
		return workqueue.Change{}, workexecution.Input{}, invalid("DNS resolver cleanup requires its actual active attempt")
	}
	stored, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	source, err := repository.LoadRecoveryForAttempt(ctx, view.Work, attempt)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	report, evidence, err := resolverexecution.DecodeRecoveryReport(attempt, stored.Input, source.Recovery, raw)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	if report.ObservedAt.After(at) {
		return workqueue.Change{}, workexecution.Input{}, invalid("DNS resolver cleanup observation is ahead of settlement time")
	}
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	completion, err := queue.ReadCompletionAt(ctx, view)
	if !view.CurrentExecutor && report.Status == resolverexecution.RecoveryInterrupted {
		completion, err = queue.ReadInterruptedAt(ctx, view, workexecution.Resolver)
	}
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	settled := report.Status == resolverexecution.RecoveryComplete
	outcome := workexecution.Failed
	if settled {
		outcome = workexecution.Succeeded
	}
	completion.At, completion.Result = at.UTC(), workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: settled}
	change, err := workqueue.PrepareCompletion(completion)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	change.Conditions = append(change.Conditions, source.Guards...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: RecoveryResultKey(attempt.ID)},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: RecoveryResultKey(attempt.ID), Value: bytes.Clone(raw)})
	if settled {
		restored, err := repository.prepareRestoredRuntime(ctx, view.Work, stored)
		if err != nil {
			return workqueue.Change{}, workexecution.Input{}, err
		}
		change.Conditions = append(change.Conditions, restored.Conditions...)
		change.Mutations = append(change.Mutations, restored.Mutations...)
	}
	return change, evidence, nil
}

// Recovery does not advance the applied head to failed desired input. The head
// must still be the captured predecessor; only its actual runtime health changes.
func (repository *Repository) prepareRestoredRuntime(ctx context.Context, view workqueue.View, stored StoredInput) (workqueue.Change, error) {
	key := componentrecord.RecordKey(stored.Input.ComponentID)
	read, err := repository.read(ctx, []string{key, AppliedKey(stored.Input.HostID, stored.Input.ComponentID)}, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || keyvalue.RevisionOf(read.Values[1]) != stored.Capture.AppliedRevision {
		return workqueue.Change{}, invalid("DNS resolver cleanup no longer has its exact applied predecessor")
	}
	current, err := componentrecord.DecodeRecord(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	var services []string
	if prior := stored.Input.Previous; prior != nil {
		services = []string{prior.Runtime.Services[0].ServiceId}
	}
	current, err = componentrecord.SetRuntime(current, services, "", stored.Input.Previous != nil)
	if err != nil {
		return workqueue.Change{}, err
	}
	raw, err := componentrecord.EncodeRecord(current)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision},
		{Key: AppliedKey(stored.Input.HostID, stored.Input.ComponentID), ModRevision: stored.Capture.AppliedRevision}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: raw}, componentrecord.WriteFenceMutation(stored.Input.ComponentID)}}, nil
}

func (repository *Repository) ReadRecoveryResult(ctx context.Context, view workqueue.ResumeView) ([]byte, keyvalue.Condition, error) {
	attempt := view.Attempt.Attempt
	if attempt.Direction != workexecution.Recovery || attempt.FinishedAt == nil || attempt.Result == nil {
		return nil, keyvalue.Condition{}, invalid("DNS resolver cleanup has no terminal receipt")
	}
	stored, err := repository.Load(ctx, view.Work)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	source, err := repository.LoadRecoveryForAttempt(ctx, view.Work, attempt)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	key := RecoveryResultKey(attempt.ID)
	read, err := repository.read(ctx, []string{key}, view.Work.ReadRevision)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, keyvalue.Condition{}, invalid("DNS resolver cleanup lost its terminal receipt")
	}
	report, evidence, err := resolverexecution.DecodeRecoveryReport(attempt, stored.Input, source.Recovery, read.Values[0].Value)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	outcome := workexecution.Failed
	if report.Status == resolverexecution.RecoveryComplete {
		outcome = workexecution.Succeeded
	}
	if evidence != *attempt.Result || attempt.Outcome != outcome {
		return nil, keyvalue.Condition{}, invalid("DNS resolver cleanup receipt differs from its accepted terminal outcome")
	}
	return bytes.Clone(read.Values[0].Value), keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}
