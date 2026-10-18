package backingprovisionwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func FailureKey(evidenceID string) string {
	return "/v1/execution/backing-provision/failures/" + evidenceID
}

func (repository *Repository) ReadFailure(ctx context.Context, view workqueue.ResumeView) (backingprovision.Failure, []byte, error) {
	attempt := view.Attempt.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil || attempt.Direction != workexecution.Forward || attempt.Outcome == workexecution.Succeeded {
		return backingprovision.Failure{}, nil, invalid("backing creation has no saved failed forward receipt")
	}
	input, _, err := repository.Load(ctx, view.Work)
	if err != nil {
		return backingprovision.Failure{}, nil, err
	}
	defer clear(input.Wire)
	procedure, err := input.Procedure()
	if err != nil {
		return backingprovision.Failure{}, nil, err
	}
	layout, err := backingprovision.CaptureLayout(attempt, procedure)
	if err != nil {
		return backingprovision.Failure{}, nil, err
	}
	key := FailureKey(attempt.Result.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if err != nil {
		return backingprovision.Failure{}, nil, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return backingprovision.Failure{}, nil, invalid("backing creation failed attempt lost its exact failure record")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.Work.ReadRevision {
		return backingprovision.Failure{}, nil, invalid("backing creation failure record revision is invalid")
	}
	failure, err := backingprovision.DecodeFailure(attempt, layout, value.Value, *attempt.Result)
	if err != nil {
		return backingprovision.Failure{}, nil, err
	}
	if !failure.RecordedAt.Equal(*attempt.FinishedAt) {
		return backingprovision.Failure{}, nil, invalid("backing creation failed receipt has different completion timing")
	}
	return failure, append([]byte(nil), value.Value...), nil
}

// PrepareFailure retains the Controller's own last checkpoint and primary
// outcome. A started helper requires separate quiescence/inspection; neither
// a failed healthcheck nor a deadline proves external execution has ended.
func (repository *Repository) PrepareFailure(ctx context.Context, view workqueue.ResumeView,
	reason backingprovision.FailureReason, at time.Time,
) (workqueue.Change, workexecution.Input, error) {
	if !view.ActiveAttempt || view.Attempt.Attempt.Direction != workexecution.Forward || view.Attempt.Attempt.FinishedAt != nil {
		return workqueue.Change{}, workexecution.Input{}, invalid("backing creation failure requires its actual active forward attempt")
	}
	input, guard, err := repository.Load(ctx, view.Work)
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
	if _, err := repository.LoadProgress(ctx, view); err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	failure := backingprovision.Failure{AttemptID: view.Attempt.Attempt.ID, Input: view.Attempt.Attempt.Input,
		Reason: reason, Checkpoint: view.Checkpoint.Record, RecordedAt: at.UTC()}
	value, evidence, err := backingprovision.EncodeFailure(view.Attempt.Attempt, layout, failure)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	defer clear(value)
	completion, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	request, err := completion.ReadCompletionAt(ctx, view)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	outcome := workexecution.Failed
	if view.Work.Task.Task.Stop != nil {
		outcome = view.Work.Task.Task.Stop.Outcome
	} else if view.Work.Item.Item.ForwardRevoked {
		outcome = workexecution.Superseded
	}
	request.At = at
	request.Result = workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: view.Attempt.Attempt.StartedAt == nil}
	var change workqueue.Change
	if request.Result.EffectsSettled {
		change, err = workqueue.PrepareCompletion(request)
	} else {
		// This first bounded cleanup only restores the captured absent
		// runtime. It does not replay the hook or retry failed cleanup.
		budget := procedure.Deadline.AsTime().Sub(procedure.CreatedAt.AsTime())
		change, err = workqueue.PrepareCompletionWithRecovery(request, at.Add(budget))
	}
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	key := FailureKey(evidence.ID)
	change.Conditions = append(change.Conditions, guard, keyvalue.Condition{Key: key},
		keyvalue.Condition{Key: workqueue.CheckpointKey(view.Attempt.Attempt.ID), ModRevision: view.Checkpoint.Revision})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: append([]byte(nil), value...)})
	return change, evidence, nil
}
