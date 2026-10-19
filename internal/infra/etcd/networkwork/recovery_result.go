package networkwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func RecoveryResultKey(attemptID string) string {
	return "/v1/network-work-recovery-result/" + attemptID
}

func (repository *InputRepository) PrepareRecoveryResult(
	ctx context.Context,
	view workqueue.ResumeView,
	raw []byte,
	at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	attempt := view.Attempt.Attempt
	if !view.CanSettleResult() || !view.ActiveAttempt || attempt.FinishedAt != nil || attempt.StartedAt == nil ||
		view.Work.Task.Task.CollectionStartedAt != nil || at.Before(view.Work.Item.Item.UpdatedAt) {
		return workqueue.Change{}, workexecution.Completion{}, conflict(
			"Network recovery no longer owns active inspection",
		)
	}
	captured, err := repository.LoadRecoveryInput(ctx, view.Work, attempt)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	report, evidence, err := networkexecution.DecodeRecoveryReport(
		attempt,
		captured.Source,
		captured.Input.Action,
		captured.Input.NetworkId,
		captured.Boundary,
		raw,
	)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if report.Observation.At.After(at) {
		return workqueue.Change{}, workexecution.Completion{}, conflict(
			"Network recovery observation is newer than settlement",
		)
	}
	preserve := captured.Input.Action == agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_REMOVE &&
		report.Observation.Present
	resource, err := repository.prepareObservedApplied(
		ctx,
		view,
		captured.Input,
		report.Observation.Absent,
		preserve,
		report.Observation.At,
	)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if view.Work.Task.Task.Action == workexecution.ActionRemove && report.Observation.Absent {
		finalization, err := repository.prepareDirectRemovalFinalization(ctx, view, captured.Input)
		if err != nil {
			keyvalue.ZeroMutationBytes(resource.Mutations)
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		resource.Conditions = append(resource.Conditions, finalization.Conditions...)
		resource.Mutations = append(resource.Mutations, finalization.Mutations...)
	}
	if report.Observation.Present && (view.Work.Task.Task.Stop != nil || view.Work.Item.Item.ForwardRevoked) {
		fence, err := repository.prepareRemovalFenceRelease(ctx, view.Work)
		if err != nil {
			keyvalue.ZeroMutationBytes(resource.Mutations)
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		resource.Conditions = append(resource.Conditions, fence.Conditions...)
		resource.Mutations = append(resource.Mutations, fence.Mutations...)
	}
	if view.Work.Task.Task.Action == workexecution.ActionApply {
		ledger, err := blueprintunits.NewRepository(repository.store)
		if err != nil {
			keyvalue.ClearMutationValues(resource.Mutations)
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		receipt, err := ledger.PrepareNetworkRecoveryReceipt(
			ctx,
			view,
			captured.Source,
			captured.Boundary,
			captured.Input.Action,
			report,
		)
		if err != nil {
			keyvalue.ClearMutationValues(resource.Mutations)
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		resource.Conditions = append(resource.Conditions, receipt.Conditions...)
		resource.Mutations = append(resource.Mutations, receipt.Mutations...)
	}
	guards := append(captured.Conditions,
		keyvalue.Condition{Key: RecoveryResultKey(attempt.ID)},
		keyvalue.Condition{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	resource.Conditions = append(resource.Conditions, guards...)
	resource.Mutations = append(
		resource.Mutations,
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   RecoveryResultKey(attempt.ID),
			Value: append([]byte(nil), raw...),
		},
	)
	// Successful inspection settles uncertainty, not the failed forward action.
	return resource, workexecution.Completion{
		Outcome:        workexecution.Succeeded,
		Evidence:       evidence,
		EffectsSettled: true,
	}, nil
}

func (repository *InputRepository) LoadRecoveryResult(
	ctx context.Context,
	view workqueue.ResumeView,
) ([]byte, bool, error) {
	attempt := view.Attempt.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil {
		return nil, false, conflict("Network recovery has no saved terminal result")
	}
	captured, err := repository.LoadRecoveryInput(ctx, view.Work, attempt)
	if err != nil {
		return nil, false, err
	}
	read, err := repository.read(ctx, []string{RecoveryResultKey(attempt.ID)}, view.Work.ReadRevision)
	if err != nil {
		return nil, false, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, false, conflict("Network recovery result is unavailable")
	}
	raw := read.Values[0].Value
	report, evidence, err := networkexecution.DecodeRecoveryReport(
		attempt,
		captured.Source,
		captured.Input.Action,
		captured.Input.NetworkId,
		captured.Boundary,
		raw,
	)
	if err != nil {
		return nil, false, err
	}
	completed := report.BoundaryCompleted(captured.Boundary)
	if evidence != *attempt.Result || attempt.Outcome != workexecution.Succeeded ||
		report.Observation.At.After(*attempt.FinishedAt) {
		return nil, false, conflict("Network recovery result differs from its terminal evidence")
	}
	return append([]byte(nil), raw...), completed, nil
}
