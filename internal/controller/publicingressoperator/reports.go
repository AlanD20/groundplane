package publicingressoperator

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	storage "github.com/AlanD20/groundplane/internal/infra/etcd/publicingress"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (work *Work) ReportPublicIngress(
	ctx context.Context, authorization agentregistration.WorkAuthorization, request *agentpb.PublicIngressResultRequest,
) (*agentpb.PublicIngressResultAck, error) {
	attempt, report, err := ingressexecution.DecodeReport(request, authorization.Identity())
	if err != nil {
		return nil, err
	}
	return work.report(ctx, authorization, attempt, report)
}

func (work *Work) ReconcileInterrupted(
	ctx context.Context, view workqueue.ResumeView, authorization agentregistration.WorkAuthorization,
) error {
	attempt := view.Attempt.Attempt
	if view.CurrentExecutor || !view.ActiveAttempt || attempt.FinishedAt != nil ||
		authorization.Identity() == attempt.Executor || authorization.Identity().HostID != attempt.Executor.HostID {
		return conflict("public ingress interruption has no replacement Agent for its Host")
	}
	receipt, err := authorization.ForReceipt(attempt.Executor)
	if err != nil {
		return err
	}
	_, err = work.report(ctx, receipt, attempt, ingressexecution.Report{Result: ingressexecution.Uncertain})
	return err
}

func (work *Work) report(
	ctx context.Context, authorization agentregistration.WorkAuthorization, supplied workexecution.Attempt,
	proof ingressexecution.Report,
) (*agentpb.PublicIngressResultAck, error) {
	if err := proof.Validate(); err != nil {
		return nil, err
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := authorization.ReadResult(ctx, work.queue, supplied.TaskID, supplied.WorkItemID, supplied.ID)
	if err != nil {
		return nil, err
	}
	attempt, task, item := view.Attempt.Attempt, view.Work.Task.Task, view.Work.Item.Item
	if !view.CanSettleResult() || !workexecution.SameAssignment(supplied, attempt) || !matchesHost(task, item) {
		return nil, conflict("public ingress report does not match its authenticated assignment")
	}
	ref, err := ingressexecution.ResultReference(attempt, proof)
	if err != nil {
		return nil, err
	}
	if attempt.FinishedAt != nil {
		if attempt.Result == nil || *attempt.Result != ref {
			return nil, conflict("public ingress report differs from its saved result")
		}
		return ingressexecution.EncodeAcknowledgement(attempt)
	}
	completion, err := work.queue.ReadCompletionAt(ctx, view)
	if err != nil {
		return nil, err
	}
	owner, result, err := work.prepareResult(ctx, task, item, attempt, proof, ref)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearMutationValues(owner.Mutations)
	completion.Result, completion.At = result, time.Now().UTC()
	var change workqueue.Change
	if !result.EffectsSettled && attempt.Direction == workexecution.Forward {
		change, err = workqueue.PrepareCompletionWithRecovery(completion, completion.At.Add(activationTimeout))
	} else {
		change, err = workqueue.PrepareCompletion(completion)
	}
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	transaction, err := work.queue.Commit(ctx, workqueue.Change{Conditions: guards}, owner, change)
	keyvalue.ClearValues(transaction.FailureReads)
	if err != nil {
		return nil, err
	}
	if !transaction.Succeeded {
		return nil, conflict("public ingress result authority changed before settlement")
	}
	_, finished, err := workexecution.Finish(item, attempt, result, completion.At)
	if err != nil {
		return nil, err
	}
	return ingressexecution.EncodeAcknowledgement(finished)
}

func (work *Work) prepareResult(
	ctx context.Context, task workexecution.Task, item workexecution.Item, attempt workexecution.Attempt,
	proof ingressexecution.Report, evidence workexecution.Input,
) (workqueue.Change, workexecution.Completion, error) {
	state, plan, err := work.captured(ctx, task, item)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	outcome := workexecution.Completion{Evidence: evidence, Outcome: workexecution.Failed}
	if attempt.StartedAt == nil && attempt.Direction == workexecution.Forward {
		if state.Pending.Authorized || proof.Result != ingressexecution.Uncertain {
			return workqueue.Change{}, outcome, conflict("unstarted public ingress cannot prove host effects")
		}
		if task.Stop != nil {
			outcome.Outcome = task.Stop.Outcome
		}
		outcome.EffectsSettled = true
		change, err := work.operator.ingress.PrepareCancel(ctx, task.TargetID, task.ID)
		return change, outcome, err
	}
	if !state.Pending.Authorized {
		return workqueue.Change{}, outcome, conflict("public ingress has no durable effect authorization")
	}
	switch proof.Result {
	case ingressexecution.Applied:
		if attempt.StartedAt == nil || proof.Receipt.Revision != plan.Revision ||
			proof.Receipt.Digest != plan.Digest || plan.Predecessor != nil && proof.Receipt.RuntimeID != plan.Predecessor.RuntimeID {
			return workqueue.Change{}, outcome, conflict("public ingress result differs from its reviewed activation")
		}
		// Recovery may independently verify a candidate whose result was
		// lost. Publish that proof while retaining the failed forward attempt.
		outcome.Outcome, outcome.EffectsSettled = workexecution.Succeeded, true
		change, err := work.operator.ingress.PrepareSettle(ctx, task.TargetID, task.ID, storage.Receipt(*proof.Receipt))
		return change, outcome, err
	case ingressexecution.Restored:
		if attempt.StartedAt == nil || plan.Predecessor == nil || *proof.Receipt != *plan.Predecessor {
			return workqueue.Change{}, outcome, conflict("public ingress recovery did not prove its predecessor")
		}
		if attempt.Direction == workexecution.Recovery {
			outcome.Outcome = workexecution.Succeeded
		}
		outcome.EffectsSettled = true
		change, err := work.operator.ingress.PrepareRestore(
			ctx,
			task.TargetID,
			task.ID,
			storage.Receipt(*proof.Receipt),
		)
		return change, outcome, err
	default:
		return stateGuard(state), outcome, nil
	}
}
