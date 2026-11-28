package remoteimage

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/remotefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/remoteimages"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (service *Service) ReportRemoteImage(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.RemoteImageResultRequest) (*agentpb.RemoteImageResultAck, error) {
	attempt, localID, err := remotefetch.DecodeReport(request, authorization.Identity())
	if err != nil {
		return nil, err
	}
	return service.report(ctx, authorization, attempt, localID)
}

func (service *Service) ReconcileInterrupted(ctx context.Context, view workqueue.ResumeView,
	authorization agentregistration.WorkAuthorization) error {
	attempt := view.Attempt.Attempt
	if view.CurrentExecutor || !view.ActiveAttempt || attempt.FinishedAt != nil ||
		authorization.Identity() == attempt.Executor || authorization.Identity().HostID != attempt.Executor.HostID {
		return conflict()
	}
	receipt, err := authorization.ForReceipt(attempt.Executor)
	if err != nil {
		return err
	}
	// Uncertain pulls can leave only verified digest-addressed bytes. They
	// never remove or retag existing images, and no success witness is minted.
	_, err = service.report(ctx, receipt, attempt, "")
	return err
}

func (service *Service) report(ctx context.Context, authorization agentregistration.WorkAuthorization,
	supplied workexecution.Attempt, localID string) (*agentpb.RemoteImageResultAck, error) {
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := authorization.ReadResult(ctx, service.queue, supplied.TaskID, supplied.WorkItemID, supplied.ID)
	if err != nil {
		return nil, err
	}
	attempt, task, item := view.Attempt.Attempt, view.Work.Task.Task, view.Work.Item.Item
	if !view.CanSettleResult() || !workexecution.SameAssignment(supplied, attempt) ||
		attempt.Direction != workexecution.Forward || !matches(task, item) {
		return nil, conflict()
	}
	evidence, err := remotefetch.ResultReference(attempt, localID)
	if err != nil {
		return nil, err
	}
	if attempt.FinishedAt != nil {
		if attempt.Result == nil || *attempt.Result != evidence {
			return nil, conflict()
		}
		return remotefetch.EncodeAcknowledgement(attempt)
	}
	plan, err := service.capture(ctx, task, item, 0)
	if err != nil {
		return nil, err
	}
	if plan.Record.Executor != attempt.Executor {
		return nil, conflict()
	}
	completion, err := service.queue.ReadCompletionAt(ctx, view)
	if err != nil {
		return nil, err
	}
	completion.At = time.Now().UTC()
	completion.Result = workexecution.Completion{Outcome: workexecution.Failed,
		Evidence: evidence, EffectsSettled: true}
	owner := planGuard(plan)
	if task.Stop != nil {
		completion.Result.Outcome = task.Stop.Outcome
	} else if localID != "" {
		// The current Host can replay an exact receipt retained by an earlier
		// process. Deploy must still independently observe the image in Docker.
		if attempt.StartedAt == nil ||
			(localID != plan.Record.Image.ConfigDigest && localID != plan.Record.Image.ManifestDigest) {
			return nil, conflict()
		}
		prior, err := remoteimages.ReadWitness(ctx, service.reader, task.TargetID, task.TargetName, 0)
		if err != nil {
			return nil, err
		}
		witness, err := remoteimages.PrepareWitness(remoteimages.Witness{HostID: task.TargetID,
			Image: task.TargetName, LocalImageID: localID, Executor: attempt.Executor}, prior.Revision)
		if err != nil {
			return nil, err
		}
		owner.Conditions = append(owner.Conditions, witness.Conditions...)
		owner.Mutations = witness.Mutations
		completion.Result.Outcome = workexecution.Succeeded
	}
	defer keyvalue.ClearMutationValues(owner.Mutations)
	change, err := workqueue.PrepareCompletion(completion)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	transaction, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, owner, change)
	keyvalue.ClearValues(transaction.FailureReads)
	if err != nil {
		return nil, err
	}
	if !transaction.Succeeded {
		return nil, errs.New(
			errs.KindExecutionWriteContention,
			"Remote Fetch receipt authority changed before settlement",
		)
	}
	_, finished, err := workexecution.Finish(item, attempt, completion.Result, completion.At)
	if err != nil {
		return nil, err
	}
	return remotefetch.EncodeAcknowledgement(finished)
}
