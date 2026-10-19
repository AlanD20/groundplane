package dnsresolver

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Work struct {
	records *resolverwork.Repository
	queue   *workqueue.Repository
}

func NewWork(records *resolverwork.Repository, queue *workqueue.Repository) (*Work, error) {
	if records == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "DNS resolver requires its native input and execution storage")
	}
	return &Work{records: records, queue: queue}, nil
}

func (owner *Work) Admit(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	item, task := request.Item.Item, request.Task.Task
	if request.DecisionOnly || item.Operation != workexecution.Resolver ||
		item.Placement.Executor != workexecution.Agent ||
		request.Executor.Record.Identity.HostID != item.Placement.HostID ||
		item.Input == nil {
		return workdispatch.Decision{}, errs.New(
			errs.KindStateConflict,
			"CoreDNS work cannot obtain this host assignment",
		)
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	if request.Direction == workexecution.Recovery {
		if !at.Before(item.Deadline) {
			change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
			return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
		}
		source, err := owner.records.LoadRecovery(ctx, view)
		return workdispatch.Decision{
			Disposition: workdispatch.Dispatch,
			Deadline:    item.Deadline,
			Change:      workqueue.Change{Conditions: source.Guards},
		}, err
	}
	if request.Direction != workexecution.Forward {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "CoreDNS assignment direction is invalid")
	}
	if task.Stop != nil || item.ForwardRevoked || !at.Before(item.Deadline) {
		outcome := workexecution.Failed
		if task.Stop != nil {
			outcome = task.Stop.Outcome
		} else if item.ForwardRevoked {
			outcome = workexecution.Superseded
		}
		change, err := owner.records.PrepareQueuedStop(ctx, view, outcome, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	_, change, current, err := owner.records.ReadAdmission(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if !current {
		settled, err := owner.records.PrepareQueuedStop(ctx, view, workexecution.Superseded, at)
		settled.Conditions = append(settled.Conditions, change.Conditions...)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: settled}, err
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline, Change: change}, nil
}

func (owner *Work) AuthorizeAgentStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		source, err := owner.records.LoadRecoveryForAttempt(ctx, view.Work, view.Attempt.Attempt)
		return workqueue.Change{Conditions: source.Guards}, err
	}
	_, change, current, err := owner.records.ReadAdmission(ctx, view.Work)
	if err == nil && !current {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"CoreDNS input was superseded before Start; submit a new action",
		)
	}
	return change, err
}

func (owner *Work) LoadWorkAssignment(
	ctx context.Context,
	view workqueue.ResumeView,
) (*agentpb.ResolverWorkAssignment, error) {
	if !view.ActiveAttempt || view.Work.Item.Item.Operation != workexecution.Resolver ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return nil, errs.New(errs.KindStateConflict, "CoreDNS delivery has no retained active assignment")
	}
	stored, err := owner.records.Load(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	identity, err := workexecution.EncodeAgentAttempt(view.Attempt.Attempt)
	if err != nil {
		return nil, err
	}
	raw, reference, err := resolverexecution.Capture(view.Attempt.Attempt.Input.ID, stored.Input)
	if err != nil || reference != view.Attempt.Attempt.Input {
		return nil, errs.New(errs.KindStateConflict, "CoreDNS assignment differs from its sealed host input")
	}
	delivery := &agentpb.ResolverWorkAssignment{Attempt: identity, Input: raw}
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		source, err := owner.records.LoadRecoveryForAttempt(ctx, view.Work, view.Attempt.Attempt)
		if err != nil {
			return nil, err
		}
		delivery.Recovery, err = resolverexecution.EncodeRecovery(source.Recovery)
		if err != nil {
			return nil, err
		}
	}
	return delivery, nil
}

func (owner *Work) Stop(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	if task.Task.Stop == nil || item.Item.Operation != workexecution.Resolver || item.Item.TaskID != task.Task.ID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "CoreDNS work has no matching stop request")
	}
	if err := ctx.Err(); err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
}

func (owner *Work) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot, revision int64, at time.Time,
) (workqueue.Change, error) {
	if item.Item.Operation != workexecution.Resolver || item.Item.ActiveAttemptID != "" ||
		task.Task.CollectionStartedAt != nil {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"CoreDNS still has active execution or expired input",
		)
	}
	view := workqueue.View{Task: task, Item: item, ReadRevision: revision}
	stored, err := owner.records.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if item.Item.AttemptNumber != 0 && (previous.Attempt.Result == nil || previous.Attempt.FinishedAt == nil ||
		previous.Attempt.Number != item.Item.AttemptNumber || previous.Attempt.TaskID != task.Task.ID || previous.Attempt.WorkItemID != item.Item.ID) {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "CoreDNS Retry needs its exact previous attempt")
	}
	change := workqueue.Change{Conditions: append([]keyvalue.Condition(nil), stored.Guards...)}
	deadline := at.Add(stored.Input.Deadline.Sub(stored.Input.CreatedAt))
	var retry workqueue.Change
	if item.Item.Phase == workexecution.Attention {
		if previous.Attempt.Result == nil {
			return workqueue.Change{}, errs.New(
				errs.KindTaskNotRetryable,
				"CoreDNS cleanup has no preceding attempt result",
			)
		}
		source, sourceErr := owner.records.LoadRecovery(ctx, view)
		if sourceErr != nil {
			return workqueue.Change{}, sourceErr
		}
		change.Conditions = append(change.Conditions, source.Guards...)
		retry, err = workqueue.PrepareRetry(
			task,
			item,
			previous,
			workexecution.RetryDecision{Direction: workexecution.Recovery,
				Result: *previous.Attempt.Result, NotBefore: at, Deadline: resolverwork.CleanupDeadline(at)},
			at,
		)
	} else {
		if task.Task.Stop != nil || item.Item.ForwardRevoked || item.Item.Phase != workexecution.Settled || item.Item.Outcome != workexecution.Failed {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "CoreDNS effects must settle before another forward attempt")
		}
		_, source, current, sourceErr := owner.records.ReadAdmission(ctx, view)
		if sourceErr != nil {
			return workqueue.Change{}, sourceErr
		}
		if !current {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "CoreDNS input changed; submit a new action instead of Retry")
		}
		change.Conditions = append(change.Conditions, source.Conditions...)
		if item.Item.AttemptNumber == 0 {
			retry, err = workqueue.PrepareUnstartedRetry(task, item, deadline, at)
		} else {
			retry, err = workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{Direction: workexecution.Forward,
				Result: *previous.Attempt.Result, NotBefore: at, Deadline: deadline}, at)
		}
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions, change.Mutations = append(change.Conditions, retry.Conditions...), retry.Mutations
	return change, nil
}
