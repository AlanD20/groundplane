// Package taskdependencies gates whole accepted Tasks before their operation
// owners capture execution inputs. Member-operation DAGs remain separate.
package taskdependencies

import (
	"context"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PreparationOwner decodes its own encrypted typed accepted request. It may
// capture current runtime/image evidence only after all predecessor Tasks have
// fully succeeded. It may stage bounded, inert immutable capture drafts after
// that gate. It performs no workload effects or execution claims; final source
// seals, claims and graph publication join the original accepted Task CAS.
type PreparationOwner interface {
	PrepareAccepted(context.Context, workqueue.TaskSnapshot, int64, time.Time) (Prepared, error)
}

type Prepared struct {
	Task            workexecution.Task
	Graph           []workexecution.Item
	Authority       workqueue.Change
	Planning        bool
	Staged          bool
	Initial         []workexecution.Item
	Pending         *Pending
	BackingCreation bool
	Decision        *workexecution.Input
	// Finalize releases operation-owned inert capture drafts. True is reserved
	// for a known successful final native promotion; unknown outcomes pass false
	// and the owner checks its durable active-root guard before abandonment.
	Finalize func(context.Context, bool) error
}

// Pending is an explicit owner decision for a recognized temporary prerequisite
// or source-read race. Returning an ordinary domain error does not request retry.
type Pending struct {
	Reason workexecution.WaitReason
	Detail string
}

type Registration struct {
	Action  workexecution.Action
	Target  ids.Kind
	Kind    string
	Owner   PreparationOwner
	Cleanup func(context.Context, workqueue.TaskSnapshot, time.Time) (bool, error)
}

type ownerKey struct {
	action workexecution.Action
	target ids.Kind
	kind   string
}

// ImageTarget selects an existing exact host-image identity, whose public ID is
// a Docker content digest rather than a Groundplane resource-id prefix.
const ImageTarget ids.Kind = "image"

type Preparations struct {
	queue           *workqueue.Repository
	owners          map[ownerKey]PreparationOwner
	cleanups        map[ownerKey]func(context.Context, workqueue.TaskSnapshot, time.Time) (bool, error)
	backingCreation *workqueue.BackingCreationPublisher
}

func (preparations *Preparations) ConfigureBackingCreation(publisher *workqueue.BackingCreationPublisher) {
	preparations.backingCreation = publisher
}

func NewPreparations(queue *workqueue.Repository, registrations []Registration) (*Preparations, error) {
	if queue == nil || len(registrations) == 0 {
		return nil, errs.New(errs.KindInternal, "accepted Task preparation requires storage and typed owners")
	}
	owners := make(map[ownerKey]PreparationOwner, len(registrations))
	cleanups := make(map[ownerKey]func(context.Context, workqueue.TaskSnapshot, time.Time) (bool, error))
	for _, registration := range registrations {
		key := ownerKey{action: registration.Action, target: registration.Target, kind: registration.Kind}
		if !workexecution.ValidAction(registration.Action) || registration.Target == "" || registration.Kind == "" ||
			registration.Owner == nil ||
			owners[key] != nil {
			return nil, errs.New(errs.KindInternal, "accepted Task preparation owner is invalid or duplicated")
		}
		owners[key] = registration.Owner
		cleanups[key] = registration.Cleanup
	}
	return &Preparations{queue: queue, owners: owners, cleanups: cleanups}, nil
}

// Advance executes one bounded coordination decision. Waiting has no work
// items, host claims, grants or Agent slot. A durable dependency decision is
// committed before calling an owner, so retries cannot silently reselect it.
func (preparations *Preparations) Advance(
	ctx context.Context,
	taskID string,
	at time.Time,
) (progressed bool, failure error) {
	ctx = imagefence.WithScope(ctx)
	task, revision, err := preparations.queue.ReadTask(ctx, taskID, 0)
	if err != nil {
		return false, err
	}
	if !task.Task.PendingPreparation() {
		return false, nil
	}
	decision, dependencyChange, err := preparations.queue.PrepareTaskDependencyProgress(ctx, task, at)
	if err != nil {
		return false, err
	}
	defer keyvalue.ClearMutationValues(dependencyChange.Mutations)
	if len(dependencyChange.Mutations) != 0 {
		result, err := preparations.queue.Commit(ctx, dependencyChange)
		return result.Succeeded, err
	}
	if task.Task.Stop != nil || !at.Before(task.Task.Deadline) || decision.Failed != nil {
		outcome := workexecution.Failed
		code, detail := "preparation.deadline", "Task expired while waiting for prerequisites or execution preparation. No workload operation started."
		if task.Task.Stop != nil {
			outcome = task.Task.Stop.Outcome
			code, detail = "preparation.cancelled", "This Task was cancelled before execution inputs were prepared. No workload operation started."
			if outcome == workexecution.Superseded {
				code, detail = "preparation.superseded", "Newer work superseded this Task before execution inputs were prepared. No workload operation started."
			}
		} else if decision.Failed != nil {
			code = "dependency.failed"
			detail = "A prerequisite Task failed, was cancelled or was superseded. This Task did not start; inspect the linked predecessor."
		}
		// The dependency record retains the failed predecessor ID/outcome for
		// inspection. No fabricated execution step or hidden child is created.
		evidence, reference, err := workqueue.PrepareAcceptedPreparationFailure(task, code, detail, at)
		if err != nil {
			return false, err
		}
		defer keyvalue.ClearMutationValues(evidence.Mutations)
		failure, err := preparations.queue.PreparePlanningRejection(ctx, task, outcome,
			reference, at, at.Add(workexecution.TaskHistoryRetention))
		if err != nil {
			return false, err
		}
		defer keyvalue.ClearMutationValues(failure.Mutations)
		result, err := preparations.queue.Commit(ctx, dependencyChange, evidence, failure)
		return result.Succeeded, err
	}
	if !decision.Ready {
		return false, nil
	}
	kind, _, _ := strings.Cut(task.Task.TargetID, "_")
	if workloadimage.LocalIDValid(task.Task.TargetID) {
		kind = string(ImageTarget)
	}
	inputKind, kindGuard, err := preparations.queue.ReadAcceptedInputKind(ctx, task, revision)
	if err != nil {
		return preparations.rejectCompilerFailure(ctx, task, dependencyChange, err, at)
	}
	dependencyChange.Conditions = append(dependencyChange.Conditions, kindGuard.Conditions...)
	owner := preparations.owners[ownerKey{action: task.Task.Action, target: ids.Kind(kind), kind: inputKind}]
	if owner == nil {
		return preparations.rejectCompilerFailure(
			ctx,
			task,
			dependencyChange,
			errs.New(
				errs.KindStateConflict,
				"This action has no configured preparation owner; check the Controller installation before submitting it again",
			),
			at,
		)
	}
	prepared, err := owner.PrepareAccepted(ctx, task, revision, at)
	defer keyvalue.ClearMutationValues(prepared.Authority.Mutations)
	applied := false
	defer func() {
		if prepared.Finalize == nil {
			return
		}
		bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if err := prepared.Finalize(bounded, applied); err != nil {
			failure = errs.WrapJoined(errs.KindInternal, failure, err)
		}
	}()
	if err != nil {
		return preparations.rejectCompilerFailure(ctx, task, dependencyChange, err, at)
	}
	if prepared.Pending != nil {
		if prepared.Pending.Reason != workexecution.WaitInput &&
			prepared.Pending.Reason != workexecution.WaitDependency &&
			prepared.Pending.Reason != workexecution.WaitConflict ||
			strings.TrimSpace(prepared.Pending.Detail) == "" ||
			len(prepared.Graph) != 0 ||
			len(prepared.Authority.Mutations) != 0 {
			return false, errs.New(
				errs.KindInternal,
				"pending preparation must identify one temporary prerequisite without publishing work",
			)
		}
		detail := strings.TrimSpace(prepared.Pending.Detail)
		if len(detail) > 512 {
			detail = detail[:512]
			for !utf8.ValidString(detail) {
				detail = detail[:len(detail)-1]
			}
		}
		change, err := preparations.queue.PrepareAcceptedPreparationWait(ctx, task, prepared.Pending.Reason, detail, at)
		if err != nil || len(change.Mutations) == 0 {
			return false, err
		}
		defer keyvalue.ClearMutationValues(change.Mutations)
		result, err := preparations.queue.Commit(ctx, dependencyChange, change)
		return result.Succeeded, err
	}
	if prepared.Planning {
		if len(prepared.Graph) != 0 {
			return false, errs.New(errs.KindInternal, "accepted intent planning cannot also publish execution work")
		}
		if len(prepared.Authority.Mutations) == 0 {
			return false, nil
		}
		if len(prepared.Authority.Conditions) == 0 {
			return false, errs.New(errs.KindInternal, "accepted intent planning requires captured source comparisons")
		}
		guard := workqueue.Change{
			Conditions: []keyvalue.Condition{{Key: workqueue.TaskKey(taskID), ModRevision: task.Revision}},
		}
		result, err := preparations.queue.Commit(ctx, guard, dependencyChange, prepared.Authority)
		return result.Succeeded, err
	}
	if len(prepared.Authority.Conditions) == 0 {
		return false, errs.New(errs.KindInternal, "accepted intent preparation requires captured source comparisons")
	}
	var promotion workqueue.Change
	if prepared.Decision != nil {
		if prepared.Staged || len(prepared.Graph) != 1 || prepared.BackingCreation {
			return false, errs.New(
				errs.KindInternal,
				"atomic metadata decision requires one unstaged Controller work item",
			)
		}
		promotion, err = preparations.queue.PrepareAcceptedTaskDecision(
			ctx,
			task,
			prepared.Task,
			prepared.Graph[0],
			*prepared.Decision,
			at,
		)
	} else {
		promotion, err = preparations.queue.PrepareAcceptedTaskPromotion(ctx, task, prepared.Task, prepared.Graph, prepared.Initial, prepared.Staged, at)
	}
	if err != nil {
		return false, err
	}
	defer keyvalue.ClearMutationValues(promotion.Mutations)
	var result keyvalue.TransactionResult
	if prepared.BackingCreation {
		if preparations.backingCreation == nil || inputKind != "backing-create" ||
			task.Task.Action != workexecution.ActionCreate ||
			ids.Validate(ids.KindService, task.Task.TargetID) != nil {
			return false, errs.New(errs.KindInternal, "this action cannot use backing creation publication")
		}
		result, err = preparations.backingCreation.Commit(
			ctx,
			prepared.Task,
			inputKind,
			dependencyChange,
			promotion,
			prepared.Authority,
		)
	} else {
		result, err = preparations.queue.Commit(ctx, dependencyChange, promotion, prepared.Authority)
	}
	applied = err == nil && result.Succeeded
	return result.Succeeded, err
}
