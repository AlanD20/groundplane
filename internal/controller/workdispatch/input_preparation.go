package workdispatch

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// InputOwner captures dependency outputs and atomically publishes executable
// input. Preparing input never grants permission to execute host effects.
type InputOwner interface {
	Prepare(context.Context, string, string) error
}

type InputRegistration struct {
	Action    workexecution.Action
	Operation workexecution.Operation
	Owner     InputOwner
}

type inputKind struct {
	action    workexecution.Action
	operation workexecution.Operation
}

type Inputs struct {
	queue  *workqueue.Repository
	owners map[inputKind]InputOwner
}

func NewInputs(queue *workqueue.Repository, registrations []InputRegistration) (*Inputs, error) {
	if queue == nil || len(registrations) == 0 {
		return nil, errs.New(errs.KindInternal, "input preparation requires storage and operation owners")
	}
	owners := make(map[inputKind]InputOwner, len(registrations))
	for _, registration := range registrations {
		key := inputKind{registration.Action, registration.Operation}
		if !workexecution.ValidAction(key.action) || !workexecution.ValidOperation(key.operation) ||
			registration.Owner == nil || owners[key] != nil {
			return nil, errs.New(errs.KindInternal, "input preparation owner is invalid or duplicated")
		}
		owners[key] = registration.Owner
	}
	return &Inputs{queue: queue, owners: owners}, nil
}

func (inputs *Inputs) Advance(ctx context.Context, entry workqueue.QueueEntry) error {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return err
	}
	ctx = imagefence.WithScope(ctx)
	view, err := inputs.queue.ReadWork(ctx, entry.TaskID, entry.WorkItemID, 0)
	if err != nil {
		return err
	}
	task, item := view.Task.Task, view.Item.Item
	// The scan can race publication, preparation or Stop. The operation owner
	// re-reads and compares authority before publishing; a scan is not a grant.
	if !task.PublicationComplete || task.FinishedAt != nil ||
		item.ForwardRevoked || item.Phase != workexecution.Waiting || item.Wait == nil ||
		item.Wait.Reason != workexecution.WaitInput || item.Input != nil {
		return nil
	}
	at := time.Now().UTC()
	if task.Stop != nil || !at.Before(task.Deadline) || !at.Before(item.Deadline) {
		change, err := workqueue.PrepareInputSettlement(view, at)
		if err != nil {
			return err
		}
		defer keyvalue.ZeroMutationBytes(change.Mutations)
		result, err := inputs.queue.Commit(ctx, change)
		keyvalue.ClearValues(result.FailureReads)
		// A failed comparison means another owner advanced the work. Never
		// repeat the old settlement against a newly prepared or assigned item.
		return err
	}
	owner := inputs.owners[inputKind{task.Action, item.Operation}]
	if owner == nil {
		return errs.New(errs.KindStateConflict, "waiting work has no registered input preparation owner")
	}
	for _, id := range item.Dependencies {
		dependency, err := inputs.queue.ReadWork(ctx, task.ID, id, view.ReadRevision)
		if err != nil {
			return err
		}
		if dependency.Item.Item.Phase != workexecution.Settled {
			// Expected waiting is not an operation failure and produces no error log.
			return nil
		}
		if dependency.Item.Item.Outcome != workexecution.Succeeded {
			change, err := workqueue.PrepareInputDependencyFailure(view, dependency.Item, at)
			if err != nil {
				return err
			}
			defer keyvalue.ZeroMutationBytes(change.Mutations)
			result, err := inputs.queue.Commit(ctx, change)
			keyvalue.ClearValues(result.FailureReads)
			return err
		}
	}
	deadline := task.Deadline
	if item.Deadline.Before(deadline) {
		deadline = item.Deadline
	}
	bounded, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	prepareErr := owner.Prepare(bounded, task.ID, item.ID)
	if prepareErr == nil && item.Wait.Code == "" {
		return nil
	}
	if err := inputs.recordDiagnostic(ctx, task.ID, item.ID, prepareErr); err != nil {
		return errs.WrapJoined(errs.KindInternal, prepareErr, err)
	}
	return prepareErr
}

func (inputs *Inputs) recordDiagnostic(ctx context.Context, taskID, workID string, preparation error) error {
	view, err := inputs.queue.ReadWork(ctx, taskID, workID, 0)
	if err != nil {
		return err
	}
	code, detail := "", ""
	if preparation != nil {
		public := errs.Wrap(errs.KindInternal, preparation).ToProblem()
		var domain *errs.Error
		if errors.As(preparation, &domain) {
			public = domain.ToProblem()
		}
		code, detail = string(public.Code), strings.TrimSpace(public.Detail)
		if len(detail) > 512 {
			detail = detail[:512]
			for !utf8.ValidString(detail) {
				detail = detail[:len(detail)-1]
			}
			detail = strings.TrimSpace(detail)
		}
	}
	change, err := workqueue.PrepareInputDiagnostic(view, code, detail, time.Now().UTC())
	if err != nil || len(change.Mutations) == 0 {
		return err
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	result, err := inputs.queue.Commit(ctx, change)
	keyvalue.ClearValues(result.FailureReads)
	return err
}

func (inputs *Inputs) Run(ctx context.Context, interval time.Duration, report func(Issue)) error {
	if ctx == nil || interval < 100*time.Millisecond || report == nil {
		return errs.New(errs.KindInternal, "input preparation loop configuration is invalid")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	after := ""
	for {
		page, err := inputs.queue.ScanInput(ctx, after)
		if err != nil {
			return err
		}
		for _, entry := range page.Entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := inputs.Advance(ctx, entry); err != nil {
				report(Issue{TaskID: entry.TaskID, WorkID: entry.WorkItemID, Err: err})
			}
		}
		after = ""
		if page.More {
			after = page.LastID
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
