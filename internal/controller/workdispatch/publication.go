package workdispatch

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublicationOwner reads the accepted immutable plan and prepares only the
// operation inputs needed by this batch. Its authority comparisons and writes
// commit with the queue batch. It cannot execute effects or select newer intent.
type PublicationOwner interface {
	PreparePublication(context.Context, workqueue.TaskSnapshot, int64, time.Time) (PublicationBatch, error)
}

type PublicationBatch struct {
	Planning  bool
	Rejected  bool
	Graph     []workexecution.Item
	Authority workqueue.Change
}

type PublicationRegistration struct {
	Action workexecution.Action
	Owner  PublicationOwner
}

type Publications struct {
	queue  *workqueue.Repository
	owners map[workexecution.Action]PublicationOwner
}

func NewPublications(queue *workqueue.Repository, registrations []PublicationRegistration) (*Publications, error) {
	if queue == nil || len(registrations) == 0 {
		return nil, errs.New(errs.KindInternal, "task publication requires storage and captured-plan owners")
	}
	owners := make(map[workexecution.Action]PublicationOwner, len(registrations))
	for _, registration := range registrations {
		if !workexecution.ValidAction(registration.Action) || registration.Owner == nil || owners[registration.Action] != nil {
			return nil, errs.New(errs.KindInternal, "task publication action is invalid or duplicated")
		}
		owners[registration.Action] = registration.Owner
	}
	return &Publications{queue: queue, owners: owners}, nil
}

// Advance performs one bounded batch, not the entire graph. On an unknown
// commit outcome the next sweep reads persisted membership before preparing
// another batch; it never replays an in-memory transaction blindly.
func (publications *Publications) Advance(ctx context.Context, taskID string, at time.Time) error {
	if at.IsZero() {
		return errs.New(errs.KindInternal, "task publication requires a timestamp")
	}
	task, revision, err := publications.queue.ReadTask(ctx, taskID, 0)
	if err != nil {
		return err
	}
	if task.Task.PublicationComplete {
		return nil
	}
	owner := publications.owners[task.Task.Action]
	if owner == nil {
		return errs.New(errs.KindStateConflict, "task action has no captured-plan publication owner")
	}
	batch, err := owner.PreparePublication(ctx, task, revision, at)
	defer keyvalue.ClearMutationValues(batch.Authority.Mutations)
	if err != nil {
		return err
	}
	if batch.Planning {
		if batch.Rejected || len(batch.Graph) != 0 || len(batch.Authority.Conditions) != 0 || len(batch.Authority.Mutations) != 0 {
			return errs.New(errs.KindInternal, "planning publication cannot also publish execution work")
		}
		return nil
	}
	if len(batch.Authority.Conditions) == 0 {
		return errs.New(errs.KindInternal, "task publication requires captured-plan authority comparisons")
	}
	var queueChange workqueue.Change
	if batch.Rejected {
		queueChange, err = workqueue.PrepareRejectedPublicationComplete(task, batch.Graph, at)
	} else if task.Task.ExpectedItems == 0 && task.Task.Action == workexecution.ActionRemove {
		if len(batch.Graph) == 0 || len(batch.Graph) > int(workexecution.MaximumTaskItems) {
			return errs.New(errs.KindStateConflict, "removal plan membership is invalid")
		}
		captured := task.Task
		captured.ExpectedItems = uint32(len(batch.Graph))
		if err := workexecution.ValidateGraph(captured, batch.Graph); err != nil {
			return err
		}
		queueChange, err = workqueue.PrepareCapturedMembership(task, captured.ExpectedItems, at)
	} else {
		queueChange, _, err = publications.queue.PrepareGraphBatch(ctx, task, batch.Graph, at)
	}
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(queueChange.Mutations)
	result, err := publications.queue.Commit(ctx, batch.Authority, queueChange)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "task or captured publication inputs changed before the batch was stored")
	}
	return nil
}

// Run gives each indexed Task one batch per sweep. An oversized or damaged
// captured plan cannot monopolize publication of unrelated accepted actions.
func (publications *Publications) Run(ctx context.Context, interval time.Duration, report func(Issue)) error {
	if ctx == nil || interval < 100*time.Millisecond || report == nil {
		return errs.New(errs.KindInternal, "publication loop configuration is invalid")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	after := ""
	for {
		page, err := publications.queue.Publishing(ctx, after)
		if err != nil {
			return err
		}
		for _, task := range page.Tasks {
			bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
			err := publications.Advance(bounded, task.Task.ID, time.Now().UTC())
			cancel()
			if err != nil {
				report(Issue{TaskID: task.Task.ID, Err: err})
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		after = page.Next
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
