package backingchecks

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (executor *Executor) AuthorizeStart(ctx context.Context, delivery workqueue.Delivery) (workqueue.Change, error) {
	loaded, err := executor.loadDelivery(ctx, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer loaded.Record.Clear()
	phase, direction := loaded.Record.Phase, delivery.Attempt.Attempt.Direction
	if direction == workexecution.Forward && phase != checkrecord.Ready ||
		direction == workexecution.Recovery && phase != checkrecord.CleanupRequired {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Backing check start no longer matches its captured operation")
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: checkrecord.Key(loaded.Record.Owner.TaskID), ModRevision: loaded.Revision}}}, nil
}

func (executor *Executor) RejectUnstarted(ctx context.Context, delivery workqueue.Delivery) error {
	loaded, err := executor.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	defer loaded.Record.Clear()
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt != nil || delivery.Item.Item.Phase != workexecution.Assigned {
		return errs.New(errs.KindStateConflict, "Backing check requires effect reconciliation")
	}
	if attempt.Direction == workexecution.Recovery {
		if loaded.Record.Phase != checkrecord.CleanupRequired {
			return errs.New(errs.KindStateConflict, "Backing cleanup differs from its unstarted assignment")
		}
		return nil
	}
	if loaded.Record.Phase == checkrecord.Finished {
		return nil
	}
	journal, err := checkrecord.NewJournal(executor.repository, loaded.Record.Owner, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	failure := checkrecord.InterruptedFailure
	if !time.Now().UTC().Before(attempt.Deadline) {
		failure = string(backingaccess.TimeoutFailure)
	}
	return journal.RejectAssigned(ctx, failure, time.Now().UTC())
}

// Reconcile cannot issue another access probe. The saved journal says whether
// a result can be acknowledged, an unstarted probe rejected or the exact helper
// cleaned up. Cleanup uses no credentials and cannot select a different host.
func (executor *Executor) Reconcile(ctx context.Context, delivery workqueue.Delivery) error {
	return executor.recordFailure(ctx, delivery, executor.reconcile(ctx, delivery))
}

func (executor *Executor) reconcile(ctx context.Context, delivery workqueue.Delivery) error {
	loaded, err := executor.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	defer loaded.Record.Clear()
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt == nil || delivery.Item.Item.Phase != workexecution.Running {
		return errs.New(errs.KindStateConflict, "Backing check reconciliation requires its started attempt")
	}
	if loaded.Record.Phase == checkrecord.Finished ||
		loaded.Record.Phase == checkrecord.CleanupRequired && attempt.Direction == workexecution.Forward {
		return nil
	}
	journal, err := checkrecord.NewJournal(executor.repository, loaded.Record.Owner, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	if loaded.Record.Phase == checkrecord.Ready {
		return journal.FinishUnstarted(ctx, checkrecord.InterruptedFailure, time.Now().UTC())
	}
	return executor.resume(ctx, loaded.Record, journal, attempt.Direction == workexecution.Recovery)
}

func (executor *Executor) loadDelivery(ctx context.Context, delivery workqueue.Delivery) (keyvalue.Versioned[checkrecord.Record], error) {
	item, attempt := delivery.Item.Item, delivery.Attempt.Attempt
	if item.Validate() != nil || attempt.Validate() != nil || item.Operation != workexecution.BackingCheck ||
		attempt.Executor.Executor != workexecution.Controller || attempt.TaskID != item.TaskID || attempt.WorkItemID != item.ID ||
		item.ActiveAttemptID != attempt.ID || item.Input == nil || *item.Input != attempt.Input || attempt.FinishedAt != nil {
		return keyvalue.Versioned[checkrecord.Record]{}, errs.New(errs.KindStateConflict, "Backing check delivery does not own its captured attempt")
	}
	loaded, err := executor.repository.Load(ctx, item.TaskID)
	if err != nil {
		return keyvalue.Versioned[checkrecord.Record]{}, err
	}
	digest, err := checkrecord.PlanHash(loaded.Record)
	if err != nil || loaded.Record.Owner.TaskID != item.TaskID || loaded.Record.Owner.WorkItemID != item.ID ||
		loaded.Record.Owner.PlanID != attempt.Input.ID || digest != attempt.Input.SHA256 ||
		loaded.Record.Source.Parameters.AttachID != item.TargetID {
		loaded.Record.Clear()
		return keyvalue.Versioned[checkrecord.Record]{}, errs.New(errs.KindStateConflict, "Backing check delivery differs from captured input")
	}
	return loaded, nil
}
