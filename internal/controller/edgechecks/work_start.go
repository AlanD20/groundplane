package edgechecks

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (executor *Executor) loadDelivery(
	ctx context.Context,
	delivery workqueue.Delivery,
) (keyvalue.Versioned[checkrecord.Record], error) {
	item, attempt := delivery.Item.Item, delivery.Attempt.Attempt
	if item.Validate() != nil || attempt.Validate() != nil || item.Operation != workexecution.EdgeCheck ||
		attempt.Executor.Executor != workexecution.Controller || attempt.TaskID != item.TaskID ||
		attempt.WorkItemID != item.ID || item.ActiveAttemptID != attempt.ID || attempt.FinishedAt != nil ||
		item.Input == nil || *item.Input != attempt.Input {
		return keyvalue.Versioned[checkrecord.Record]{}, errs.New(
			errs.KindStateConflict,
			"edge check does not own this execution attempt",
		)
	}
	loaded, err := executor.repository.Load(ctx, item.TaskID)
	if err != nil {
		return keyvalue.Versioned[checkrecord.Record]{}, err
	}
	record := loaded.Record
	digest, err := checkrecord.PlanHash(record)
	if err != nil || record.Owner.TaskID != item.TaskID || record.Owner.WorkItemID != item.ID ||
		record.Owner.PlanID != attempt.Input.ID || digest != attempt.Input.SHA256 || record.Source.Request.TargetID() != item.TargetID {
		return keyvalue.Versioned[checkrecord.Record]{}, errs.New(
			errs.KindStateConflict,
			"edge check differs from its captured input",
		)
	}
	return loaded, nil
}

func (executor *Executor) AuthorizeStart(ctx context.Context, delivery workqueue.Delivery) (workqueue.Change, error) {
	loaded, err := executor.loadDelivery(ctx, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	record, attempt := loaded.Record, delivery.Attempt.Attempt
	if attempt.Direction == workexecution.Forward && record.Phase != checkrecord.Ready ||
		attempt.Direction == workexecution.Recovery &&
			(record.Phase != checkrecord.CleanupRequired || !record.CleanupPending) {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"edge check is not ready for this execution direction",
		)
	}
	return workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: checkrecord.Key(record.Owner.TaskID), ModRevision: loaded.Revision}},
	}, nil
}

func (executor *Executor) RejectUnstarted(ctx context.Context, delivery workqueue.Delivery) error {
	loaded, err := executor.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt != nil || delivery.Item.Item.Phase != workexecution.Assigned {
		return errs.New(errs.KindStateConflict, "edge check has started; reconcile its effects")
	}
	if loaded.Record.Phase == checkrecord.Finished ||
		attempt.Direction == workexecution.Recovery && loaded.Record.Phase == checkrecord.CleanupRequired {
		return nil
	}
	journal, err := checkrecord.NewJournal(executor.repository, loaded.Record.Owner, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	failure := "canceled"
	if !time.Now().UTC().Before(attempt.Deadline) {
		failure = "timeout"
	}
	return journal.RejectAssigned(ctx, failure, time.Now().UTC())
}

func (executor *Executor) reconcile(ctx context.Context, delivery workqueue.Delivery) error {
	loaded, err := executor.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	attempt, record := delivery.Attempt.Attempt, loaded.Record
	if attempt.StartedAt == nil || delivery.Item.Item.Phase != workexecution.Running {
		return errs.New(errs.KindStateConflict, "edge reconciliation requires a started attempt")
	}
	if record.Phase == checkrecord.Finished ||
		record.Phase == checkrecord.CleanupRequired && attempt.Direction == workexecution.Forward {
		return nil
	}
	journal, err := checkrecord.NewJournal(executor.repository, record.Owner, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	if record.CleanupPending {
		return executor.cleanup(ctx, record, journal)
	}
	// Do not begin a public probe after reconnect, even when the private probe
	// finished. Its saved outcome may be settled, otherwise this is interrupted.
	if record.Public != nil || record.PublicFailure != "" {
		return executor.finish(ctx, record, journal)
	}
	return journal.Stop(ctx, "interrupted", time.Now().UTC())
}
