package dnsresolver

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (executor *ConnectionCheckExecutor) AuthorizeStart(
	ctx context.Context,
	delivery workqueue.Delivery,
) (workqueue.Change, error) {
	loaded, err := executor.loadDelivery(ctx, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	if loaded.Record.Phase != dnschecks.Ready {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"DNS check has already started; reconcile its result",
		)
	}
	return workqueue.Change{
		Conditions: []keyvalue.Condition{
			{Key: dnschecks.RecordKey(loaded.Record.Owner.TaskID), ModRevision: loaded.Revision},
		},
	}, nil
}

func (executor *ConnectionCheckExecutor) Reconcile(ctx context.Context, delivery workqueue.Delivery) error {
	loaded, err := executor.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt == nil || delivery.Item.Item.Phase != workexecution.Running {
		return errs.New(errs.KindStateConflict, "DNS reconciliation requires a started attempt")
	}
	if loaded.Record.Phase == dnschecks.Finished {
		return nil
	}
	journal, err := dnschecks.NewJournal(executor.records, loaded.Record.Owner, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	// Read-only queries have no retained external effects. An interrupted check
	// remains inconclusive rather than querying a newly edited resolver again.
	return journal.Stop(ctx, "interrupted", time.Now().UTC())
}

func (executor *ConnectionCheckExecutor) RejectUnstarted(ctx context.Context, delivery workqueue.Delivery) error {
	loaded, err := executor.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt != nil || delivery.Item.Item.Phase != workexecution.Assigned {
		return errs.New(errs.KindStateConflict, "DNS check already started; reconcile its result")
	}
	if loaded.Record.Phase == dnschecks.Finished {
		return nil
	}
	journal, err := dnschecks.NewJournal(executor.records, loaded.Record.Owner, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	failure := "canceled"
	if !time.Now().UTC().Before(attempt.Deadline) {
		failure = "timeout"
	}
	return journal.RejectAssigned(ctx, failure, time.Now().UTC())
}

func (executor *ConnectionCheckExecutor) loadDelivery(
	ctx context.Context,
	delivery workqueue.Delivery,
) (keyvalue.Versioned[dnschecks.Record], error) {
	item, attempt := delivery.Item.Item, delivery.Attempt.Attempt
	if item.Validate() != nil || attempt.Validate() != nil || item.Operation != workexecution.DNSCheck ||
		attempt.Executor.Executor != workexecution.Controller || attempt.Direction != workexecution.Forward ||
		attempt.TaskID != item.TaskID || attempt.WorkItemID != item.ID || item.ActiveAttemptID != attempt.ID ||
		item.Input == nil || *item.Input != attempt.Input || attempt.FinishedAt != nil {
		return keyvalue.Versioned[dnschecks.Record]{}, errs.New(
			errs.KindStateConflict,
			"DNS delivery does not own its captured attempt",
		)
	}
	loaded, err := executor.records.Load(ctx, item.TaskID)
	if err != nil {
		return keyvalue.Versioned[dnschecks.Record]{}, err
	}
	digest, err := dnschecks.PlanHash(loaded.Record)
	if err != nil || loaded.Record.Owner.TaskID != item.TaskID || loaded.Record.Owner.WorkItemID != item.ID ||
		loaded.Record.Owner.PlanID != attempt.Input.ID || digest != attempt.Input.SHA256 || loaded.Record.Source.ComponentID != item.TargetID {
		return keyvalue.Versioned[dnschecks.Record]{}, errs.New(
			errs.KindStateConflict,
			"DNS delivery differs from captured resolver input",
		)
	}
	return loaded, nil
}
