package connectors

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/s3connector"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (executor *CheckExecutor) AuthorizeStart(ctx context.Context, delivery workqueue.Delivery) (workqueue.Change, error) {
	loaded, err := executor.loadDelivery(ctx, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer loaded.Record.Clear()
	phase, direction := loaded.Record.Phase, delivery.Attempt.Attempt.Direction
	if direction == workexecution.Forward && phase != connectorchecks.Ready ||
		direction == workexecution.Recovery && phase != connectorchecks.CleanupRequired {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "connector check start differs from its captured phase")
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: connectorchecks.Key(loaded.Record.Owner.TaskID), ModRevision: loaded.Revision}}}, nil
}

// RejectUnstarted records the absence of probe effects without inventing a
// start receipt. The journal compares the same unstarted attempt, so a racing
// start cannot lose its credentials or resource protection.
func (executor *CheckExecutor) RejectUnstarted(ctx context.Context, delivery workqueue.Delivery) error {
	loaded, err := executor.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	defer loaded.Record.Clear()
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt != nil || delivery.Item.Item.Phase != workexecution.Assigned {
		return errs.New(errs.KindStateConflict, "connector check requires effect reconciliation, not unstarted rejection")
	}
	if attempt.Direction == workexecution.Recovery {
		// This cleanup attempt never started, but the original probe's effects
		// remain unresolved. Settle releases only its worker slot, not claims,
		// encrypted credentials or cleanup authority.
		if loaded.Record.Phase != connectorchecks.CleanupRequired {
			return errs.New(errs.KindStateConflict, "connector cleanup no longer matches its unstarted assignment")
		}
		return nil
	}
	if loaded.Record.Phase == connectorchecks.Finished {
		return nil
	}
	journal, err := connectorchecks.NewJournal(executor.repository, loaded.Record.Owner, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	failure := s3connector.CheckFailureInterrupted
	if !time.Now().UTC().Before(attempt.Deadline) {
		failure = s3connector.CheckFailureForError(context.DeadlineExceeded)
	}
	return journal.RejectAssigned(ctx, failure, time.Now().UTC())
}

// Reconcile never repeats a probe. A saved start with no journal start has no
// external effects: Start is persisted before the first remote call. Otherwise
// only captured cleanup may proceed, preserving the original failed outcome.
func (executor *CheckExecutor) Reconcile(ctx context.Context, delivery workqueue.Delivery) error {
	loaded, err := executor.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	defer loaded.Record.Clear()
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt == nil || delivery.Item.Item.Phase != workexecution.Running {
		return errs.New(errs.KindStateConflict, "connector check reconciliation requires a started attempt")
	}
	if loaded.Record.Phase == connectorchecks.Finished ||
		loaded.Record.Phase == connectorchecks.CleanupRequired && attempt.Direction == workexecution.Forward {
		return nil
	}
	journal, err := connectorchecks.NewJournal(executor.repository, loaded.Record.Owner, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	if loaded.Record.Phase == connectorchecks.Ready {
		return journal.Stop(ctx, s3connector.CheckFailureInterrupted, time.Now().UTC())
	}
	return executor.resumeCheckCleanup(ctx, loaded.Record, journal, attempt.Direction == workexecution.Recovery)
}

func (executor *CheckExecutor) loadDelivery(ctx context.Context, delivery workqueue.Delivery) (keyvalue.Versioned[connectorchecks.Record], error) {
	item, attempt := delivery.Item.Item, delivery.Attempt.Attempt
	if item.Validate() != nil || attempt.Validate() != nil || item.Operation != workexecution.ConnectorCheck ||
		attempt.Executor.Executor != workexecution.Controller || attempt.TaskID != item.TaskID || attempt.WorkItemID != item.ID ||
		item.ActiveAttemptID != attempt.ID || item.Input == nil || *item.Input != attempt.Input || attempt.FinishedAt != nil {
		return keyvalue.Versioned[connectorchecks.Record]{}, errs.New(errs.KindStateConflict, "connector check delivery is not its active captured attempt")
	}
	loaded, err := executor.repository.Load(ctx, item.TaskID)
	if err != nil {
		return keyvalue.Versioned[connectorchecks.Record]{}, err
	}
	record := loaded.Record
	digest, err := connectorchecks.PlanHash(record)
	if err != nil || record.Owner.TaskID != item.TaskID || record.Owner.WorkItemID != item.ID ||
		record.Owner.PlanID != attempt.Input.ID || digest != attempt.Input.SHA256 || record.Connector.Connector.ID != item.TargetID {
		loaded.Record.Clear()
		return keyvalue.Versioned[connectorchecks.Record]{}, errs.New(errs.KindStateConflict, "connector check delivery differs from captured input")
	}
	return loaded, nil
}
