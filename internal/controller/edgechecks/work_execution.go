package edgechecks

import (
	"context"
	"time"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (executor *Executor) Execute(ctx context.Context, delivery workqueue.Delivery) error {
	var err error
	if delivery.Attempt.Attempt.Direction == workexecution.Recovery {
		// Recovery only runs the captured helper cleanup, never either probe.
		err = executor.reconcile(ctx, delivery)
	} else {
		err = executor.execute(ctx, delivery)
	}
	return executor.recordFailure(ctx, delivery, err)
}

func (executor *Executor) Reconcile(ctx context.Context, delivery workqueue.Delivery) error {
	return executor.recordFailure(ctx, delivery, executor.reconcile(ctx, delivery))
}

func (executor *Executor) recordFailure(ctx context.Context, delivery workqueue.Delivery, cause error) error {
	if cause == nil || ctx == nil {
		return cause
	}
	settlement, cancel := settlementContext(ctx)
	defer cancel()
	loaded, err := executor.loadDelivery(settlement, delivery)
	if err != nil {
		return errs.WrapJoined(errs.KindStateConflict, cause, err)
	}
	if loaded.Record.Phase == checkrecord.Finished || loaded.Record.Phase == checkrecord.CleanupRequired {
		return cause
	}
	attempt := delivery.Attempt.Attempt
	journal, err := checkrecord.NewJournal(executor.repository, loaded.Record.Owner, attempt.ID, attempt.Executor)
	if err != nil {
		return errs.WrapJoined(errs.KindStateConflict, cause, err)
	}
	if err := journal.Stop(settlement, contract.MachineFailure(cause), time.Now().UTC()); err != nil {
		return errs.WrapJoined(errs.KindStateConflict, cause, err)
	}
	return cause
}
