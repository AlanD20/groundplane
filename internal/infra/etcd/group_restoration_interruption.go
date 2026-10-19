package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReconcileInterruptedGroupRestoration preserves a fully recorded restoration,
// otherwise records interruption without assuming helper termination or releasing
// unknown-effect claims. A new Agent generation alone is not restoration proof.
func (ledger *ReleaseLedger) ReconcileInterruptedGroupRestoration(ctx context.Context, view workqueue.ResumeView,
	input releaseexecution.RecoveryInput, authorization agentregistration.WorkAuthorization, at time.Time,
) error {
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	identity, original := authorization.Identity(), input.Input.Attempt.Executor
	if ledger == nil || ledger.store == nil || input.Group == nil || input.Validate() != nil || view.CurrentExecutor ||
		identity.Executor != workexecution.Agent || identity.HostID != original.HostID || identity.Generation <= original.Generation {
		return errs.New(
			errs.KindStateConflict,
			"Group restoration interruption requires its replacement Agent on the same host",
		)
	}
	queue, err := workqueue.NewRepository(ledger.store)
	if err != nil {
		return err
	}
	current, err := queue.ReadExecutor(ctx, view.Work.Item.Item.Placement)
	if err != nil {
		return err
	}
	if current.Revision != view.ExecutorRevision || current.Record.Identity != identity {
		return errs.New(errs.KindStateConflict, "replacement Agent changed before Group restoration reconciliation")
	}
	guards = append(guards, keyvalue.Condition{Key: workqueue.ExecutorKey(identity), ModRevision: current.Revision})
	saved, err := releasework.LoadRecovery(ctx, ledger.store, input, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if saved.Record != nil && saved.Record.Report.Progress.Phase == releaseexecution.RecoveryComplete {
		return ledger.settleProvenGroupRestoration(ctx, view, input, guards, at)
	}
	request, err := queue.ReadInterruptedGroupRestorationAt(ctx, view)
	if err != nil {
		return err
	}
	failure, result, err := releasework.PrepareInterruptedRecoveryFailure(ctx, ledger.store, view, input, at)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(failure.Mutations)
	request.At, request.Result = at, result
	execution, err := workqueue.PrepareCompletion(request)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(execution.Mutations)
	committed, err := queue.Commit(ctx, workqueue.Change{Conditions: guards}, failure, execution)
	keyvalue.ClearValues(committed.FailureReads)
	if err == nil && !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "Group restoration changed before interruption was recorded")
	}
	return err
}
