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

// SettleInterruptedBlueprintRecovery preserves complete restoration, or records
// interruption while keeping resource claims for explicit cleanup Retry.
func (ledger *ReleaseLedger) SettleInterruptedBlueprintRecovery(ctx context.Context, view workqueue.ResumeView,
	input releaseexecution.RecoveryInput, authorization agentregistration.WorkAuthorization, at time.Time,
) error {
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	identity, previous := authorization.Identity(), input.Input.Attempt.Executor
	if ledger == nil || ledger.store == nil || view.CurrentExecutor || identity.Executor != workexecution.Agent ||
		identity.HostID != previous.HostID || identity.Generation <= previous.Generation {
		return errs.New(errs.KindStateConflict, "Recovery reconciliation requires a replacement Agent on the same host")
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
		return errs.New(errs.KindStateConflict, "replacement Agent changed before recovery reconciliation")
	}
	guards = append(guards, keyvalue.Condition{Key: workqueue.ExecutorKey(identity), ModRevision: current.Revision})
	progress, err := releasework.LoadRecovery(ctx, ledger.store, input, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if progress.Record == nil && view.Checkpoint.Record != nil {
		return errs.New(errs.KindStateConflict, "interrupted recovery checkpoint has no captured progress")
	}
	if progress.Record != nil && progress.Record.Report.Progress.Phase == releaseexecution.RecoveryComplete {
		return ledger.settleProvenBlueprintRecovery(ctx, view, input, guards, at)
	}
	completion, err := queue.ReadInterruptedRecoveryAt(ctx, view)
	if err != nil {
		return err
	}
	failure, result, err := releasework.PrepareInterruptedRecoveryFailure(ctx, ledger.store, view, input, at)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(failure.Mutations)
	completion.At, completion.Result = at, result
	execution, err := workqueue.PrepareCompletion(completion)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(execution.Mutations)
	committed, err := queue.Commit(ctx, workqueue.Change{Conditions: guards}, failure, execution)
	keyvalue.ClearValues(committed.FailureReads)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "recovery changed before its interruption was recorded")
	}
	return nil
}
