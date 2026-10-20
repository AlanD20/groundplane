package blueprintcoordinator

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Completion uses resource acknowledgements, never child Task terminal status.
// Work owners settle resources before their items. The shared completion
// coordinator commits this resource fence with the visible Task and replay.
type RuntimePreparer interface {
	PrepareRuntimeProjection(context.Context, workexecution.Task, blueprintunits.Snapshot) (bool, error)
}

type Completion struct {
	ledger  *blueprintunits.Repository
	runtime RuntimePreparer
}

func NewCompletion(ledger *blueprintunits.Repository, runtime RuntimePreparer) (*Completion, error) {
	if ledger == nil || runtime == nil {
		return nil, errs.New(errs.KindInternal, "blueprint completion requires resource acknowledgements")
	}
	return &Completion{ledger: ledger, runtime: runtime}, nil
}

func (completion *Completion) PrepareFinish(
	ctx context.Context,
	finished workqueue.FinishedWork,
	at time.Time,
) (workqueue.Change, error) {
	task := finished.Task.Task
	if task.Action != workexecution.ActionApply || task.Scope.EnvironmentID == "" ||
		task.TargetID != task.Scope.EnvironmentID ||
		!task.PublicationComplete ||
		task.Progress.Unsettled() != 0 ||
		len(finished.Items) != int(task.ExpectedItems) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "blueprint apply has unfinished or mismatched work")
	}
	// CompletionOutcome is the sole outcome policy. Cleanup does not erase an original
	// failure, and cancellation/supersession cannot become successful Apply.
	outcome, err := workexecution.CompletionOutcome(task)
	if err != nil {
		return workqueue.Change{}, err
	}
	if outcome != workexecution.Succeeded {
		// All items have settled their own effects. Do not select or modify a newer
		// desired head while finishing an unsuccessful captured Apply.
		return workqueue.Change{}, nil
	}
	snapshot, err := completion.ledger.LoadAtRevision(ctx, task.TargetID, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if snapshot.HeadTaskID != task.ID || snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != task.ID ||
		!snapshot.Desired.Record.Complete {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"blueprint apply no longer owns the complete desired plan",
		)
	}
	if len(snapshot.Executions) != 0 {
		return workqueue.Change{}, errs.New(errs.KindResourceInUse, "blueprint resources still have unsettled effects")
	}
	for _, applied := range snapshot.Applied {
		if applied.Record.State == blueprintunits.Uncertain || applied.Record.State == blueprintunits.Diverged {
			return workqueue.Change{}, errs.New(
				errs.KindResourceInUse,
				"blueprint resource outcome requires reconciliation",
			)
		}
	}
	selection, err := blueprintunits.Select(snapshot)
	if err != nil {
		return workqueue.Change{}, err
	}
	if len(selection.Satisfied) != len(snapshot.Desired.Record.Units) ||
		len(selection.Ready)+len(selection.Waiting)+len(selection.ResolveEffects)+len(selection.CancelPending)+
			len(selection.CancelRunning)+len(selection.ContinuePending)+len(selection.ContinueRunning) != 0 {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"blueprint desired resources are not fully acknowledged",
		)
	}
	// Resource-ledger writes advance its epoch, fencing the full resource read
	// without adding one transaction comparison per resource in a large Apply.
	ready, err := completion.runtime.PrepareRuntimeProjection(ctx, task, snapshot)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !ready {
		return workqueue.Change{}, errs.New(errs.KindResourceInUse, "Blueprint runtime configuration is not sealed yet")
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: blueprints.EnvironmentBlueprintHeadKey(task.TargetID), ModRevision: snapshot.HeadRevision},
		{Key: blueprintunits.EpochKey(task.TargetID), ModRevision: snapshot.EpochRevision},
		{Key: blueprintunits.DesiredPlanKey(task.TargetID), ModRevision: snapshot.Desired.Revision},
	}}, nil
}
