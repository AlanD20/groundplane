package blueprintcoordinator

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core/blueprintreconcile"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const supersededAbortReason = "blueprint_superseded"

// Run resumes durable parent claims before claiming new work. Every effectful
// decision is recalculated from a fresh ledger snapshot and committed through
// repository operations that fence the desired head, parent claim, and epoch.
func (runner *Runner) Run(ctx context.Context) {
	if ctx == nil {
		return
	}
	ticker := time.NewTicker(runner.interval)
	defer ticker.Stop()
	for {
		progressed, err := runner.runOne(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			runner.logger.Error(
				"Blueprint coordinator pass failed",
				slog.String("code", errorCode(err)),
				slog.Any("error", err),
			)
		}
		if progressed && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-runner.wake:
		case <-ticker.C:
		}
	}
}

func (runner *Runner) runOne(ctx context.Context) (bool, error) {
	claims, err := runner.tasks.ListBlueprintParentClaims(ctx)
	if err != nil {
		return false, err
	}
	var firstErr error
	progressed := false
	for _, claim := range claims {
		parentProgressed, reconcileErr := runner.reconcileParent(ctx, claim.Record)
		if reconcileErr != nil && firstErr == nil {
			firstErr = reconcileErr
		}
		progressed = progressed || parentProgressed
	}
	_, found, claimErr := runner.tasks.ClaimNextBlueprintParent(ctx, runner.now().UTC())
	if claimErr != nil && firstErr == nil {
		firstErr = claimErr
	}
	return progressed || found, firstErr
}

func (runner *Runner) reconcileParent(ctx context.Context, parent etcd.TaskRecord) (bool, error) {
	if !validParent(parent) {
		return false, errs.New(errs.KindInternal, "blueprint coordinator received an invalid parent claim")
	}
	snapshot, err := runner.ledger.Load(ctx, parent.Owner.EnvironmentID)
	if err != nil {
		return false, err
	}
	abortRequested, err := runner.tasks.BlueprintParentAbortRequested(ctx, parent.ID)
	if err != nil {
		return false, err
	}
	if abortRequested {
		return runner.reconcileAbortingParent(ctx, parent, snapshot)
	}
	if snapshot.HeadTaskID != parent.ID {
		_, err := runner.tasks.RetireSupersededBlueprintParent(
			ctx,
			parent.Owner.EnvironmentID,
			parent.ID,
			runner.now().UTC(),
		)
		if isWaiting(err) {
			return false, nil
		}
		return err == nil, err
	}

	desired, err := runner.planner.Plan(ctx, parent, snapshot)
	if err != nil {
		return false, err
	}
	published, err := runner.tasks.PublishBlueprintDesiredPlan(ctx, parent.ID, desired)
	if err != nil {
		if isStateRace(err) {
			return false, nil
		}
		return false, err
	}
	planChanged := snapshot.Desired == nil || published.Revision != snapshot.Desired.Revision
	snapshot, err = runner.ledger.Load(ctx, parent.Owner.EnvironmentID)
	if err != nil {
		return planChanged, err
	}
	if snapshot.HeadTaskID != parent.ID {
		return planChanged, nil
	}
	// A pending child of an older parent can never pass the assignment head
	// fence, even when its immutable unit still matches the new desired plan.
	// Withdraw it before selection so the current parent can publish its own
	// recoverable child identity for that unit.
	for _, execution := range snapshot.Executions {
		if execution.Record.State != blueprintunits.Pending || execution.Record.ParentTaskID == parent.ID {
			continue
		}
		_, err := runner.tasks.AbortPendingTask(ctx, execution.Record.TaskID, runner.now().UTC())
		if isStateRace(err) {
			return true, nil
		}
		return err == nil, err
	}
	selection, err := blueprintunits.Select(snapshot)
	if err != nil {
		return planChanged, err
	}

	if len(selection.CancelPending) != 0 {
		execution, found := executionByPlanID(snapshot, selection.CancelPending[0])
		if !found || execution.State != blueprintunits.Pending {
			return planChanged, errs.New(errs.KindInternal, "pending Blueprint cancellation lost its execution")
		}
		_, err := runner.tasks.AbortPendingTask(ctx, execution.TaskID, runner.now().UTC())
		if isStateRace(err) {
			return true, nil
		}
		return err == nil, err
	}
	if len(selection.CancelRunning) != 0 {
		execution, found := executionByPlanID(snapshot, selection.CancelRunning[0])
		if !found || execution.State != blueprintunits.Running {
			return planChanged, errs.New(errs.KindInternal, "running Blueprint cancellation lost its execution")
		}
		if err := runner.abortRunningChild(ctx, execution.TaskID); err != nil {
			if isStateRace(err) {
				return true, nil
			}
			if isWaiting(err) {
				return planChanged, nil
			}
			return planChanged, err
		}
		return true, nil
	}
	if len(selection.Ready) != 0 {
		unit, found := desiredUnit(snapshot, selection.Ready[0])
		if !found {
			return planChanged, errs.New(errs.KindInternal, "ready Blueprint unit is absent from its desired plan")
		}
		prepared, err := runner.children.Prepare(ctx, parent, unit)
		if err != nil {
			return planChanged, err
		}
		defer prepared.Clear()
		if _, err := runner.tasks.PublishBlueprintChild(
			ctx,
			parent.ID,
			prepared.Task,
			unit,
			prepared.ReleasePublication,
		); err != nil {
			if !unknownBlueprintChildPublication(err) {
				if cleanupErr := prepared.ReleasePublication.Abandon(ctx); cleanupErr != nil {
					return planChanged, errors.Join(err, cleanupErr)
				}
			}
			if isStateRace(err) {
				return true, nil
			}
			return planChanged, err
		}
		runner.agents.WakeTaskDispatch()
		return true, nil
	}
	if snapshot.Desired != nil && snapshot.Desired.Record.Complete &&
		len(selection.Satisfied) == len(snapshot.Desired.Record.Units) && selectionSettled(selection) {
		_, err := runner.tasks.CompleteBlueprintParent(
			ctx,
			parent.Owner.EnvironmentID,
			parent.ID,
			runner.now().UTC(),
		)
		if isStateRace(err) {
			return true, nil
		}
		return err == nil, err
	}
	if len(selection.ContinuePending) != 0 {
		runner.agents.WakeTaskDispatch()
	}
	return planChanged, nil
}

// AbortTask durably stops further forward selection, wakes reconciliation, and
// waits until the parent terminal transaction proves every owned child claim
// settled. Cancellation of this wait does not revoke the persisted request.
func (runner *Runner) AbortTask(ctx context.Context, taskID string) error {
	if ctx == nil || ids.Validate(ids.KindTask, taskID) != nil {
		return errs.New(errs.KindValidationFailed, "blueprint parent abort target is invalid")
	}
	if err := runner.tasks.RequestBlueprintParentAbort(ctx, taskID, runner.now().UTC()); err != nil {
		return err
	}
	runner.Wake()
	ticker := time.NewTicker(runner.interval)
	defer ticker.Stop()
	for {
		current, err := runner.tasks.GetTask(ctx, taskID)
		if err != nil {
			return err
		}
		switch current.Record.Status {
		case taskjournal.TaskStatusAborted:
			return nil
		case taskjournal.TaskStatusCompleted, taskjournal.TaskStatusFailed, taskjournal.TaskStatusTimedOut:
			return errs.New(errs.KindStateConflict, "blueprint parent became terminal before abort completed")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (runner *Runner) reconcileAbortingParent(
	ctx context.Context,
	parent etcd.TaskRecord,
	snapshot blueprintunits.Snapshot,
) (bool, error) {
	for _, execution := range snapshot.Executions {
		if execution.Record.ParentTaskID != parent.ID || execution.Record.State != blueprintunits.Pending {
			continue
		}
		_, err := runner.tasks.AbortPendingTask(ctx, execution.Record.TaskID, runner.now().UTC())
		if isStateRace(err) {
			return true, nil
		}
		return err == nil, err
	}
	for _, execution := range snapshot.Executions {
		if execution.Record.ParentTaskID != parent.ID || execution.Record.State != blueprintunits.Running {
			continue
		}
		if err := runner.abortRunningChild(ctx, execution.Record.TaskID); err != nil {
			if isStateRace(err) {
				return true, nil
			}
			if isWaiting(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	for _, execution := range snapshot.Executions {
		if execution.Record.ParentTaskID == parent.ID {
			return false, nil
		}
	}
	_, err := runner.tasks.AbortBlueprintParent(
		ctx,
		parent.Owner.EnvironmentID,
		parent.ID,
		runner.now().UTC(),
	)
	if isWaiting(err) {
		return false, nil
	}
	return err == nil, err
}

func (runner *Runner) abortRunningChild(ctx context.Context, taskID string) error {
	assignment, err := runner.tasks.GetTaskAssignment(ctx, taskID)
	if err != nil {
		return err
	}
	record := assignment.Assignment.Record
	if record.Executor != taskjournal.TaskExecutorAgent || record.TaskID != taskID {
		return errs.New(errs.KindInternal, "blueprint child assignment executor is invalid")
	}
	subscriptionContext, cancel := context.WithCancel(ctx)
	defer cancel()
	terminal, err := runner.agents.TaskTerminal(
		subscriptionContext,
		record.AgentID,
		record.AgentGeneration,
		record.TaskID,
		record.AssignmentID,
	)
	if err != nil {
		return err
	}
	if terminal == nil {
		return errs.New(errs.KindInternal, "blueprint child terminal subscription is nil")
	}
	current, err := runner.tasks.GetTaskAssignment(ctx, taskID)
	if err != nil {
		return err
	}
	if current.Assignment.Revision != assignment.Assignment.Revision {
		return errs.New(errs.KindStateConflict, "blueprint child assignment changed before abort delivery")
	}
	if err := runner.agents.AbortTask(
		ctx,
		record.AgentID,
		record.AgentGeneration,
		record.TaskID,
		record.AssignmentID,
		supersededAbortReason,
	); err != nil {
		return err
	}
	select {
	case terminalErr, open := <-terminal:
		if !open {
			return errs.New(errs.KindInternal, "blueprint child terminal subscription closed without a result")
		}
		return terminalErr
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
		return errs.New(errs.KindResourceInUse, "Blueprint child abort is awaiting an exact terminal receipt")
	}
}

func executionByPlanID(snapshot blueprintunits.Snapshot, planID string) (blueprintunits.ExecutionRecord, bool) {
	for _, execution := range snapshot.Executions {
		if execution.Record.PlanID == planID {
			return execution.Record, true
		}
	}
	return blueprintunits.ExecutionRecord{}, false
}

func desiredUnit(
	snapshot blueprintunits.Snapshot,
	target blueprintreconcile.ResourceKey,
) (blueprintunits.Unit, bool) {
	if snapshot.Desired == nil {
		return blueprintunits.Unit{}, false
	}
	for _, unit := range snapshot.Desired.Record.Units {
		if unit.Target.Kind == target.Kind && unit.Target.ID == target.ID {
			return unit, true
		}
	}
	return blueprintunits.Unit{}, false
}

func selectionSettled(selection blueprintreconcile.Selection) bool {
	return len(selection.Ready) == 0 && len(selection.Waiting) == 0 && len(selection.ResolveEffects) == 0 &&
		len(selection.CancelPending) == 0 && len(selection.CancelRunning) == 0 &&
		len(selection.ContinuePending) == 0 && len(selection.ContinueRunning) == 0
}

func isStateRace(err error) bool {
	if err == nil {
		return false
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStateConflict
}

func unknownBlueprintChildPublication(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}

func isWaiting(err error) bool {
	if err == nil {
		return false
	}
	kind, ok := errs.KindOf(err)
	return ok && (kind == errs.KindStateConflict || kind == errs.KindResourceInUse)
}

func errorCode(err error) string {
	kind, ok := errs.KindOf(err)
	if ok {
		return string(kind)
	}
	if errors.Is(err, context.Canceled) {
		return "context.canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "context.deadline_exceeded"
	}
	return string(errs.KindInternal)
}
