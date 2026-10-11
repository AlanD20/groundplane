package runner

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/runnerconnectioncheck"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type checkAssignments interface {
	GetTaskAssignment(context.Context, string) (etcd.TaskAssignment, error)
}
type checkRuntime interface {
	Check(
		context.Context,
		runnerallocation.RuntimePlan,
		runnerallocation.RunnerRuntimeEvidence,
		runnerconnectioncheck.Request,
	) (runnerconnectioncheck.Result, error)
}

type ConnectionCheckExecutor struct {
	records   *runnerchecks.Repository
	tasks     checkAssignments
	runners   *etcd.RunnerRepository
	runtime   checkRuntime
	lifecycle *Executor
}

func NewConnectionCheckExecutor(
	records *runnerchecks.Repository,
	tasks checkAssignments,
	runnerRecords *etcd.RunnerRepository,
	runtime checkRuntime,
	lifecycle *Executor,
) (*ConnectionCheckExecutor, error) {
	if records == nil || tasks == nil || runnerRecords == nil || runtime == nil || lifecycle == nil {
		return nil, errs.New(errs.KindInternal, "Runner diagnostic execution dependencies are required")
	}
	return &ConnectionCheckExecutor{
		records:   records,
		tasks:     tasks,
		runners:   runnerRecords,
		runtime:   runtime,
		lifecycle: lifecycle,
	}, nil
}

func (executor *ConnectionCheckExecutor) Execute(ctx context.Context, task etcd.TaskRecord) error {
	if ctx == nil || etcd.ValidateTaskRecord(task) != nil || task.Executor != taskjournal.TaskExecutorController ||
		task.Type != taskjournal.TaskCheck ||
		task.Status != taskjournal.TaskStatusRunning ||
		task.Params[taskjournal.TaskResourceKindParam] != runners.TaskResourceRunner {
		return errs.New(errs.KindValidationFailed, "Runner diagnostic Task is invalid")
	}
	loaded, err := executor.records.Load(ctx, task.ID)
	if err != nil {
		return err
	}
	record := loaded.Record
	hash, err := runnerchecks.PlanHash(record)
	owner, ownerErr := runners.TaskOwner(record.Source.Desired)
	if err != nil || ownerErr != nil || task.Owner != owner || record.Owner.TaskID != task.ID ||
		record.Owner.PlanID != task.PlanID ||
		record.Owner.OperationID != task.OperationID ||
		record.Source.RunnerID != task.Target ||
		hash != task.PlanHash ||
		record.InputSHA256 != task.Params[taskjournal.TaskRunnerCheckInputParam] {
		return errs.New(errs.KindStateConflict, "Runner diagnostic differs from its captured Task input")
	}
	if record.Phase == runnerchecks.Finished {
		return runnerCheckOutcomeError(record)
	}
	claim, err := executor.tasks.GetTaskAssignment(ctx, task.ID)
	if err != nil {
		return err
	}
	claimed := claim.Task.Record
	if claimed.PlanID != task.PlanID || claimed.PlanHash != task.PlanHash || claimed.OperationID != task.OperationID ||
		claimed.Target != task.Target {
		return errs.New(errs.KindStateConflict, "Runner diagnostic Task claim changed")
	}
	ctx, cancel := context.WithDeadline(ctx, claim.Assignment.Record.Deadline)
	defer cancel()
	journal, err := runnerchecks.NewJournal(executor.records, record.Owner, claim.Assignment.Record.AssignmentID)
	if err != nil {
		return err
	}
	if record.Phase == runnerchecks.Checking {
		return stopRunnerCheck(
			ctx,
			journal,
			"interrupted",
			errs.New(
				errs.KindStateConflict,
				"Runner diagnostic was interrupted; start a new check instead of replaying it",
			),
		)
	}
	if err := ctx.Err(); err != nil {
		return stopRunnerCheck(ctx, journal, runnerCheckExecutionFailure(err), err)
	}
	if !time.Now().UTC().Before(claim.Assignment.Record.Deadline) {
		return stopRunnerCheck(ctx, journal, "timeout", context.DeadlineExceeded)
	}
	// Share the existing lifecycle lock with maintenance and removal. Waiting
	// for it cannot grant permission to execute after the Task deadline.
	if err := executor.lockLifecycle(ctx); err != nil {
		return stopRunnerCheck(ctx, journal, runnerCheckExecutionFailure(err), err)
	}
	defer executor.lifecycle.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return stopRunnerCheck(ctx, journal, runnerCheckExecutionFailure(err), err)
	}
	stale, err := executor.runners.RunnerCheckSourcesChanged(ctx, record.Source)
	if err != nil {
		return err
	}
	if stale {
		return stopRunnerCheck(
			ctx,
			journal,
			"runtime_changed",
			errs.New(errs.KindStateConflict, "Runner runtime changed after acceptance; start a new connection check"),
		)
	}
	if err := journal.Start(ctx, time.Now().UTC()); err != nil {
		return err
	}
	ownership := record.Source.Ownership
	expected := runnerallocation.RunnerRuntimeEvidence{
		Software:     record.Source.Software,
		ContainerID:  record.Source.ContainerID,
		DaemonNonce:  ownership.DaemonInstanceNonce,
		SocketDevice: ownership.SocketDevice,
		SocketInode:  ownership.SocketInode,
	}
	result, err := executor.runtime.Check(ctx, record.Plan, expected, record.Request)
	if err != nil {
		return stopRunnerCheck(ctx, journal, runnerCheckExecutionFailure(err), err)
	}
	settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	stale, err = executor.runners.RunnerCheckSourcesChanged(settlement, record.Source)
	if err != nil {
		return err
	}
	if stale {
		return stopRunnerCheck(
			ctx,
			journal,
			"runtime_changed",
			errs.New(errs.KindStateConflict, "Runner ownership changed during its diagnostic; start a new check"),
		)
	}
	if err := journal.Finish(settlement, result, time.Now().UTC()); err != nil {
		return err
	}
	completed, err := executor.records.Load(settlement, task.ID)
	if err != nil {
		return err
	}
	return runnerCheckOutcomeError(completed.Record)
}
func (executor *ConnectionCheckExecutor) lockLifecycle(ctx context.Context) error {
	timer := time.NewTicker(50 * time.Millisecond)
	defer timer.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if executor.lifecycle.mu.TryLock() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}
func stopRunnerCheck(ctx context.Context, journal *runnerchecks.Journal, failure string, cause error) error {
	settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := journal.Stop(settlement, failure, time.Now().UTC()); err != nil {
		return err
	}
	return cause
}
func runnerCheckExecutionFailure(err error) string {
	kind, _ := errs.KindOf(err)
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case kind == errs.KindStateConflict:
		return "runtime_changed"
	default:
		return "execution_failed"
	}
}
func runnerCheckOutcomeError(record runnerchecks.Record) error {
	if record.Outcome == "passed" {
		return nil
	}
	message := "Runner connection check failed; inspect its individual stages"
	if record.Outcome == "interrupted" {
		message = "Runner connection check was interrupted or its runtime changed; start a new check"
	}
	return errs.New(errs.KindStateConflict, message)
}
