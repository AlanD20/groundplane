package runner

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/runnersoftware"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerupdates"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type softwareRuntime interface {
	Prune(context.Context, runnerallocation.RuntimePlan, runnersoftware.Request) error
	WaitIdle(context.Context, runnerallocation.RuntimePlan, runnerallocation.RunnerRuntimeEvidence) error
	Prepare(context.Context, runnerallocation.RuntimePlan, runnersoftware.Request) error
	Recover(
		context.Context,
		runnerallocation.RuntimePlan,
		runnerallocation.RunnerRuntimeEvidence,
		runnersoftware.Request,
	) (*runnersoftware.Result, error)

	Update(
		context.Context,
		runnerallocation.RuntimePlan,
		runnerallocation.RunnerRuntimeEvidence,
		runnersoftware.Request,
	) (runnersoftware.Result, error)
}

type SoftwareExecutor struct {
	queue *workqueue.Repository

	records   *runnerupdates.Repository
	runners   *etcd.RunnerRepository
	runtime   softwareRuntime
	lifecycle *Executor
}

func NewSoftwareExecutor(
	records *runnerupdates.Repository,
	runnerRecords *etcd.RunnerRepository,
	runtime softwareRuntime,
	lifecycle *Executor, queue *workqueue.Repository,
) (*SoftwareExecutor, error) {
	if records == nil || runnerRecords == nil || runtime == nil || lifecycle == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "Runner update execution dependencies are required")
	}
	return &SoftwareExecutor{queue: queue,
		records:   records,
		runners:   runnerRecords,
		runtime:   runtime,
		lifecycle: lifecycle,
	}, nil
}

func (executor *SoftwareExecutor) Execute(ctx context.Context, delivery workqueue.Delivery) error {
	err := executor.execute(ctx, delivery)
	if err == nil || ctx == nil {
		return err
	}
	settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	loaded, loadErr := executor.loadDelivery(settlement, delivery)
	if loadErr != nil {
		return errs.WrapJoined(errs.KindStateConflict, err, loadErr)
	}
	if loaded.Record.Phase == runnerupdates.Finished {
		return err
	}
	attempt := delivery.Attempt.Attempt
	journal, journalErr := runnerupdates.NewJournal(executor.records, loaded.Record.Owner, attempt.ID, attempt.Executor)
	if journalErr != nil {
		return errs.WrapJoined(errs.KindStateConflict, err, journalErr)
	}
	recovered, recoverErr := executor.recover(settlement, loaded.Record)
	if recoverErr != nil {
		return errs.WrapJoined(errs.KindStateConflict, err, recoverErr)
	}
	if recovered != nil {
		return journal.Finish(settlement, *recovered, time.Now().UTC())
	}
	if stopErr := journal.Stop(settlement, runnerUpdateExecutionFailure(err), time.Now().UTC()); stopErr != nil {
		return errs.WrapJoined(errs.KindStateConflict, err, stopErr)
	}
	return err
}

func (executor *SoftwareExecutor) execute(ctx context.Context, delivery workqueue.Delivery) error {
	item, attempt := delivery.Item.Item, delivery.Attempt.Attempt
	if ctx == nil || item.Validate() != nil || attempt.Validate() != nil ||
		item.Operation != workexecution.RunnerSoftware || item.Phase != workexecution.Running ||
		attempt.Executor.Executor != workexecution.Controller || attempt.Direction != workexecution.Forward ||
		attempt.StartedAt == nil || attempt.FinishedAt != nil || item.ActiveAttemptID != attempt.ID ||
		attempt.TaskID != item.TaskID || attempt.WorkItemID != item.ID ||
		item.Input == nil || *item.Input != attempt.Input {
		return errs.New(errs.KindValidationFailed, "Runner update execution is invalid")
	}
	loaded, err := executor.records.Load(ctx, item.TaskID)
	if err != nil {
		return err
	}
	record := loaded.Record
	hash, err := runnerupdates.PlanHash(record)
	if err != nil || record.Owner.TaskID != item.TaskID || record.Owner.WorkItemID != item.ID ||
		record.Owner.PlanID != attempt.Input.ID || hash != attempt.Input.SHA256 || record.Source.RunnerID != item.TargetID {
		return errs.New(errs.KindStateConflict, "Runner update differs from captured execution input")
	}
	if record.Phase == runnerupdates.Finished {
		return runnerUpdateOutcomeError(record)
	}
	ctx, cancel := context.WithDeadline(ctx, attempt.Deadline)
	defer cancel()
	journal, err := runnerupdates.NewJournal(executor.records, record.Owner, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	if record.Phase == runnerupdates.Updating {
		return errs.New(errs.KindStateConflict, "Runner update was interrupted; reconcile its captured package state")
	}

	if err := ctx.Err(); err != nil {
		return stopRunnerUpdate(ctx, journal, runnerUpdateExecutionFailure(err), err)
	}
	if !time.Now().UTC().Before(attempt.Deadline) {
		return stopRunnerUpdate(ctx, journal, "timeout", context.DeadlineExceeded)
	}
	if err := executor.runtime.Prepare(ctx, record.Plan, record.Request); err != nil {
		return err
	}
	o := record.Source.Ownership
	idleExpected := runnerallocation.RunnerRuntimeEvidence{
		Software:     record.Source.Software,
		ContainerID:  record.Source.ContainerID,
		DaemonNonce:  o.DaemonInstanceNonce,
		SocketDevice: o.SocketDevice,
		SocketInode:  o.SocketInode,
	}
	if err := executor.runtime.WaitIdle(ctx, record.Plan, idleExpected); err != nil {
		return err
	}
	// Share the existing lifecycle lock with maintenance and removal. Waiting
	// for it cannot grant permission to execute after the Task deadline.
	if err := executor.lockLifecycle(ctx); err != nil {
		return stopRunnerUpdate(ctx, journal, runnerUpdateExecutionFailure(err), err)
	}
	defer executor.lifecycle.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return stopRunnerUpdate(ctx, journal, runnerUpdateExecutionFailure(err), err)
	}
	stale, err := executor.runners.RunnerRuntimeSourcesChanged(ctx, record.Source)
	if err != nil {
		return err
	}
	if stale {
		return stopRunnerUpdate(
			ctx,
			journal,
			"runtime_changed",
			errs.New(errs.KindStateConflict, "Runner runtime changed after acceptance; start a new software update"),
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
	result, err := executor.runtime.Update(ctx, record.Plan, expected, record.Request)
	if err != nil {
		return err
	}
	settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	stale, err = executor.runners.RunnerRuntimeSourcesChanged(settlement, record.Source)
	if err != nil {
		return err
	}
	if stale {
		return errs.New(errs.KindStateConflict, "Runner ownership changed during its software update")
	}

	if err := journal.Finish(settlement, result, time.Now().UTC()); err != nil {
		return err
	}
	completed, err := executor.records.Load(settlement, item.TaskID)
	if err != nil {
		return err
	}
	return runnerUpdateOutcomeError(completed.Record)
}
func (executor *SoftwareExecutor) lockLifecycle(ctx context.Context) error {
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
func stopRunnerUpdate(ctx context.Context, journal *runnerupdates.Journal, failure string, cause error) error {
	settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := journal.Stop(settlement, failure, time.Now().UTC()); err != nil {
		return err
	}
	return cause
}
func runnerUpdateExecutionFailure(err error) string {
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
func runnerUpdateOutcomeError(record runnerupdates.Record) error {
	if record.Outcome == "passed" {
		return nil
	}
	message := "Runner software update failed; inspect its individual stages"
	if record.Outcome == "interrupted" {
		message = "Runner software update was interrupted or its runtime changed; inspect recovery before requesting another update"
	}
	return errs.New(errs.KindStateConflict, message)
}

func (executor *SoftwareExecutor) recover(
	ctx context.Context,
	record runnerupdates.Record,
) (*runnersoftware.Result, error) {
	if record.Phase == runnerupdates.Finished {
		return nil, nil
	}
	o := record.Source.Ownership
	expected := runnerallocation.RunnerRuntimeEvidence{
		Software:     record.Source.Software,
		ContainerID:  record.Source.ContainerID,
		DaemonNonce:  o.DaemonInstanceNonce,
		SocketDevice: o.SocketDevice,
		SocketInode:  o.SocketInode,
	}
	if err := executor.lockLifecycle(ctx); err != nil {
		return nil, err
	}
	defer executor.lifecycle.mu.Unlock()
	return executor.runtime.Recover(ctx, record.Plan, expected, record.Request)
}
