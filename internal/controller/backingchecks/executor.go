// Package backingchecks coordinates one native consumer access check. Agent
// exchange failures leave exact cleanup authority, never permission to replay.
package backingchecks

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type assignments interface {
	GetTaskAssignment(context.Context, string) (etcd.TaskAssignment, error)
}
type exchange interface {
	CheckBackingAccess(context.Context, string, *agentpb.CheckBackingAccess) (*agentpb.BackingAccessResult, error)
}

type Executor struct {
	repository  *checkrecord.Repository
	assignments assignments
	exchange    exchange
	protector   *secretvalue.Protector
}

func NewExecutor(
	repository *checkrecord.Repository,
	tasks assignments,
	channel exchange,
	protector *secretvalue.Protector,
) (*Executor, error) {
	if repository == nil || tasks == nil || channel == nil || protector == nil {
		return nil, errs.New(errs.KindInternal, "Backing access check executor dependencies are required")
	}
	return &Executor{repository: repository, assignments: tasks, exchange: channel, protector: protector}, nil
}

func (executor *Executor) Execute(ctx context.Context, task etcd.TaskRecord) error {
	if ctx == nil || task.Executor != taskjournal.TaskExecutorController || task.Type != taskjournal.TaskCheck ||
		task.Status != taskjournal.TaskStatusRunning || task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceAttach ||
		len(
			task.Params,
		) != 2 || len(task.Steps) != 0 || len(task.Materializations) != 0 || task.Configuration != nil || task.RenderGeneration != 1 {
		return errs.New(errs.KindValidationFailed, "Backing access check Task is invalid")
	}
	loaded, err := executor.repository.Load(ctx, task.ID)
	if err != nil {
		return err
	}
	record := loaded.Record
	defer record.Clear()
	planHash, err := checkrecord.PlanHash(record)
	if err != nil || record.Owner.TaskID != task.ID || record.Owner.PlanID != task.PlanID ||
		record.Owner.OperationID != task.OperationID ||
		record.Owner.EnvironmentID != task.Owner.EnvironmentID ||
		record.Source.Parameters.AttachID != task.Target ||
		record.RetryOf != task.RetryOf ||
		planHash != task.PlanHash ||
		record.ConfigurationSHA256 != task.Params[taskjournal.TaskBackingCheckInputParam] {
		return errs.New(errs.KindStateConflict, "Backing access check differs from its sealed Task input")
	}
	if record.Phase == checkrecord.Finished {
		return outcomeError(record, task.RetryOf != "")
	}
	claim, err := executor.assignments.GetTaskAssignment(ctx, task.ID)
	if err != nil {
		return err
	}
	claimed := claim.Task.Record
	if claimed.PlanID != task.PlanID || claimed.PlanHash != task.PlanHash || claimed.OperationID != task.OperationID ||
		claimed.Target != task.Target {
		return errs.New(errs.KindStateConflict, "Backing access check Task claim changed")
	}
	journal, err := checkrecord.NewJournal(executor.repository, record.Owner, claim.Assignment.Record.AssignmentID)
	if err != nil {
		return err
	}
	if record.Phase != checkrecord.Ready {
		return executor.resume(ctx, record, journal)
	}
	if err := ctx.Err(); err != nil {
		return finishUnstarted(ctx, journal, err)
	}
	if !time.Now().UTC().Before(claim.Assignment.Record.Deadline) {
		return finishUnstarted(ctx, journal, context.DeadlineExceeded)
	}
	started := false
	err = executor.withInput(ctx, record, func(input backingaccess.Request) error {
		request, err := requestFor(record, input, false)
		if err != nil {
			return err
		}
		defer backingaccess.ClearMachineRequest(request)
		if err := journal.Start(ctx, time.Now().UTC()); err != nil {
			return err
		}
		started = true
		result, probeErr := executor.exchange.CheckBackingAccess(ctx, record.Source.AgentID, request)
		if probeErr != nil {
			return probeErr
		}
		return executor.recordResult(ctx, record, journal, result)
	})
	if err == nil || started {
		return err
	}
	// If Start was uncertain, the Ready compare fails rather than permitting a
	// second probe. The next execution sees Checking and performs cleanup only.
	return finishUnstarted(ctx, journal, err)
}

func requestFor(
	record checkrecord.Record,
	input backingaccess.Request,
	cleanup bool,
) (*agentpb.CheckBackingAccess, error) {
	target, err := record.Source.Target()
	if err != nil {
		return nil, err
	}
	if cleanup {
		input = backingaccess.Request{AttachID: input.AttachID, CredentialAttachID: input.CredentialAttachID,
			EnvironmentID: input.EnvironmentID, ServiceID: input.ServiceID, BackingServiceID: input.BackingServiceID}
	}
	return &agentpb.CheckBackingAccess{TaskId: record.HelperTaskID, HelperImage: record.Source.HelperImage,
		ConsumerContainerId: record.Source.ContainerID, ConsumerRuntime: target,
		Parameters: backingaccess.ParametersForMachine(input), CleanupOnly: cleanup}, nil
}

func (executor *Executor) recordResult(ctx context.Context, record checkrecord.Record, journal *checkrecord.Journal,
	result *agentpb.BackingAccessResult,
) error {
	settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if result == nil || result.TaskId != record.HelperTaskID ||
		result.HelperName != backingaccess.HelperName(record.HelperTaskID) {
		return errs.New(
			errs.KindStateConflict,
			"Backing access check returned no owned result; cleanup remains required",
		)
	}
	var observed *backingaccess.Result
	if result.Observation != nil {
		observation, err := backingaccess.ObservationFromMachine(result.Observation)
		if err != nil {
			return err
		}
		observed = &observation
	}
	if err := journal.Observe(settlement, observed, result.CleanupRequired, result.Failure); err != nil {
		return err
	}
	latest, err := executor.repository.Load(settlement, record.Owner.TaskID)
	if err != nil {
		return err
	}
	defer latest.Record.Clear()
	if latest.Record.CleanupPending {
		return errs.New(
			errs.KindStateConflict,
			"Backing access check helper cleanup is incomplete; retry its cleanup Task",
		)
	}
	if err := journal.Finish(settlement, latest.Record.ProbeFailure, time.Now().UTC()); err != nil {
		return err
	}
	return failureError(latest.Record.ProbeFailure)
}

func (executor *Executor) resume(ctx context.Context, record checkrecord.Record, journal *checkrecord.Journal) error {
	settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), backingaccess.ExchangeTimeout+15*time.Second)
	defer cancel()
	if record.Phase == checkrecord.Checking && record.ProbeRecorded && !record.CleanupPending {
		if err := journal.Finish(settlement, record.ProbeFailure, time.Now().UTC()); err != nil {
			return err
		}
		return failureError(record.ProbeFailure)
	}
	if err := journal.ResumeCleanup(settlement); err != nil {
		return err
	}
	request, err := requestFor(record, record.Source.Parameters, true)
	if err != nil {
		return err
	}
	result, err := executor.exchange.CheckBackingAccess(settlement, record.Source.AgentID, request)
	if err != nil {
		return err
	}
	if result == nil || result.TaskId != record.HelperTaskID ||
		result.HelperName != backingaccess.HelperName(record.HelperTaskID) ||
		result.CleanupRequired ||
		result.Failure != "" ||
		result.Observation != nil {
		return errs.New(errs.KindStateConflict, "Backing access cleanup is not proven; its ownership is retained")
	}
	if err := journal.FinishCleanup(settlement, time.Now().UTC()); err != nil {
		return err
	}
	return outcomeError(
		checkrecord.Record{Outcome: checkrecord.Interrupted, Failure: checkrecord.InterruptedFailure},
		record.RetryOf != "",
	)
}

func finishUnstarted(ctx context.Context, journal *checkrecord.Journal, cause error) error {
	settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := journal.FinishUnstarted(settlement, backingaccess.MachineFailure(cause), time.Now().UTC()); err != nil {
		return err
	}
	return cause
}

func outcomeError(record checkrecord.Record, cleanupRetry bool) error {
	if record.Outcome == checkrecord.Passed || record.Outcome == checkrecord.Interrupted && cleanupRetry {
		return nil
	}
	return failureError(record.Failure)
}

func failureError(failure string) error {
	switch failure {
	case "":
		return nil
	case string(backingaccess.TimeoutFailure):
		return context.DeadlineExceeded
	case string(backingaccess.Canceled):
		return context.Canceled
	case string(backingaccess.ConnectionFailure):
		return errs.New(errs.KindStorageUnavailable, "consumer could not connect to the Backing endpoint")
	case string(backingaccess.AuthenticationFailed):
		return errs.New(errs.KindValidationFailed, "Backing service rejected this consumer's credentials")
	case string(backingaccess.AccessDenied):
		return errs.New(errs.KindValidationFailed, "consumer could not access the selected Backing database")
	case backingaccess.RuntimeUnavailable:
		return errs.New(
			errs.KindStateConflict,
			"consumer container is no longer running with the captured runtime; deploy it before a new access check",
		)
	case backingaccess.OwnershipFailure:
		return errs.New(
			errs.KindStateConflict,
			"Backing access helper ownership did not match; inspect retained cleanup",
		)
	case backingaccess.CleanupFailure:
		return errs.New(
			errs.KindStateConflict,
			"Backing access helper cleanup failed; retry cleanup before removing its resources",
		)
	case checkrecord.InterruptedFailure:
		return errs.New(
			errs.KindStateConflict,
			"Backing access check was interrupted; helper cleanup completed, but access was not certified",
		)
	default:
		return errs.New(
			errs.KindInternal,
			"Backing access check could not produce a valid result; inspect its stages and cleanup",
		)
	}
}
