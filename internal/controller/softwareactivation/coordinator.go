package softwareactivation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	activationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	firstRetryDelay = 5 * time.Second
)

// Tick performs at most one durable transition or one child observation. It
// never waits for a native Controller or Agent Task to finish.
func (service *Service) Tick(ctx context.Context) (TickResult, error) {
	if ctx == nil {
		return TickResult{}, errs.New(errs.KindInternal, "software activation context is required")
	}
	claims, err := service.tasks.ListSoftwareActivationClaims(ctx)
	if err != nil {
		return TickResult{}, err
	}
	if len(claims) > 1 {
		return TickResult{}, errs.New(errs.KindInternal, "multiple software activations are claimed")
	}
	if len(claims) == 0 {
		claimed, found, err := service.tasks.ClaimNextSoftwareActivation(ctx, service.now().UTC())
		if err != nil || !found {
			return TickResult{}, err
		}
		return TickResult{TaskID: claimed.Record.ID, Progressed: true}, nil
	}
	task := claims[0].Record
	record, err := service.records.Get(ctx, task.ID)
	if err != nil {
		return TickResult{}, err
	}
	if record.Record.OperationID != task.OperationID || record.Record.InputSHA256 != task.PlanHash ||
		task.Status != taskjournal.TaskStatusRunning {
		return TickResult{}, errs.New(errs.KindStateConflict, "software activation claim and progress differ")
	}
	progress := record.Record.Progress
	now := service.now().UTC()
	if !progress.RetryAt.IsZero() && now.Before(progress.RetryAt) {
		return TickResult{TaskID: task.ID}, nil
	}

	selection := record.Record.Input.Preparation.Source.Selection
	switch progress.Phase {
	case activationrecord.PhaseAccepted:
		if selection.IncludesController() {
			next := cleared(progress)
			next.Phase = activationrecord.PhaseStagingController
			return service.checkpoint(ctx, task, next, now)
		}
		return service.publishAgent(ctx, task, record.Record, now)

	case activationrecord.PhaseStagingController:
		release, actionErr := service.stager.Stage(
			ctx, record.Record.Input.Preparation, record.Record.Input.StagingAgentImage,
		)
		if actionErr != nil {
			return service.handleActionError(ctx, task, record.Record, actionErr, now)
		}
		controller := record.Record.Input.Preparation.Progress.Result.Controller
		if err := release.Validate(); err != nil || controller == nil ||
			string(release.Manifest.ControllerSHA256) != controller.BinarySHA256 ||
			release.Manifest.AgentImage != record.Record.Input.StagingAgentImage {
			if err == nil {
				err = errs.New(errs.KindStateConflict, "staged Controller manifest differs from the frozen preparation")
			}
			return service.handleActionError(ctx, task, record.Record, err, now)
		}
		next := cleared(progress)
		next.Phase, next.StagedController = activationrecord.PhaseControllerStaged, &release
		return service.checkpoint(ctx, task, next, now)

	case activationrecord.PhaseControllerStaged:
		accepted, actionErr := service.controllerUpdates.PublishControllerUpdate(
			ctx, string(progress.StagedController.Release), childKey(task.ID, "controller"), record.Record.Input.Agent,
		)
		if actionErr != nil {
			return service.handleActionError(ctx, task, record.Record, actionErr, now)
		}
		if ids.Validate(ids.KindTask, accepted.TaskID) != nil {
			return service.handleActionError(ctx, task, record.Record,
				errs.New(errs.KindInternal, "Controller update publisher returned an invalid Task"), now)
		}
		next := cleared(progress)
		next.Phase, next.ControllerTaskID = activationrecord.PhaseControllerPublished, accepted.TaskID
		return service.checkpoint(ctx, task, next, now)

	case activationrecord.PhaseControllerPublished:
		child, actionErr := service.tasks.GetTask(ctx, progress.ControllerTaskID)
		if actionErr != nil {
			return service.handleActionError(ctx, task, record.Record, actionErr, now)
		}
		if !validChild(
			child.Record,
			taskjournal.TaskResourceController,
			"controller",
			childKey(task.ID, "controller"),
		) {
			return service.fail(ctx, task, progress,
				errs.New(errs.KindInternal, "Controller update child authority is invalid"), now)
		}
		switch child.Record.Status {
		case taskjournal.TaskStatusPending, taskjournal.TaskStatusRunning:
			return TickResult{TaskID: task.ID}, nil
		case taskjournal.TaskStatusCompleted:
			next := cleared(progress)
			next.Phase, next.ControllerApplied = activationrecord.PhaseControllerApplied, true
			return service.checkpoint(ctx, task, next, now)
		default:
			return service.fail(
				ctx,
				task,
				progress,
				errs.Newf(
					errs.KindRequestFailed,
					"Controller update Task ended with status %s",
					child.Record.Status,
				),
				now,
			)
		}

	case activationrecord.PhaseControllerApplied:
		if !selection.IncludesAgent() {
			next := cleared(progress)
			next.Phase = activationrecord.PhaseCompleted
			return service.terminal(ctx, task, next, taskjournal.TaskStatusCompleted, now)
		}
		return service.publishAgent(ctx, task, record.Record, now)

	case activationrecord.PhaseAgentPublished:
		child, actionErr := service.tasks.GetTask(ctx, progress.AgentTaskID)
		if actionErr != nil {
			return service.handleActionError(ctx, task, record.Record, actionErr, now)
		}
		if !validChild(
			child.Record,
			taskjournal.TaskResourceAgent,
			record.Record.Input.Agent.ID,
			childKey(task.ID, "agent"),
		) {
			return service.fail(ctx, task, progress,
				errs.New(errs.KindInternal, "Agent update child authority is invalid"), now)
		}
		switch child.Record.Status {
		case taskjournal.TaskStatusPending, taskjournal.TaskStatusRunning:
			return TickResult{TaskID: task.ID}, nil
		case taskjournal.TaskStatusCompleted:
			next := cleared(progress)
			next.Phase, next.AgentApplied = activationrecord.PhaseCompleted, true
			return service.terminal(ctx, task, next, taskjournal.TaskStatusCompleted, now)
		default:
			return service.fail(ctx, task, progress,
				errs.Newf(errs.KindRequestFailed, "Agent update Task ended with status %s", child.Record.Status), now)
		}

	case activationrecord.PhaseCompleted, activationrecord.PhaseFailed:
		return TickResult{}, errs.New(errs.KindInternal, "terminal software activation retained a running claim")
	default:
		return TickResult{}, errs.New(errs.KindInternal, "software activation phase is unknown")
	}
}

func (service *Service) publishAgent(
	ctx context.Context,
	task etcd.TaskRecord,
	record activationrecord.Record,
	now time.Time,
) (TickResult, error) {
	predecessor := record.Input.Agent
	if predecessor == nil || record.Input.Preparation.Progress.Result.Agent == nil {
		return service.fail(ctx, task, record.Progress,
			errs.New(errs.KindStateConflict, "Agent activation input is incomplete"), now)
	}
	target := record.Input.Preparation.Progress.Result.Agent.Artifact.Reference
	if err := service.stager.StageAgent(ctx, record.Input.Preparation.Progress.Result.Agent.Artifact); err != nil {
		return service.handleActionError(ctx, task, record, err, now)
	}
	accepted, err := service.agentUpdates.PublishAgentUpdate(
		ctx, *predecessor, target, childKey(task.ID, "agent"),
	)
	if err != nil {
		return service.handleActionError(ctx, task, record, err, now)
	}
	if ids.Validate(ids.KindTask, accepted.TaskID) != nil {
		return service.handleActionError(ctx, task, record,
			errs.New(errs.KindInternal, "Agent update publisher returned an invalid Task"), now)
	}
	next := cleared(record.Progress)
	next.Phase, next.AgentTaskID = activationrecord.PhaseAgentPublished, accepted.TaskID
	return service.checkpoint(ctx, task, next, now)
}

func (service *Service) checkpoint(
	ctx context.Context,
	task etcd.TaskRecord,
	progress activationrecord.Progress,
	now time.Time,
) (TickResult, error) {
	_, err := service.tasks.CheckpointSoftwareActivation(
		ctx, task.ID, task.OperationID, task.PlanHash, progress, now,
	)
	return TickResult{TaskID: task.ID, Progressed: err == nil}, err
}

func (service *Service) terminal(
	ctx context.Context,
	task etcd.TaskRecord,
	progress activationrecord.Progress,
	status taskjournal.TaskStatus,
	now time.Time,
) (TickResult, error) {
	_, err := service.tasks.TerminalizeSoftwareActivation(ctx, task.ID, progress, status, now)
	return TickResult{TaskID: task.ID, Progressed: err == nil}, err
}

func (service *Service) handleActionError(
	ctx context.Context,
	task etcd.TaskRecord,
	record activationrecord.Record,
	actionErr error,
	now time.Time,
) (TickResult, error) {
	if ctx.Err() != nil {
		return TickResult{TaskID: task.ID}, ctx.Err()
	}
	failures := record.Progress.FailureCount + 1
	retryable := errs.IsRetryable(actionErr) || errors.Is(actionErr, context.Canceled) ||
		errors.Is(actionErr, context.DeadlineExceeded)
	if retryable && failures < activationrecord.MaximumPhaseFailures {
		next := record.Progress
		next.FailureCount = failures
		next.ErrorCode, next.ErrorDetail = publicDiagnostic(actionErr)
		next.RetryAt = now.Add(firstRetryDelay * time.Duration(1<<(failures-1))).UTC()
		return service.checkpoint(ctx, task, next, now)
	}
	next := record.Progress
	next.FailureCount = failures
	return service.fail(ctx, task, next, actionErr, now)
}

func (service *Service) fail(
	ctx context.Context,
	task etcd.TaskRecord,
	progress activationrecord.Progress,
	cause error,
	now time.Time,
) (TickResult, error) {
	next := progress
	next.Phase, next.RetryAt = activationrecord.PhaseFailed, time.Time{}
	next.ErrorCode, next.ErrorDetail = publicDiagnostic(cause)
	if next.FailureCount == 0 {
		next.FailureCount = 1
	}
	return service.terminal(ctx, task, next, taskjournal.TaskStatusFailed, now)
}

func cleared(progress activationrecord.Progress) activationrecord.Progress {
	progress.FailureCount, progress.RetryAt = 0, time.Time{}
	progress.ErrorCode, progress.ErrorDetail = "", ""
	return progress
}

func childKey(parentTaskID, component string) string {
	return fmt.Sprintf("software-%s-%s", parentTaskID, component)
}

func validChild(task etcd.TaskRecord, resource, target, key string) bool {
	return task.Type == taskjournal.TaskUpdate && task.Executor == taskjournal.TaskExecutorController &&
		task.Owner == taskjournal.PlatformTaskOwner() && task.Actor == taskjournal.TaskActorOperator &&
		task.Params[taskjournal.TaskResourceKindParam] == resource && task.Target == target && task.IdempotencyKey == key
}

func publicDiagnostic(err error) (string, string) {
	problem := errs.New(errs.KindInternal, "").ToProblem()
	var domainError *errs.Error
	if errors.As(err, &domainError) {
		problem = domainError.ToProblem()
	}
	return string(problem.Code), problem.Detail
}
