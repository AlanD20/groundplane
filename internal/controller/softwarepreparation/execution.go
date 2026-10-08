package softwarepreparation

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const failureCheckpointTimeout = 5 * time.Second

// Execute resumes only the frozen native Task input. Published outputs are
// skipped after managed-registry readback; no ref or release tag is resolved
// during execution.
func (service *Service) Execute(ctx context.Context, task etcd.TaskRecord) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "software preparation context is required")
	}
	if task.Status != taskjournal.TaskStatusRunning {
		return errs.New(errs.KindValidationFailed, "software preparation requires a running Controller Task")
	}
	input, err := DecodeTask(task)
	if err != nil {
		return err
	}
	current, err := service.records.Get(ctx, task.ID)
	if err != nil {
		return err
	}
	if current.Record.OperationID != task.OperationID || current.Record.InputSHA256 != task.PlanHash ||
		!current.Record.Source.Equal(input.Source) {
		return errs.New(errs.KindStateConflict, "software preparation progress does not match its Task")
	}
	if current.Record.Progress.Phase == preparation.PhaseVerified {
		return nil
	}
	if current.Record.Progress.Phase == preparation.PhaseFailed {
		return errs.New(errs.KindStateConflict, "failed software preparation cannot resume")
	}
	report := func(reportCtx context.Context, progress preparation.Progress) error {
		updated, err := service.records.Checkpoint(
			reportCtx, task.ID, task.OperationID, task.PlanHash, progress, service.now().UTC(),
		)
		if err == nil {
			current = updated
			return nil
		}
		return errs.Wrap(errs.KindRequestUnavailable, err)
	}
	result, executionErr := service.preparer.Prepare(ctx, input, current.Record.Progress.Result, report)
	if executionErr == nil {
		return nil
	}

	checkpointCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), failureCheckpointTimeout)
	defer cancel()
	if !result.Equal(current.Record.Progress.Result) {
		progress := preparation.Progress{Phase: publishedPhase(result), Result: result}
		if checkpointed, checkpointErr := service.records.Checkpoint(
			checkpointCtx, task.ID, task.OperationID, task.PlanHash, progress, service.now().UTC(),
		); checkpointErr != nil {
			executionErr = errs.WrapJoined(errs.KindRequestUnavailable, checkpointErr, executionErr)
		} else {
			current = checkpointed
		}
	}
	if ctx.Err() != nil || errors.Is(executionErr, context.Canceled) ||
		errors.Is(executionErr, context.DeadlineExceeded) || errs.IsRetryable(executionErr) ||
		errors.Is(executionErr, errs.New(errs.KindStorageUnavailable, "")) {
		return executionErr
	}

	failure := preparation.Progress{Phase: preparation.PhaseFailed, Result: current.Record.Progress.Result}
	failure.ErrorCode, failure.ErrorDetail = publicDiagnostic(executionErr)
	_, checkpointErr := service.records.Checkpoint(
		checkpointCtx, task.ID, task.OperationID, task.PlanHash, failure, service.now().UTC(),
	)
	if checkpointErr != nil {
		return errs.WrapJoined(errs.KindRequestUnavailable, checkpointErr, executionErr)
	}
	return executionErr
}

func publishedPhase(result preparation.Result) preparation.Phase {
	if result.Controller != nil {
		return preparation.PhaseControllerPublished
	}
	if result.Agent != nil {
		return preparation.PhaseAgentPublished
	}
	return preparation.PhasePreparing
}

func publicDiagnostic(err error) (string, string) {
	problem := errs.New(errs.KindInternal, "").ToProblem()
	var domainError *errs.Error
	if errors.As(err, &domainError) {
		problem = domainError.ToProblem()
	}
	return string(problem.Code), problem.Detail
}

var _ RecordStore = (*preparationrecord.Repository)(nil)
