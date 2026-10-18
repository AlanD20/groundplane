package controllerupgrade

import (
	"context"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Progress records safe observations only. It grants no effect permission;
// release publication and cutover remain guarded by the native operation.
type Progress interface {
	Run(context.Context, string, func(context.Context) error) error
	Start(string) error
}

func (coordinator *Coordinator) Execute(ctx context.Context, expected upgrade.Journal, forward bool, progress Progress) (upgrade.ExecutionResult, error) {
	if progress == nil {
		return upgrade.ExecutionResult{}, errs.New(errs.KindInternal, "Controller activation observation is required")
	}
	status, err := coordinator.execute(ctx, expected, forward, progress)
	if err != nil {
		return upgrade.ExecutionResult{}, err
	}
	coordinator.mu.Lock()
	operation := coordinator.active
	if operation == nil || !operation.expected.SameOperation(expected) || operation.settled != status {
		coordinator.mu.Unlock()
		return upgrade.ExecutionResult{}, errs.New(errs.KindStateConflict, "Controller activation outcome changed")
	}
	result := upgrade.ExecutionResult{Outcome: status, Settled: true, Failure: taskprogress.CloneFailure(operation.failure)}
	coordinator.mu.Unlock()
	journal, found, err := coordinator.current(ctx, operation)
	if err != nil {
		return upgrade.ExecutionResult{}, err
	}
	if found {
		result.Journal = &journal
		if journal.Failure != nil {
			result.Failure = taskprogress.CloneFailure(journal.Failure)
		}
	}
	if status == workexecution.Succeeded {
		result.Failure = nil
	} else if result.Failure == nil {
		cause := errs.New(errs.KindRequestFailed, "Controller activation did not complete")
		if status == workexecution.Cancelled {
			result.Failure = taskprogress.CaptureControllerFailure(context.Canceled)
		} else if !coordinator.now().Before(expected.Deadline) {
			result.Failure = taskprogress.CaptureControllerFailure(context.DeadlineExceeded)
		} else {
			result.Failure = taskprogress.CaptureControllerFailure(cause)
		}
	}
	return result, result.Validate(expected)
}

// Unresolved preserves failure evidence when the handoff did not finish in this
// process. It is never used on ordinary daemon shutdown: the replacement process
// reconciles that original attempt rather than manufacturing a failed upgrade.
func (coordinator *Coordinator) Unresolved(ctx context.Context, expected upgrade.Journal, cause error) (upgrade.ExecutionResult, error) {
	coordinator.mu.Lock()
	operation := coordinator.active
	coordinator.mu.Unlock()
	if operation == nil || !operation.expected.SameOperation(expected) {
		return upgrade.ExecutionResult{}, errs.New(errs.KindStateConflict, "Controller activation has no captured owner")
	}
	journal, found, err := coordinator.current(ctx, operation)
	if err != nil {
		return upgrade.ExecutionResult{}, err
	}
	result := upgrade.ExecutionResult{Outcome: workexecution.Failed, Failure: taskprogress.CaptureControllerFailure(cause)}
	if found {
		result.Journal = &journal
	}
	return result, result.Validate(expected)
}
