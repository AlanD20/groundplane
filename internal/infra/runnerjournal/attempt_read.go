package runnerjournal

import (
	"context"
	"errors"
	"os"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadAttempt is inspection only. It can read a previous boot's evidence without
// reopening that journal for effects or granting another registration attempt.
func (journal *Journal) ReadAttempt(ctx context.Context, attempt runnerallocation.RunnerRuntimeAttempt,
	operation runnerallocation.RuntimeOperation,
) (runnerallocation.RunnerRuntimeProgress, bool, error) {
	if err := validateAttempt(ctx, attempt, operation); err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, false, err
	}
	directory, err := journal.operationDirectory(attempt)
	if err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, false, err
	}
	if _, err := os.Lstat(directory); errors.Is(err, os.ErrNotExist) {
		return runnerallocation.RunnerRuntimeProgress{}, false, nil
	} else if err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, false, errs.Wrap(errs.KindInternal, err)
	}
	progress, exists, err := readLongestChain(directory)
	if err != nil || !exists {
		return progress, exists, err
	}
	if progress.TaskID != attempt.TaskID || progress.WorkItemID != attempt.WorkItemID ||
		progress.AttemptID != attempt.AttemptID || progress.AttemptNumber != attempt.AttemptNumber ||
		progress.Executor != attempt.Executor || progress.Operation != operation ||
		progress.RunnerID != attempt.Plan.RunnerID || progress.PlanDigest != attempt.Plan.Digest() ||
		progress.RuntimeEpoch != attempt.Plan.RuntimeEpoch || progress.OwnershipNonce != attempt.OwnershipNonce {
		return runnerallocation.RunnerRuntimeProgress{}, false, errs.New(
			errs.KindStateConflict,
			"Runner journal does not belong to this captured attempt",
		)
	}
	return progress, true, nil
}
