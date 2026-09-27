package runnerjournal

import (
	"context"
	"path/filepath"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadyPlan reads completed creation authority across a host reboot without
// replaying the original Task or changing its boot-bound journal.
func (journal *Journal) ReadyPlan(
	ctx context.Context,
	runnerID string,
	epoch uint64,
	taskID string,
) (runnerallocation.RuntimePlan, error) {
	if ctx == nil || ctx.Err() != nil || ids.Validate(ids.KindRunner, runnerID) != nil ||
		ids.Validate(ids.KindTask, taskID) != nil || epoch == 0 {
		return runnerallocation.RuntimePlan{}, errs.New(
			errs.KindValidationFailed,
			"Runner ready-plan identity is invalid",
		)
	}
	progress, exists, err := readLongestChain(
		filepath.Join(journal.root, "v1", runnerID, strconv.FormatUint(epoch, 10), taskID),
	)
	if err != nil {
		return runnerallocation.RuntimePlan{}, err
	}
	if !exists || progress.RunnerID != runnerID || progress.TaskID != taskID || progress.RuntimeEpoch != epoch ||
		progress.Operation != runnerallocation.RuntimeOperationCreate || progress.Status != runnerallocation.RuntimeStatusReady ||
		progress.Evidence == nil || !progress.Evidence.Valid() {
		return runnerallocation.RuntimePlan{}, errs.New(
			errs.KindStateConflict,
			"Runner has no completed registration plan",
		)
	}
	return progress.Plan.Clone(), nil
}
