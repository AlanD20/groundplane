package runnerjournal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// LatestCreation returns the last plan that could have issued creation effects.
// Admission alone does not create a journal. Retries that fail during cleanup
// therefore still refer to the preceding creation, not a reconstructed plan.
func (journal *Journal) LatestCreation(
	ctx context.Context,
	runnerID string,
) (runnerallocation.RunnerRuntimeProgress, bool, error) {
	var latest runnerallocation.RunnerRuntimeProgress
	if ctx == nil || ids.Validate(ids.KindRunner, runnerID) != nil {
		return latest, false, errs.New(errs.KindValidationFailed, "Runner journal identity is invalid")
	}
	if err := ctx.Err(); err != nil {
		return latest, false, err
	}
	root := filepath.Join(journal.root, "v1", runnerID)
	epochs, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return latest, false, nil
	}
	if err != nil {
		return latest, false, errs.Wrap(errs.KindInternal, err)
	}
	for _, directory := range epochs {
		if err := ctx.Err(); err != nil {
			return latest, false, err
		}
		epoch, err := strconv.ParseUint(directory.Name(), 10, 64)
		if err != nil || epoch == 0 || strconv.FormatUint(epoch, 10) != directory.Name() || !directory.IsDir() {
			return latest, false, corruptJournal()
		}
		tasks, err := os.ReadDir(filepath.Join(root, directory.Name()))
		if err != nil {
			return latest, false, errs.Wrap(errs.KindInternal, err)
		}
		creationFound := false
		for _, task := range tasks {
			if !task.IsDir() || ids.Validate(ids.KindTask, task.Name()) != nil {
				return latest, false, corruptJournal()
			}
			progress, exists, err := readLongestChain(filepath.Join(root, directory.Name(), task.Name()))
			if err != nil {
				return latest, false, err
			}
			if !exists {
				continue // Begin created the directory but issued no host effect.
			}
			if progress.RunnerID != runnerID || progress.RuntimeEpoch != epoch || progress.TaskID != task.Name() {
				return latest, false, corruptJournal()
			}
			if progress.Operation != runnerallocation.RuntimeOperationCreate {
				continue
			}
			if creationFound {
				return latest, false, corruptJournal()
			}
			creationFound = true
			if latest.RuntimeEpoch < epoch {
				latest = progress
			}
		}
	}
	latest.Plan = latest.Plan.Clone()
	return latest, latest.RuntimeEpoch != 0, nil
}
