package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type FinishedWork struct {
	Task         TaskSnapshot
	Items        []ItemSnapshot
	ReadRevision int64
}

// ReadFinishedWork reads the complete bounded membership at one revision. The
// caller must compare Task.Revision when publishing aggregate settlement: every
// work transition advances that revision, so no per-item transaction expansion
// is needed for a large Blueprint. Diagnostics are not completion evidence.
func (repository *Repository) ReadFinishedWork(ctx context.Context, taskID string) (FinishedWork, error) {
	task, revision, err := repository.ReadTask(ctx, taskID, 0)
	if err != nil {
		return FinishedWork{}, err
	}
	result := FinishedWork{Task: task, ReadRevision: revision}
	if task.Task.FinishedAt != nil {
		return result, nil
	}
	if !task.Task.PublicationComplete || task.Task.Progress.Unsettled() != 0 || task.Task.CollectionStartedAt != nil {
		return FinishedWork{}, errs.New(errs.KindStateConflict, "task still has unpublished or unsettled work")
	}
	counts := workexecution.Progress{}
	after := ""
	for {
		page, err := repository.ListWork(ctx, taskID, after, 100, revision)
		if err != nil {
			return FinishedWork{}, err
		}
		if len(result.Items)+len(page.Items) > int(task.Task.ExpectedItems) {
			return FinishedWork{}, corrupt()
		}
		for _, snapshot := range page.Items {
			item := snapshot.Item
			if item.Phase != workexecution.Settled || item.Result == nil || item.ActiveAttemptID != "" {
				return FinishedWork{}, corrupt()
			}
			switch item.Outcome {
			case workexecution.Succeeded:
				counts.Succeeded++
			case workexecution.Failed:
				counts.Failed++
			case workexecution.Cancelled:
				counts.Cancelled++
			case workexecution.Superseded:
				counts.Superseded++
			default:
				return FinishedWork{}, corrupt()
			}
			result.Items = append(result.Items, snapshot)
		}
		if !page.More {
			break
		}
		if len(page.Items) == 0 || page.LastID <= after {
			return FinishedWork{}, corrupt()
		}
		after = page.LastID
	}
	if len(result.Items) != int(task.Task.ExpectedItems) || counts != task.Task.Progress {
		return FinishedWork{}, corrupt()
	}
	return result, nil
}
