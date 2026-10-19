package taskinspection

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ImageDetails exposes safe, typed operation evidence, not captured executable
// input or a reconstruction from Task log text. Progress totals cover reported
// layers only; clients must not present them as whole-operation percentages.
type ImageDetails struct {
	TaskID, WorkItemID, AttemptID string
	Requested, Manifest, ImageID  string
	Platform                      string
	Phase                         imageoperations.Phase
	UncertainPhase                imageoperations.Phase
	Outcome                       workexecution.Outcome
	Summary                       string
	Progress                      *imagefetch.Progress
	ProgressUnavailable           bool
	UpdatedAt                     time.Time
	FinishedAt                    *time.Time
	Revision                      int64
}

func (service *Service) Image(ctx context.Context, taskID, workID string, revision int64) (ImageDetails, error) {
	task, readRevision, err := service.queue.ReadTask(ctx, taskID, revision)
	if err != nil {
		return ImageDetails{}, err
	}
	if task.Task.CollectionStartedAt != nil {
		return ImageDetails{}, errs.New(errs.KindRequestNotFound, "image execution details have expired")
	}
	inspection, err := service.images.Inspect(ctx, taskID, readRevision)
	if err != nil {
		return ImageDetails{}, err
	}
	record := inspection.Operation
	if record.Input.WorkItemID != workID || record.Input.Action != task.Task.Action ||
		record.Input.Target() != task.Task.TargetID {
		return ImageDetails{}, errs.New(errs.KindRequestNotFound, "image work does not belong to this Task")
	}
	result := ImageDetails{TaskID: taskID, WorkItemID: workID, AttemptID: record.AttemptID,
		ImageID: record.Input.Target(), Phase: record.Phase, UncertainPhase: record.UncertainPhase,
		Outcome: record.Outcome, Summary: imageSummary(record), UpdatedAt: record.UpdatedAt,
		FinishedAt: record.FinishedAt, Revision: readRevision}
	if plan := record.Input.Fetch; plan != nil {
		result.Requested, result.Manifest = plan.Requested, plan.Reference()
		result.Platform = "linux/" + plan.Architecture
		if plan.Variant != "" {
			result.Platform += "/" + plan.Variant
		}
		result.ProgressUnavailable = !inspection.ProgressAvailable
		if inspection.ProgressAvailable {
			progress := inspection.Progress
			result.Progress = &progress
		}
	}
	return result, nil
}

func imageSummary(record imageoperations.Record) string {
	switch record.Phase {
	case imageoperations.Ready:
		return "Waiting for an executor; no image operation has started"
	case imageoperations.Fetching:
		return "Fetching the accepted image content from its registry"
	case imageoperations.Tagging:
		return "Image content is present; assigning the requested local tag"
	case imageoperations.Removing:
		return "Checking image protections and removing the local image"
	case imageoperations.Uncertain:
		if record.UncertainPhase == imageoperations.Tagging {
			return "Tag assignment outcome is unknown; the tag remains protected against conflicting writes"
		}
		if record.UncertainPhase == imageoperations.Removing {
			return "Removal outcome is unknown; image protection remains held until the Docker operation is reconciled"
		}
		return "Image fetch outcome is unknown; execution requires reconciliation"
	case imageoperations.Finished:
		switch record.Outcome {
		case workexecution.Succeeded:
			if record.Input.Action == workexecution.ActionRemove {
				return "Local image removal completed"
			}
			return "Accepted image content is available locally"
		case workexecution.Cancelled:
			return "Image operation was cancelled; any downloaded content may remain cached"
		case workexecution.Superseded:
			return "Image operation was superseded by newer work"
		case workexecution.Failed:
			return "Image operation failed; inspect the recorded failure details"
		}
	}
	return "Image execution details are unavailable"
}
