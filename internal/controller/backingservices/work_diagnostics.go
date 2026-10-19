package backingservices

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (service *WorkReports) reportProgress(ctx context.Context, view workqueue.ResumeView,
	layout backingprovision.Layout, progress backingprovision.Progress,
) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	attempt := view.Attempt.Attempt
	begin, err := service.records.DiagnosticBegin(bounded, view, layout, progress)
	if err == nil && !progress.InFlight {
		snapshot, readErr := service.diagnostics.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, 0)
		err = readErr
		if err == nil && (snapshot.Record == nil || snapshot.Record.Sequence < begin.Sequence) {
			stage, stageErr := backingprovision.DiagnosticStage(attempt, layout, begin, begin)
			err = stageErr
			if err == nil {
				err = service.diagnostics.Report(
					bounded,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
					begin.Sequence,
					stage,
					begin.UpdatedAt,
				)
			}
		}
	}
	if err == nil {
		stage, stageErr := backingprovision.DiagnosticStage(attempt, layout, begin, progress)
		err = stageErr
		if err == nil {
			err = service.diagnostics.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor,
				progress.Sequence, stage, progress.UpdatedAt)
		}
	}
	service.diagnosticLoss(ctx, bounded, attempt, err)
}

func (service *WorkReports) reportTerminal(ctx context.Context, view workqueue.ResumeView, cleanup bool) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	attempt := view.Attempt.Attempt
	stage := workexecution.Stage{Key: "creation_result", Name: "Publish Backing creation result",
		Expected:  "Publish the verified runtime or retain the creation failure independently of cleanup",
		StartedAt: attempt.StartedAt, FinishedAt: attempt.FinishedAt}
	sequence := uint64(1)
	if cursor := view.Checkpoint.Record; cursor != nil {
		sequence = cursor.Sequence + 1
	}
	if cleanup {
		stage.Key, stage.Name = "cleanup_runtime", "Remove failed creation runtime"
		stage.Expected = "Join the captured helper and file writers, remove only the captured container and verify its absence; preserve storage data"
	}
	if attempt.StartedAt == nil {
		stage.State, stage.Observed = workexecution.StageSkipped, "Execution did not start"
	} else if attempt.Outcome == workexecution.Succeeded {
		stage.State, stage.Observed = workexecution.StageSucceeded, "Verified Backing runtime was published"
		if cleanup {
			stage.Observed = "Captured helpers and file writers stopped; captured runtime is absent; storage data was preserved"
		}
	} else if !cleanup && attempt.Result != nil && attempt.Result.ID == attempt.Input.ID {
		stage.State = workexecution.StageSucceeded
		stage.Observed = "Captured runtime was verified and published; the action was " + string(attempt.Outcome) + " after its authorized effects"
	} else {
		stage.State, stage.FailureCode = workexecution.StageFailed, string(attempt.Outcome)
		stage.Observed = "Creation did not complete; inspect the recorded execution stages and separate cleanup result"
		if cleanup {
			stage.Observed = "Cleanup is not proven complete; resource protection remains held and operator Retry is required"
		}
	}
	err := service.diagnostics.ReportResult(bounded, attempt, sequence, stage)
	service.diagnosticLoss(ctx, bounded, attempt, err)
}

func (service *WorkReports) diagnosticLoss(ctx, bounded context.Context, attempt workexecution.Attempt, err error) {
	if err == nil {
		return
	}
	slog.WarnContext(ctx, "Backing execution diagnostics incomplete", "attempt_id", attempt.ID, "error", err)
	if lossErr := service.diagnostics.MarkIncomplete(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor); lossErr != nil {
		slog.WarnContext(ctx, "could not record Backing diagnostic loss", "attempt_id", attempt.ID, "error", lossErr)
	}
}
