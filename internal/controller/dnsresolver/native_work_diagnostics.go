package dnsresolver

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (service *WorkReports) reportProgress(
	ctx context.Context,
	view workqueue.ResumeView,
	input resolverexecution.Input,
	layout resolverexecution.Layout,
	progress resolverexecution.Progress,
) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	attempt, begin := view.Attempt.Attempt, progress
	var err error
	if !progress.InFlight {
		begin, err = service.records.ReadProgress(bounded, view, input, progress.Sequence-1)
		if err == nil {
			snapshot, readErr := service.diagnostics.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, 0)
			err = readErr
			if err == nil && (snapshot.Record == nil || snapshot.Record.Sequence < begin.Sequence) {
				stage, stageErr := resolverexecution.DiagnosticStage(attempt, layout, begin, begin)
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
	}
	if err == nil {
		stage, stageErr := resolverexecution.DiagnosticStage(attempt, layout, begin, progress)
		err = stageErr
		if err == nil {
			err = service.diagnostics.Report(
				bounded,
				attempt.TaskID,
				attempt.WorkItemID,
				attempt.ID,
				attempt.Executor,
				progress.Sequence,
				stage,
				progress.UpdatedAt,
			)
		}
	}
	service.diagnosticLoss(ctx, bounded, attempt, err)
}

func (service *WorkReports) reportTerminal(ctx context.Context, view workqueue.ResumeView, kind string) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	attempt := view.Attempt.Attempt
	stage := workexecution.Stage{
		Key:        "resolver_result",
		Name:       "Publish CoreDNS result",
		StartedAt:  attempt.StartedAt,
		FinishedAt: attempt.FinishedAt,
		Expected:   "Publish proven applied DNS state, or retain the original failure independently of cleanup",
	}
	sequence := uint64(1)
	if cursor := view.Checkpoint.Record; cursor != nil {
		sequence = cursor.Sequence + 1
	}
	if kind == "recovery" {
		stage.Key, stage.Name = "restore_resolver", "Restore previous DNS resolver"
		stage.Expected = "Join earlier helpers and restore the captured runtime, Corefile and host resolver; prove the previous resolver serving or its captured absence"
	}
	if attempt.StartedAt == nil {
		stage.State, stage.Observed = workexecution.StageSkipped, "Execution did not start"
	} else if kind == "completion" || kind == "recovery" && attempt.Outcome == workexecution.Succeeded {
		stage.State, stage.Observed = workexecution.StageSucceeded, "The captured resolver state was verified and published"
		if kind == "recovery" {
			stage.Observed = "The captured previous resolver and host DNS configuration were restored; the original action remains unsuccessful"
		}
	} else {
		stage.State, stage.FailureCode = workexecution.StageFailed, string(attempt.Outcome)
		stage.Observed = "The resolver action did not complete; inspect its recorded execution stages and separate cleanup result"
		if kind == "recovery" {
			stage.Observed = "Restoration is not proven complete; resource protection remains held and explicit Retry is required"
		}
	}
	if kind == "failure" && view.Checkpoint.Record != nil && attempt.StartedAt != nil {
		stored, err := service.records.Load(bounded, view.Work)
		if err == nil {
			layout, layoutErr := resolverexecution.CaptureLayout(attempt, stored.Input)
			progress, progressErr := service.records.ReadProgress(
				bounded,
				view,
				stored.Input,
				view.Checkpoint.Record.Sequence,
			)
			if layoutErr == nil && progressErr == nil && progress.InFlight {
				active, activeErr := resolverexecution.DiagnosticStage(attempt, layout, progress, progress)
				if activeErr == nil {
					active.State, active.FinishedAt, active.FailureCode = stage.State, stage.FinishedAt, stage.FailureCode
					active.Observed = stage.Observed
					stage = active
				}
			}
		}
	}
	service.diagnosticLoss(ctx, bounded, attempt, service.diagnostics.ReportResult(bounded, attempt, sequence, stage))
}

func (service *WorkReports) diagnosticLoss(ctx, bounded context.Context, attempt workexecution.Attempt, err error) {
	if err == nil {
		return
	}
	slog.WarnContext(ctx, "CoreDNS execution diagnostics incomplete", "attempt_id", attempt.ID, "error", err)
	if lossErr := service.diagnostics.MarkIncomplete(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor); lossErr != nil {
		slog.WarnContext(ctx, "could not record CoreDNS diagnostic loss", "attempt_id", attempt.ID, "error", lossErr)
	}
}
