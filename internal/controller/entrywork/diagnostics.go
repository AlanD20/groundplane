package entrywork

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (service *WorkReports) reportProgress(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.EntryConfigurationWorkInput, progress entryexecution.Progress,
) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	attempt := view.Attempt.Attempt
	begin, err := service.records.DiagnosticBegin(bounded, view, input, progress)
	if err == nil && !progress.InFlight {
		// Repair one missed Begin diagnostic from its durable checkpoint. Never
		// invent earlier stages when diagnostic retention or delivery lost them.
		snapshot, readErr := service.diagnostics.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, 0)
		err = readErr
		if err == nil && (snapshot.Record == nil || snapshot.Record.Sequence < begin.Sequence) {
			stage, stageErr := entryexecution.DiagnosticStage(attempt, input, begin, begin)
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
		stage, stageErr := entryexecution.DiagnosticStage(attempt, input, begin, progress)
		err = stageErr
		if err == nil {
			err = service.diagnostics.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor,
				progress.Sequence, stage, progress.UpdatedAt)
		}
	}
	if err != nil {
		slog.WarnContext(ctx, "Entry execution diagnostics incomplete", "attempt_id", attempt.ID, "error", err)
		if lossErr := service.diagnostics.MarkIncomplete(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor); lossErr != nil {
			slog.WarnContext(ctx, "could not record Entry diagnostic loss", "attempt_id", attempt.ID, "error", lossErr)
		}
	}
}
