package attachwork

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Diagnostic loss must never turn an acknowledged command into a retry.
func (service *Reports) reportProgress(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.ProvisionInput, progress attachexecution.ProvisionProgress,
) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	attempt := view.Attempt.Attempt
	authority, err := input.Authority()
	if err == nil {
		begin, stage, stageErr := attachexecution.ProvisionDiagnosticStages(attempt, authority, progress)
		err = stageErr
		if err == nil && !progress.InFlight {
			snapshot, readErr := service.diagnostics.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, 0)
			err = readErr
			if err == nil && (snapshot.Record == nil || snapshot.Record.Sequence < progress.Sequence-1) {
				err = service.diagnostics.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID,
					attempt.Executor, progress.Sequence-1, begin, *begin.StartedAt)
			}
		}
		if err == nil {
			err = service.diagnostics.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID,
				attempt.Executor, progress.Sequence, stage, progress.UpdatedAt)
		}
	}
	if err != nil {
		slog.WarnContext(ctx, "Attach execution diagnostics incomplete", "attempt_id", attempt.ID, "error", err)
		if lossErr := service.diagnostics.MarkIncomplete(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor); lossErr != nil {
			slog.WarnContext(ctx, "could not record Attach diagnostic loss", "attempt_id", attempt.ID, "error", lossErr)
		}
	}
}
