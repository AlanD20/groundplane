package network

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Diagnostic loss never fails checkpoint acknowledgement or permits an effect
// replay. Only the accepted report is execution authority.
func (service *WorkReports) reportProgress(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.NetworkWorkInput, progress networkexecution.Progress,
) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	attempt := view.Attempt.Attempt
	begin, stage, err := service.records.NetworkDiagnosticStages(bounded, view, input, progress)
	if err == nil {
		snapshot, readErr := service.diagnostics.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, 0)
		err = readErr
		if err == nil && snapshot.Expired {
			return
		}
		if err == nil && snapshot.Record != nil && snapshot.Record.Sequence > progress.Sequence {
			return // A replayed begin cannot replace an already recorded result.
		}
		if err == nil && !progress.InFlight && snapshot.Record == nil {
			err = service.diagnostics.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID,
				attempt.Executor, 1, begin, *begin.StartedAt)
		}
		if err == nil {
			err = service.diagnostics.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID,
				attempt.Executor, progress.Sequence, stage, progress.UpdatedAt)
		}
	}
	if err != nil {
		slog.WarnContext(ctx, "Network execution diagnostics incomplete", "attempt_id", attempt.ID, "error", err)
		if lossErr := service.diagnostics.MarkIncomplete(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor); lossErr != nil {
			slog.WarnContext(
				ctx,
				"could not record Network diagnostic loss",
				"attempt_id",
				attempt.ID,
				"error",
				lossErr,
			)
		}
	}
}
