package servicelifecycle

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Diagnostic storage never gates an acknowledged effect or authorizes replay.
func (service *WorkReports) reportProgress(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.ServiceLifecycleWorkInput, progress lifecycleexecution.Progress,
) {
	if progress.BeganAt == nil && len(progress.Completed) == progress.Inherited {
		return // An inherited prefix is not a newly executed action.
	}
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	attempt := view.Attempt.Attempt
	sequence := progress.Sequence
	if progress.Inherited > 0 {
		sequence-- // The inherited-prefix acknowledgement did not execute a stage.
	}
	begin, stage, err := service.records.LifecycleDiagnosticStages(bounded, view, input, progress)
	if err == nil {
		snapshot, readErr := service.diagnostics.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, 0)
		err = readErr
		if err == nil && (snapshot.Expired || snapshot.Record != nil && snapshot.Record.Sequence > sequence) {
			return
		}
		if err == nil && progress.BeganAt == nil && (snapshot.Record == nil || snapshot.Record.Sequence < sequence-1) {
			err = service.diagnostics.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID,
				attempt.Executor, sequence-1, begin, *begin.StartedAt)
		}
		if err == nil {
			err = service.diagnostics.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID,
				attempt.Executor, sequence, stage, progress.UpdatedAt)
		}
	}
	service.diagnosticLoss(bounded, attempt, err)
}

func (service *WorkReports) reportResult(ctx context.Context, attempt workexecution.Attempt,
	stage workexecution.Stage,
) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	snapshot, err := service.diagnostics.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, 0)
	if err == nil && snapshot.Expired {
		return
	}
	if err == nil {
		sequence := uint64(1)
		if record := snapshot.Record; record != nil {
			sequence = record.Sequence + 1
			if record.Stages[len(record.Stages)-1].Key == stage.Key {
				sequence = record.Sequence // Exact terminal-report replay.
			}
		}
		err = service.diagnostics.ReportResult(bounded, attempt, sequence, stage)
	}
	service.diagnosticLoss(bounded, attempt, err)
}

func (service *WorkReports) diagnosticLoss(ctx context.Context, attempt workexecution.Attempt, err error) {
	if err == nil {
		return
	}
	slog.WarnContext(ctx, "Service lifecycle diagnostics incomplete", "attempt_id", attempt.ID, "error", err)
	if markErr := service.diagnostics.MarkIncomplete(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor); markErr != nil {
		slog.WarnContext(
			ctx,
			"could not record Service lifecycle diagnostic loss",
			"attempt_id",
			attempt.ID,
			"error",
			markErr,
		)
	}
}
