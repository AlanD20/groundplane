package taskcheckpoint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
)

func (service *ReleaseCheckpointService) reportDeployment(ctx context.Context, input releaseexecution.Input, previous releasework.Snapshot, report releaseexecution.CheckpointReport) {
	var before *releaseexecution.Progress
	if previous.Record != nil {
		before = &previous.Record.Progress
	}
	stage, err := releaseexecution.DeploymentStage(input, before, report.Progress)
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	attempt := input.Attempt
	needsStart := false
	if err == nil && !report.Progress.InFlight && before != nil {
		snapshot, readErr := service.diagnostics.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, 0)
		err = readErr
		needsStart = snapshot.Record == nil || snapshot.Record.Sequence < report.Progress.Sequence
	}
	if err == nil && needsStart {
		// If Controller stopped after saving Begin but before publishing its
		// diagnostic, Confirm can repair that one missing observation using
		// the saved boundary. Exact diagnostic replay is idempotent.
		started, startErr := releaseexecution.DeploymentStage(input, before, *before)
		if startErr == nil {
			startErr = service.diagnostics.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor,
				before.Sequence, started, before.UpdatedAt)
		}
		err = startErr
	}
	if err == nil {
		// Progress counts deployment boundaries only. The shared checkpoint
		// sequence also includes hook reports and is not a diagnostic sequence.
		err = service.diagnostics.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor,
			report.Progress.Sequence, stage, report.Progress.UpdatedAt)
	}
	if err != nil {
		service.logger.WarnContext(ctx, "deployment diagnostics are incomplete", "attempt_id", attempt.ID, "error", err)
		if lossErr := service.diagnostics.MarkIncomplete(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor); lossErr != nil {
			service.logger.WarnContext(ctx, "could not record deployment diagnostic loss", "attempt_id", attempt.ID, "error", lossErr)
		}
	}
}
