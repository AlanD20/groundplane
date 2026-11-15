package taskcheckpoint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
)

func (service *ReleaseCheckpointService) reportDeployment(
	ctx context.Context,
	input releaseexecution.Input,
	previous releasework.Snapshot,
	report releaseexecution.CheckpointReport,
) {
	var before *releaseexecution.Progress
	if previous.Record != nil {
		before = &previous.Record.Progress
	}
	stage, err := releaseexecution.DeploymentStage(input, before, report.Progress)
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	attempt := input.Attempt
	var started *workexecution.Stage
	if err == nil && !report.Progress.InFlight && before != nil {
		value, startErr := releaseexecution.DeploymentStage(input, before, *before)
		if startErr == nil {
			started = &value
		}
		err = startErr
	}
	if err == nil {
		err = service.reportDiagnosticStage(bounded, attempt, stage, started, report.Progress.UpdatedAt)
	}
	if err != nil {
		service.logger.WarnContext(ctx, "deployment diagnostics are incomplete", "attempt_id", attempt.ID, "error", err)
		if lossErr := service.diagnostics.MarkIncomplete(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor); lossErr != nil {
			service.logger.WarnContext(
				ctx,
				"could not record deployment diagnostic loss",
				"attempt_id",
				attempt.ID,
				"error",
				lossErr,
			)
		}
	}
}
