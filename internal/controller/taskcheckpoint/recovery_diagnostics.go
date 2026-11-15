package taskcheckpoint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
)

func (service *ReleaseCheckpointService) reportRecovery(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	previous releasework.RecoverySnapshot,
	report releaseexecution.RecoveryReport,
) {
	var before *releaseexecution.RecoveryReport
	if previous.Record != nil {
		before = &previous.Record.Report
		// A confirmation replay has already replaced the active cursor. Its
		// retained authorization boundary still supplies the actual start time.
		if !report.Progress.InFlight &&
			releaseexecution.EqualRecoveryProgress(previous.Record.Report.Progress, report.Progress) {
			index := report.Progress.Sequence/2 - 1
			if index < uint64(len(previous.Record.Boundaries)) {
				before = previous.Record.Boundaries[index]
			}
		}
	}
	stage, err := releaseexecution.RecoveryStage(input, before, report)
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	attempt := input.Input.Attempt
	var started *workexecution.Stage
	if err == nil && !report.Progress.InFlight && before != nil {
		value, startErr := releaseexecution.RecoveryStage(input, nil, *before)
		if startErr == nil {
			started = &value
		}
		err = startErr
	}
	if err == nil {
		err = service.reportDiagnosticStage(bounded, attempt, stage, started, report.Progress.UpdatedAt)
	}
	if err != nil {
		service.logger.WarnContext(ctx, "recovery diagnostics are incomplete", "attempt_id", attempt.ID, "error", err)
		if lossErr := service.diagnostics.MarkIncomplete(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor); lossErr != nil {
			service.logger.WarnContext(
				ctx,
				"could not record recovery diagnostic loss",
				"attempt_id",
				attempt.ID,
				"error",
				lossErr,
			)
		}
	}
}
