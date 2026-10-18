package taskcheckpoint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
)

func (service *ReleaseCheckpointService) reportRecovery(ctx context.Context, input releaseexecution.RecoveryInput, previous releasework.RecoverySnapshot, report releaseexecution.RecoveryReport) {
	var before *releaseexecution.RecoveryReport
	if previous.Record != nil {
		before = &previous.Record.Report
		// A confirmation replay has already replaced the active cursor. Its
		// retained authorization boundary still supplies the actual start time.
		if !report.Progress.InFlight && releaseexecution.EqualRecoveryProgress(previous.Record.Report.Progress, report.Progress) {
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
	needsStart := false
	if err == nil && !report.Progress.InFlight && before != nil {
		snapshot, readErr := service.diagnostics.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, 0)
		err = readErr
		needsStart = snapshot.Record == nil || snapshot.Record.Sequence < report.Progress.Sequence
	}
	if err == nil && needsStart {
		// Recreate only a retained, real Begin observation, never an inferred
		// time or effect. A failed diagnostic write must not veto settlement.
		started, startErr := releaseexecution.RecoveryStage(input, nil, *before)
		if startErr == nil {
			startErr = service.diagnostics.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor,
				before.Progress.Sequence, started, before.Progress.UpdatedAt)
		}
		err = startErr
	}
	if err == nil {
		err = service.diagnostics.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor,
			report.Progress.Sequence, stage, report.Progress.UpdatedAt)
	}
	if err != nil {
		service.logger.WarnContext(ctx, "recovery diagnostics are incomplete", "attempt_id", attempt.ID, "error", err)
		if lossErr := service.diagnostics.MarkIncomplete(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor); lossErr != nil {
			service.logger.WarnContext(ctx, "could not record recovery diagnostic loss", "attempt_id", attempt.ID, "error", lossErr)
		}
	}
}
