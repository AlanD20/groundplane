package taskinspection

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) environmentOutcome(ctx context.Context, attempt workexecution.Attempt, result *Stages) error {
	view, err := service.queue.ReadWork(ctx, attempt.TaskID, attempt.WorkItemID, result.Revision)
	if err != nil {
		return err
	}
	if view.Item.Item.Operation != workexecution.Environment {
		return nil
	}
	capture, _, err := service.environments.Load(ctx, view)
	if err != nil {
		return err
	}
	directory := workexecution.Stage{Key: "environment_directory", Name: "Provision Environment directory",
		Expected: "Verify the captured root-owned Environment directory", State: workexecution.StageWaiting}
	if len(capture.Environment.VolumeDir) <= 900 {
		directory.Expected = "Verify the root-owned directory at " + capture.Environment.VolumeDir
	}
	if attempt.Direction == workexecution.Recovery {
		directory.Name = "Inspect interrupted Environment provisioning"
		directory.Expected = "Stop only the captured helper and inspect its directory without repeating creation"
	}
	if attempt.StartedAt != nil {
		directory.State, directory.StartedAt = workexecution.StageRunning, attempt.StartedAt
		directory.Observed = "The Agent has started execution; the final directory observation has not been recorded."
	}
	if attempt.FinishedAt == nil {
		return appendComponentOutcome(result, directory)
	}
	receipt, err := service.environments.ReadResult(ctx, attempt, result.Revision)
	if err != nil {
		return err
	}
	report := receipt.Record.Report
	evidence, err := report.Reference(attempt)
	if err != nil {
		return err
	}
	if attempt.Result == nil || evidence != *attempt.Result {
		return errs.New(errs.KindStateConflict, "Environment directory observation differs from its accepted result")
	}
	directory.FinishedAt, directory.State = attempt.FinishedAt, workexecution.StageFailed
	if report.DirectoryReady {
		directory.Observed = "The captured Environment directory exists and passed descriptor-relative ownership checks."
		directory.State = workexecution.StageSucceeded
	} else {
		directory.Observed = "Directory readiness was not proved; the Environment is not marked ready."
	}
	if attempt.StartedAt == nil {
		directory.State, directory.Observed = workexecution.StageSkipped, "The directory helper was never authorized. No directory effect started under this attempt."
	}
	if report.Failure != "" {
		if attempt.StartedAt != nil {
			directory.State, directory.FailureCode = workexecution.StageFailed, "execution.failed"
		}
		directory.Observed += " " + report.Failure
	}
	if attempt.Direction == workexecution.Recovery && report.HelpersAbsent {
		directory.State, directory.FailureCode = workexecution.StageSucceeded, ""
		directory.Observed += " Inspection settled the retained effects; the original action failure is preserved."
	}
	if err := appendComponentOutcome(result, directory); err != nil {
		return err
	}
	helpers := workexecution.Stage{Key: "environment_helpers", Name: "Settle provisioning helper", Expected: "Prove the captured helper is stopped and removed",
		StartedAt: attempt.StartedAt, FinishedAt: attempt.FinishedAt, State: workexecution.StageFailed}
	if report.HelpersAbsent || attempt.StartedAt == nil {
		helpers.State, helpers.Observed = workexecution.StageSucceeded, "No captured provisioning helper remains. Unrelated workloads and Volume data were not removed."
		if attempt.StartedAt == nil {
			helpers.State, helpers.Observed = workexecution.StageSkipped, "The helper was never authorized, so no cleanup was required."
		}
	} else {
		helpers.FailureCode, helpers.Observed = "execution.cleanup_required", report.CleanupFailure+". Retry inspects this captured operation; it does not repeat directory creation."
	}
	return appendComponentOutcome(result, helpers)
}
