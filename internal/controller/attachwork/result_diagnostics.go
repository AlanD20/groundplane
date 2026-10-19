package attachwork

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

func (service *Reports) reportForwardResult(ctx context.Context, identity workexecution.Attempt, bindingReady bool) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	view, err := service.queue.ReadResume(bounded, identity.TaskID, identity.WorkItemID, identity.ID, identity.Executor)
	if err == nil {
		attempt := view.Attempt.Attempt
		if bindingReady {
			_, err = service.inputs.repository.LoadResult(bounded, view)
			if err == nil {
				service.reportResultStage(bounded, attempt, workexecution.Stage{
					Key: "provision-result", Name: "Complete Attach provisioning",
					Expected: "Complete provisioning and publish the ready connection with its captured facts",
					State:    workexecution.StageSucceeded, StartedAt: attempt.StartedAt, FinishedAt: attempt.FinishedAt,
					Observed: "Provisioning completed and the ready Attach binding and facts were stored. The Task outcome separately records any cancellation or supersession.",
				})
			}
		} else {
			var raw []byte
			raw, err = service.inputs.repository.LoadFailure(bounded, view)
			if err == nil {
				var failure attachexecution.ProvisionFailure
				failure, err = attachexecution.DecodeProvisionFailure(attempt, raw, *attempt.Result)
				if err == nil {
					service.reportFailure(bounded, attempt, failure)
				}
			}
		}
	}
	if err != nil {
		slog.WarnContext(ctx, "Attach terminal diagnostics incomplete", "attempt_id", identity.ID, "error", err)
	}
}

func (service *Reports) reportFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
	failure attachexecution.ProvisionFailure,
) {
	stage := workexecution.Stage{Key: "provision-result", Name: "Complete Attach provisioning",
		Expected: "Complete provisioning and publish the ready connection with its captured facts",
		State:    workexecution.StageFailed, StartedAt: attempt.StartedAt, FinishedAt: attempt.FinishedAt,
		FailureCode: string(failure.Reason)}
	switch failure.Reason {
	case attachexecution.DeadlineExpired:
		stage.Observed = "Provisioning exceeded its execution deadline."
	case attachexecution.ExecutionInterrupted:
		stage.Observed = "The Agent execution was interrupted before provisioning completed."
	default:
		stage.Observed = "Provisioning did not complete successfully."
	}
	if failure.Checkpoint == nil {
		stage.Observed += " No provisioning command was authorized."
	} else {
		stage.Observed += " Inspection must establish whether the authorized command stopped; existing credentials and database effects are retained."
	}
	if attempt.StartedAt == nil {
		stage.State, stage.FailureCode = workexecution.StageSkipped, ""
	}
	service.reportResultStage(ctx, attempt, stage)
}

func (service *Reports) reportInspection(
	ctx context.Context,
	attempt workexecution.Attempt,
	report attachexecution.ProvisionInspection,
) {
	stage := workexecution.Stage{Key: "inspect-result", Name: "Inspect interrupted provisioning",
		Expected: "Determine whether the captured provisioning command stopped without replaying it or deleting its effects",
		State:    workexecution.StageFailed, StartedAt: attempt.StartedAt, FinishedAt: attempt.FinishedAt}
	switch report.State {
	case attachexecution.ProvisionConsumerInspected:
		stage.Name = "Inspect interrupted Service connection"
		stage.Observed = "The prior network helper stopped; not all captured workloads are connected."
		if report.Consumer != nil && report.Consumer.Connected {
			stage.Observed = "The prior network helper stopped; all captured workloads are connected."
		}
	case attachexecution.ProvisionCommandCompleted:
		stage.Observed = "The command already has an acknowledged successful result."
	case attachexecution.CommandNotCreated:
		stage.Observed = "The journal confirms that no command process was created."
	case attachexecution.CommandNotPermitted:
		stage.Observed = "The captured process was never authorized to start."
	case attachexecution.CommandExited:
		stage.Observed = "The captured process exited with code " + strconv.Itoa(*report.ExitCode) + "."
	case attachexecution.CommandRunning:
		stage.Observed = "The captured process is still running; its resource protection remains held."
	case attachexecution.CommandJournalLost:
		stage.Observed = "The execution journal is unavailable; process termination cannot be established."
	case attachexecution.CommandUnobservable:
		stage.Observed = "The captured process could not be inspected; termination remains unproven."
	case attachexecution.InspectionInterrupted:
		stage.Observed = "Inspection was interrupted before process termination could be established."
	}
	if report.ExecutionStopped() {
		stage.State = workexecution.StageSucceeded
		stage.Observed += " The original Attach remains failed; credentials and database changes were not rolled back."
	}
	if attempt.StartedAt == nil {
		stage.State = workexecution.StageSkipped
	}
	service.reportResultStage(ctx, attempt, stage)
}

func (service *Reports) reportResultStage(
	ctx context.Context,
	attempt workexecution.Attempt,
	stage workexecution.Stage,
) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	snapshot, err := service.diagnostics.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, 0)
	sequence := uint64(1)
	if err == nil && snapshot.Record != nil {
		for _, recorded := range snapshot.Record.Stages {
			if recorded.Key == stage.Key {
				return // An acknowledgement replay cannot rewrite its diagnostic result.
			}
		}
		sequence = snapshot.Record.Sequence + 1
	}
	if err == nil {
		err = service.diagnostics.ReportResult(bounded, attempt, sequence, stage)
	}
	if err != nil {
		slog.WarnContext(ctx, "Attach result diagnostics incomplete", "attempt_id", attempt.ID, "error", err)
	}
}
