package runner

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
)

type runtimeStage struct {
	records  *executiondiagnostics.Repository
	attempt  Attempt
	stage    workexecution.Stage
	sequence uint64
}

func (lifecycle *Lifecycle) beginStage(ctx context.Context, attempt Attempt, step runnerallocation.RunnerRuntimeStep) *runtimeStage {
	name, expected := runnerStageText(step)
	now := time.Now().UTC()
	report := &runtimeStage{records: lifecycle.diagnostics, attempt: attempt,
		stage: workexecution.Stage{Key: string(step), Name: name, Expected: expected, State: workexecution.StageRunning, StartedAt: &now}}
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	snapshot, err := report.records.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.AttemptID, 0)
	if err != nil {
		report.lost(bounded, err)
		return report
	}
	if snapshot.Record != nil {
		report.sequence = snapshot.Record.Sequence
		for _, previous := range snapshot.Record.Stages {
			if previous.Key == report.stage.Key && previous.StartedAt != nil {
				report.stage.StartedAt = previous.StartedAt
			}
		}
	}
	report.write(bounded, now)
	return report
}

func (report *runtimeStage) finish(ctx context.Context, succeeded bool) {
	now := time.Now().UTC()
	if succeeded {
		report.stage.State, report.stage.FinishedAt = workexecution.StageSucceeded, &now
		report.stage.Observed = "The expected host state was observed and its checkpoint saved."
	} else {
		report.stage.State = workexecution.StageUnknown
		report.stage.Observed = "This step ended without a saved completion checkpoint. Its host effects must be reconciled before further work."
	}
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	report.write(bounded, now)
}

func (report *runtimeStage) write(ctx context.Context, at time.Time) {
	report.sequence++
	if err := report.records.Report(ctx, report.attempt.TaskID, report.attempt.WorkItemID, report.attempt.AttemptID,
		report.attempt.Executor, report.sequence, report.stage, at); err != nil {
		report.lost(ctx, err)
	}
}

func (report *runtimeStage) lost(ctx context.Context, err error) {
	slog.WarnContext(ctx, "Runner stage diagnostics incomplete", "attempt_id", report.attempt.AttemptID, "error", err)
	if err := report.records.MarkIncomplete(ctx, report.attempt.TaskID, report.attempt.WorkItemID, report.attempt.AttemptID, report.attempt.Executor); err != nil {
		slog.WarnContext(ctx, "could not record Runner diagnostic loss", "attempt_id", report.attempt.AttemptID, "error", err)
	}
}

func runnerStageText(step runnerallocation.RunnerRuntimeStep) (string, string) {
	switch step {
	case runnerallocation.StepEnsureIdentity:
		return "Prepare Runner host identity", "Create the captured host identity and owned runtime directories"
	case runnerallocation.StepEnsureNetwork:
		return "Create Runner network", "Create the reserved network with the captured ownership and address allocation"
	case runnerallocation.StepEnsureEgress:
		return "Configure Runner egress", "Install the captured outbound network rules"
	case runnerallocation.StepStartDaemon:
		return "Start dedicated Docker daemon", "Start and identify the Runner's isolated Docker daemon"
	case runnerallocation.StepStartRunner:
		return "Register and start GitHub Runner", "Register the Runner using its one-time token and verify container ownership"
	case runnerallocation.StepStopRunner:
		return "Remove Runner container", "Verify the owned Runner container is absent"
	case runnerallocation.StepStopDaemon:
		return "Stop dedicated Docker daemon", "Verify the owned Docker daemon and socket are absent"
	case runnerallocation.StepRemoveNetwork:
		return "Remove Runner network", "Verify the owned network is absent"
	case runnerallocation.StepRemoveEgress:
		return "Remove Runner egress rules", "Verify the owned outbound rules are absent"
	case runnerallocation.StepRemoveIdentity:
		return "Remove Runner host identity", "Verify the owned host identity and runtime files are absent"
	default:
		return "Unknown Runner step", "Reject an unsupported Runner operation"
	}
}
