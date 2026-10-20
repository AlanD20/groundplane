package taskcheckpoint

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Standalone Script diagnostics observe accepted checkpoints only. Embedded
// hooks remain with their parent operation's diagnostic owner.
func (service *ScriptCheckpointService) reportScriptProgress(
	ctx context.Context,
	input scriptexecutions.ScriptCheckpointInput,
) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	view, err := service.queue.ReadWork(bounded, input.TaskID, input.WorkItemID, 0)
	if err == nil && view.Item.Item.Operation != workexecution.Script {
		return
	}
	var snapshot executiondiagnostics.Snapshot
	if err == nil {
		snapshot, err = service.diagnostics.Read(bounded, input.TaskID, input.WorkItemID, input.AttemptID, 0)
	}
	if err == nil && (snapshot.Expired || snapshot.FinishedAt != nil) {
		return
	}
	if err == nil {
		err = service.writeScriptProgress(bounded, input, snapshot)
	}
	if err != nil {
		slog.WarnContext(bounded, "Script diagnostics incomplete", "attempt_id", input.AttemptID, "error", err)
		if markErr := service.diagnostics.MarkIncomplete(bounded, input.TaskID, input.WorkItemID, input.AttemptID, input.Executor); markErr != nil {
			slog.WarnContext(
				bounded,
				"could not record Script diagnostic loss",
				"attempt_id",
				input.AttemptID,
				"error",
				markErr,
			)
		}
	}
}

func (service *ScriptCheckpointService) writeScriptProgress(ctx context.Context,
	input scriptexecutions.ScriptCheckpointInput, snapshot executiondiagnostics.Snapshot,
) error {
	sequence := uint64(0)
	var stages []workexecution.Stage
	if snapshot.Record != nil {
		sequence, stages = snapshot.Record.Sequence, snapshot.Record.Stages
	}
	find := func(key string) *workexecution.Stage {
		for index := range stages {
			if stages[index].Key == key {
				stage := stages[index]
				return &stage
			}
		}
		return nil
	}
	report := func(stage workexecution.Stage) error {
		sequence++
		return service.diagnostics.Report(ctx, input.TaskID, input.WorkItemID, input.AttemptID,
			input.Executor, sequence, stage, input.At)
	}
	start := func(key, name, expected string) error {
		if find(key) != nil {
			return nil // Exact checkpoint replay must not reopen a stage.
		}
		return report(workexecution.Stage{Key: key, Name: name, Expected: expected,
			State: workexecution.StageRunning, StartedAt: &input.At})
	}
	finish := func(key, observed string, failed bool) error {
		stage := find(key)
		if stage == nil {
			return errs.New(errs.KindStateConflict, "Script stage start observation is unavailable")
		}
		if stage.FinishedAt != nil {
			return nil
		}
		stage.State, stage.FinishedAt, stage.Observed = workexecution.StageSucceeded, &input.At, observed
		if failed {
			stage.State, stage.FailureCode = workexecution.StageFailed, "script.execution_failed"
		}
		return report(*stage)
	}
	switch input.State {
	case scriptexecutions.ScriptExecutionStartAuthorized:
		return start(
			"prepare_body",
			"Prepare Script body",
			"Captured Script bytes are written to the owned execution directory",
		)
	case scriptexecutions.ScriptExecutionBodyPrepared:
		if err := finish("prepare_body", "Captured body digest and file ownership verified", false); err != nil {
			return err
		}
		return start(
			"create_container",
			"Create Script container",
			"The execution container matches the captured image and ownership labels",
		)
	case scriptexecutions.ScriptExecutionContainerCreated:
		if err := finish("create_container", "Execution container identity and ownership verified", false); err != nil {
			return err
		}
		return start(
			"run_script",
			"Execute captured Script",
			"The captured command exits successfully within its execution deadline",
		)
	case scriptexecutions.ScriptExecutionOutcomeRecorded:
		outcome := input.Evidence.Outcome
		if outcome == nil {
			return errs.New(errs.KindStateConflict, "Script outcome evidence is unavailable")
		}
		observed := "Execution outcome: " + string(outcome.Reason)
		if outcome.ExitCode != nil {
			observed += fmt.Sprintf("; exit code %d", *outcome.ExitCode)
		}
		key := ""
		for _, stage := range stages {
			if stage.FinishedAt == nil && stage.Key != "cleanup" {
				key = stage.Key
			}
		}
		if key != "" {
			failed := outcome.Reason != scriptexecutions.ScriptOutcomeNormalExit || outcome.ExitCode == nil ||
				*outcome.ExitCode != 0
			if err := finish(key, observed, failed); err != nil {
				return err
			}
		} else if len(stages) == 0 {
			if err := report(workexecution.Stage{Key: "run_script", Name: "Execute captured Script",
				Expected: "The captured command exits successfully within its execution deadline",
				State:    workexecution.StageSkipped, FinishedAt: &input.At, Observed: observed}); err != nil {
				return err
			}
		}
		return start(
			"cleanup",
			"Clean Script execution",
			"The owned execution container, Script body and temporary directory are absent",
		)
	case scriptexecutions.ScriptExecutionCleanupProven:
		return finish("cleanup", "Container, Script body and execution directory absence verified", false)
	default:
		return nil
	}
}
