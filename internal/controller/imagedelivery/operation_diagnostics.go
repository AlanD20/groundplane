package imagedelivery

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
)

// imageReporter belongs to one fresh execution call. Reconciliation reads saved
// evidence instead of generating a second timeline or repeating Docker effects.
type imageReporter struct {
	repository *executiondiagnostics.Repository
	attempt    workexecution.Attempt
	sequence   uint64
	lost       bool
}

func (reporter *imageReporter) record(ctx context.Context, stage workexecution.Stage) {
	if reporter.lost {
		return
	}
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	err := reporter.repository.Report(bounded, reporter.attempt.TaskID, reporter.attempt.WorkItemID,
		reporter.attempt.ID, reporter.attempt.Executor, reporter.sequence+1, stage, time.Now().UTC())
	if err == nil {
		reporter.sequence++
		return
	}
	reporter.incomplete(ctx, err)
}

func (reporter *imageReporter) incomplete(ctx context.Context, cause error) {
	if reporter.lost {
		return
	}
	reporter.lost = true
	slog.WarnContext(ctx, "image execution diagnostics incomplete", "task_id", reporter.attempt.TaskID, "attempt_id", reporter.attempt.ID, "error", cause)
	loss, finish := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer finish()
	if err := reporter.repository.MarkIncomplete(loss, reporter.attempt.TaskID, reporter.attempt.WorkItemID, reporter.attempt.ID, reporter.attempt.Executor); err != nil {
		slog.WarnContext(ctx, "unable to persist image diagnostic loss", "attempt_id", reporter.attempt.ID, "error", err)
	}
}

func (reporter *imageReporter) start(ctx context.Context, key, name, expected string) workexecution.Stage {
	at := time.Now().UTC()
	stage := workexecution.Stage{Key: key, Name: name, Expected: expected, State: workexecution.StageRunning, StartedAt: &at}
	reporter.record(ctx, stage)
	return stage
}

func (reporter *imageReporter) finish(ctx context.Context, stage workexecution.Stage, state workexecution.StageState, observed string) {
	stage.State, stage.Observed = state, observed
	if state != workexecution.StageUnknown {
		at := time.Now().UTC()
		stage.FinishedAt = &at
	}
	reporter.record(ctx, stage)
}
