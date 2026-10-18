package agentmanagement

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (owner *Work) report(ctx context.Context, delivery workqueue.Delivery, key, name, expected string, running bool, effectErr error) {
	attempt := delivery.Attempt.Attempt
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	view, err := owner.queue.ReadResume(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil || !view.CurrentExecutor {
		// A replacement process never impersonates the old executor. Its
		// accepted typed receipt supplies the terminal inspection instead.
		return
	}
	snapshot, err := owner.diagnostics.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, view.Work.ReadRevision)
	if err != nil {
		slog.WarnContext(ctx, "Agent lifecycle diagnostics unavailable", "attempt_id", attempt.ID, "error", err)
		return
	}
	at := time.Now().UTC()
	stage := workexecution.Stage{Key: key, Name: name, Expected: expected, State: workexecution.StageRunning, StartedAt: &at}
	sequence := uint64(1)
	if snapshot.Record != nil {
		sequence = snapshot.Record.Sequence + 1
		for _, previous := range snapshot.Record.Stages {
			if previous.Key == key {
				if previous.FinishedAt != nil {
					return
				}
				stage.StartedAt = previous.StartedAt
			}
		}
	}
	if !running {
		stage.State, stage.Observed, stage.FinishedAt = workexecution.StageSucceeded, "The captured lifecycle operation completed.", &at
		if effectErr != nil {
			failure := taskprogress.CaptureControllerFailure(effectErr)
			stage.State, stage.Observed, stage.FailureCode = workexecution.StageFailed, failure.Summary(), failure.Code
		}
	}
	if err := owner.diagnostics.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor, sequence, stage, at); err != nil {
		slog.WarnContext(ctx, "Agent lifecycle diagnostic report failed", "attempt_id", attempt.ID, "error", err)
	}
}
