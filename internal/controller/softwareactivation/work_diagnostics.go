package softwareactivation

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (service *Service) observe(ctx context.Context, delivery workqueue.Delivery, key, name, expected string, action func(context.Context) error) error {
	started := time.Now().UTC()
	stage := workexecution.Stage{Key: key, Name: name, Expected: expected, State: workexecution.StageRunning, StartedAt: &started}
	service.reportStage(ctx, delivery.Attempt.Attempt, stage)
	err := action(ctx)
	finished := time.Now().UTC()
	stage.State, stage.FinishedAt, stage.Observed = workexecution.StageSucceeded, &finished, "The captured staging action completed. Running software was not replaced."
	if err != nil {
		failure := taskprogress.CaptureControllerFailure(err)
		stage.State, stage.FailureCode, stage.Observed = workexecution.StageFailed, failure.Code, failure.Summary()
	}
	service.reportStage(ctx, delivery.Attempt.Attempt, stage)
	return err
}

func (service *Service) reportStage(ctx context.Context, attempt workexecution.Attempt, stage workexecution.Stage) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	view, err := service.queue.ReadResume(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil || !view.CurrentExecutor {
		return
	}
	snapshot, err := service.diagnostics.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, view.Work.ReadRevision)
	if err != nil {
		slog.WarnContext(ctx, "software staging diagnostics unavailable", "attempt_id", attempt.ID, "error", err)
		return
	}
	sequence := uint64(1)
	if snapshot.Record != nil {
		sequence = snapshot.Record.Sequence + 1
	}
	if err := service.diagnostics.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor, sequence, stage, time.Now().UTC()); err != nil {
		slog.WarnContext(ctx, "software staging diagnostics incomplete", "attempt_id", attempt.ID, "error", err)
	}
}
