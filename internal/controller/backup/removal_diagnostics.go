package backup

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RemovalReporter belongs to one fresh attempt. It never supplies execution
// authority and never reports provider responses, credentials or object contents.
type RemovalReporter struct {
	repository *executiondiagnostics.Repository
	attempt    workexecution.Attempt
	sequence   uint64
	lost       bool
	active     *workexecution.Stage
}

func NewRemovalReporter(
	repository *executiondiagnostics.Repository,
	attempt workexecution.Attempt,
) (*RemovalReporter, error) {
	if repository == nil || attempt.Validate() != nil || attempt.StartedAt == nil || attempt.FinishedAt != nil {
		return nil, errs.New(errs.KindInternal, "backup removal diagnostics require a started attempt")
	}
	return &RemovalReporter{repository: repository, attempt: attempt}, nil
}

func (reporter *RemovalReporter) start(ctx context.Context, key, name, expected string) {
	at := time.Now().UTC()
	stage := workexecution.Stage{
		Key:       key,
		Name:      name,
		Expected:  expected,
		State:     workexecution.StageRunning,
		StartedAt: &at,
	}
	reporter.active = &stage
	reporter.record(ctx, stage)
}

func (reporter *RemovalReporter) finish(ctx context.Context, observed string) {
	if reporter.active == nil {
		return
	}
	stage := *reporter.active
	at := time.Now().UTC()
	stage.State, stage.Observed, stage.FinishedAt = workexecution.StageSucceeded, observed, &at
	reporter.record(ctx, stage)
	reporter.active = nil
}

func (reporter *RemovalReporter) failed(ctx context.Context, deletionGranted bool) {
	if reporter.active == nil {
		return
	}
	stage := *reporter.active
	if deletionGranted {
		stage.State = workexecution.StageUnknown
		stage.FailureCode = "backup.object_absence_unconfirmed"
		stage.Observed = "Deletion was authorized, but exact object absence was not recorded. Resource protection remains until cleanup confirms absence."
	} else {
		at := time.Now().UTC()
		stage.State, stage.FinishedAt = workexecution.StageFailed, &at
		stage.FailureCode = "backup.delete_not_authorized"
		stage.Observed = "This attempt could not prepare the captured object for deletion. It did not authorize a delete request."
	}
	reporter.record(ctx, stage)
	reporter.active = nil
}

func (reporter *RemovalReporter) record(ctx context.Context, stage workexecution.Stage) {
	if reporter.lost {
		return
	}
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	err := reporter.repository.Report(bounded, reporter.attempt.TaskID, reporter.attempt.WorkItemID,
		reporter.attempt.ID, reporter.attempt.Executor, reporter.sequence+1, stage, time.Now().UTC())
	cancel()
	if err == nil {
		reporter.sequence++
		return
	}
	reporter.lost = true
	slog.WarnContext(
		ctx,
		"backup removal diagnostics incomplete",
		"task_id",
		reporter.attempt.TaskID,
		"attempt_id",
		reporter.attempt.ID,
		"error",
		err,
	)
	bounded, cancel = context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := reporter.repository.MarkIncomplete(bounded, reporter.attempt.TaskID, reporter.attempt.WorkItemID, reporter.attempt.ID, reporter.attempt.Executor); err != nil {
		slog.WarnContext(
			ctx,
			"unable to retain backup diagnostic loss",
			"attempt_id",
			reporter.attempt.ID,
			"error",
			err,
		)
	}
}
