package connectors

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/s3connector"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type checkAssignments interface {
	GetTaskAssignment(context.Context, string) (etcd.TaskAssignment, error)
}

// CheckExecutor executes the captured permission check once. A resumed claim
// performs only cleanup with the original encrypted credentials and object keys.
type CheckExecutor struct {
	repository  *connectorchecks.Repository
	assignments checkAssignments
	protector   *secretvalue.Protector
}

func NewCheckExecutor(repository *connectorchecks.Repository, assignments checkAssignments,
	protector *secretvalue.Protector,
) (*CheckExecutor, error) {
	if repository == nil || assignments == nil || protector == nil {
		return nil, errs.New(errs.KindInternal, "connector check executor dependencies are required")
	}
	return &CheckExecutor{repository: repository, assignments: assignments, protector: protector}, nil
}

func (executor *CheckExecutor) Execute(ctx context.Context, task etcd.TaskRecord) error {
	if ctx == nil || task.Executor != taskjournal.TaskExecutorController || task.Type != taskjournal.TaskCheck ||
		task.Status != taskjournal.TaskStatusRunning || task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceConnector ||
		len(task.Params) != 2 || len(task.Steps) != 0 || task.RenderGeneration != 1 {
		return errs.New(errs.KindValidationFailed, "connector connection-check Task is invalid")
	}
	loaded, err := executor.repository.Load(ctx, task.ID)
	if err != nil {
		return err
	}
	record := loaded.Record
	defer record.Clear()
	planHash, err := connectorchecks.PlanHash(record)
	if err != nil || record.Owner.TaskID != task.ID || record.Owner.PlanID != task.PlanID ||
		record.Owner.OperationID != task.OperationID || record.Owner.EnvironmentID != task.Owner.EnvironmentID ||
		record.RetryOf != task.RetryOf ||
		record.Connector.Connector.ID != task.Target || planHash != task.PlanHash ||
		record.ConfigurationSHA256 != task.Params[taskjournal.TaskConnectorCheckInputParam] {
		return errs.New(errs.KindStateConflict, "connector check differs from its sealed Task input")
	}
	if record.Phase == connectorchecks.Finished {
		// A cleanup-only attempt may lose its terminal acknowledgement after
		// cleanup commits. Replaying that result completes cleanup, not the probe.
		if record.Outcome == connectorchecks.Interrupted && task.RetryOf != "" {
			return nil
		}
		return checkOutcomeError(record)
	}
	claim, err := executor.assignments.GetTaskAssignment(ctx, task.ID)
	if err != nil {
		return err
	}
	if claim.Task.Record.PlanID != task.PlanID || claim.Task.Record.PlanHash != task.PlanHash ||
		claim.Task.Record.OperationID != task.OperationID || claim.Task.Record.Target != task.Target {
		return errs.New(errs.KindStateConflict, "connector check Task claim changed")
	}
	journal, err := connectorchecks.NewJournal(executor.repository, record.Owner, claim.Assignment.Record.AssignmentID)
	if err != nil {
		return err
	}
	if record.Phase != connectorchecks.Ready {
		return executor.resumeCheckCleanup(ctx, record, journal, task.RetryOf != "")
	}
	if !time.Now().UTC().Before(claim.Assignment.Record.Deadline) {
		return finishUnstartedCheck(ctx, journal, context.DeadlineExceeded)
	}
	adapter, err := executor.capturedCheckAdapter(ctx, record)
	if err != nil {
		return finishUnstartedCheck(ctx, journal, err)
	}
	if err := journal.Start(ctx, time.Now().UTC()); err != nil {
		return err
	}
	_, probeErr := adapter.CheckConnection(ctx, task.OperationID, journal)
	// A disconnected request cannot erase mutation authority. Reload the durable
	// journal; the transport's in-memory cleanup result is not settlement proof.
	settlementCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	latest, err := executor.repository.Load(settlementCtx, task.ID)
	if err != nil {
		return err
	}
	defer latest.Record.Clear()
	if checkCleanupPending(latest.Record) {
		return errs.New(
			errs.KindStateConflict,
			"connector check cleanup is incomplete; its captured credentials are retained",
		)
	}
	outcome, failure := connectorchecks.Passed, s3connector.CheckFailureNone
	if probeErr != nil {
		outcome, failure = connectorchecks.Failed, s3connector.CheckFailureForError(probeErr)
	}
	if err := journal.Finish(settlementCtx, outcome, failure, time.Now().UTC()); err != nil {
		return err
	}
	return probeErr
}

func (executor *CheckExecutor) resumeCheckCleanup(ctx context.Context, record connectorchecks.Record,
	journal *connectorchecks.Journal,
	cleanupRetry bool,
) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := journal.ResumeCleanup(cleanupCtx); err != nil {
		return err
	}
	if checkCleanupPending(record) {
		adapter, err := executor.capturedCheckAdapter(cleanupCtx, record)
		if err != nil {
			return err
		}
		started := time.Now()
		_, cleanupErr := adapter.CleanupConnectionCheck(cleanupCtx, record.Cleanup, journal)
		stage := s3connector.ConnectionCheckStage{Name: s3connector.CheckCleanup,
			Elapsed: time.Since(
				started,
			), Succeeded: cleanupErr == nil, Failure: s3connector.CheckFailureForError(cleanupErr)}
		if err := journal.WriteConnectionCheckStage(cleanupCtx, stage); err != nil {
			return err
		}
		if cleanupErr != nil {
			return cleanupErr
		}
	}
	if err := journal.Finish(cleanupCtx, connectorchecks.Interrupted, s3connector.CheckFailureInterrupted, time.Now().UTC()); err != nil {
		return err
	}
	if cleanupRetry {
		return nil
	}
	return checkOutcomeError(
		connectorchecks.Record{Outcome: connectorchecks.Interrupted, Failure: s3connector.CheckFailureInterrupted},
	)
}

func finishUnstartedCheck(ctx context.Context, journal *connectorchecks.Journal, cause error) error {
	settlementCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := journal.FinishUnstarted(settlementCtx, s3connector.CheckFailureForError(cause), time.Now().UTC()); err != nil {
		return err
	}
	return cause
}

func checkCleanupPending(record connectorchecks.Record) bool {
	return record.Cleanup.ObjectPending || record.Cleanup.MultipartPending || len(record.Cleanup.UploadIDs) != 0
}

func checkOutcomeError(record connectorchecks.Record) error {
	if record.Outcome == connectorchecks.Passed {
		return nil
	}
	switch record.Failure {
	case s3connector.CheckFailureTimeout:
		return context.DeadlineExceeded
	case s3connector.CheckFailureCanceled:
		return context.Canceled
	case s3connector.CheckFailurePermissions:
		return errs.New(
			errs.KindValidationFailed,
			"connector check could not authenticate or use the required storage permissions",
		)
	case s3connector.CheckFailureConnection:
		return errs.New(errs.KindStorageUnavailable, "connector check could not reach the configured storage endpoint")
	case s3connector.CheckFailureOwnership:
		return errs.New(
			errs.KindStateConflict,
			"connector check test-object ownership did not match; inspect its cleanup result",
		)
	case s3connector.CheckFailureInterrupted:
		return errs.New(
			errs.KindStateConflict,
			"connector check was interrupted; cleanup completed, but permissions were not certified",
		)
	default:
		return errs.New(errs.KindInternal, "connector check could not record its execution result")
	}
}
