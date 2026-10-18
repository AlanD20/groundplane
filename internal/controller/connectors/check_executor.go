package connectors

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/s3connector"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CheckExecutor executes the captured permission check once. A resumed claim
// performs only cleanup with the original encrypted credentials and object keys.
type CheckExecutor struct {
	repository *connectorchecks.Repository
	protector  *secretvalue.Protector
}

func NewCheckExecutor(repository *connectorchecks.Repository,
	protector *secretvalue.Protector,
) (*CheckExecutor, error) {
	if repository == nil || protector == nil {
		return nil, errs.New(errs.KindInternal, "connector check executor dependencies are required")
	}
	return &CheckExecutor{repository: repository, protector: protector}, nil
}

func (executor *CheckExecutor) Execute(ctx context.Context, delivery workqueue.Delivery) error {
	err := executor.execute(ctx, delivery)
	if err == nil || ctx == nil {
		return err
	}
	settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	loaded, loadErr := executor.repository.Load(settlement, delivery.Item.Item.TaskID)
	if loadErr != nil {
		return errs.WrapJoined(errs.KindStateConflict, err, loadErr)
	}
	defer loaded.Record.Clear()
	if loaded.Record.Phase == connectorchecks.Finished || loaded.Record.Phase == connectorchecks.CleanupRequired {
		return err
	}
	journal, journalErr := connectorchecks.NewJournal(executor.repository, loaded.Record.Owner,
		delivery.Attempt.Attempt.ID, delivery.Attempt.Attempt.Executor)
	if journalErr != nil {
		return errs.WrapJoined(errs.KindStateConflict, err, journalErr)
	}
	if stopErr := journal.Stop(settlement, s3connector.CheckFailureForError(err), time.Now().UTC()); stopErr != nil {
		return errs.WrapJoined(errs.KindStateConflict, err, stopErr)
	}
	return err
}

func (executor *CheckExecutor) execute(ctx context.Context, delivery workqueue.Delivery) error {
	item, attempt := delivery.Item.Item, delivery.Attempt.Attempt
	if ctx == nil || item.Validate() != nil || attempt.Validate() != nil ||
		item.Operation != workexecution.ConnectorCheck || item.Phase != workexecution.Running ||
		attempt.Executor.Executor != workexecution.Controller || item.ActiveAttemptID != attempt.ID ||
		attempt.TaskID != item.TaskID || attempt.WorkItemID != item.ID || item.Input == nil ||
		*item.Input != attempt.Input || attempt.StartedAt == nil || attempt.FinishedAt != nil {
		return errs.New(errs.KindValidationFailed, "connector connection-check execution is invalid")
	}
	loaded, err := executor.repository.Load(ctx, item.TaskID)
	if err != nil {
		return err
	}
	record := loaded.Record
	defer record.Clear()
	planHash, err := connectorchecks.PlanHash(record)
	if err != nil || record.Owner.TaskID != item.TaskID || record.Owner.WorkItemID != item.ID ||
		record.Owner.PlanID != attempt.Input.ID || record.Connector.Connector.ID != item.TargetID || planHash != attempt.Input.SHA256 {
		return errs.New(errs.KindStateConflict, "connector check differs from its sealed Task input")
	}
	if record.Phase == connectorchecks.Finished {
		// A cleanup-only attempt may lose its terminal acknowledgement after
		// cleanup commits. Replaying that result completes cleanup, not the probe.
		if record.Outcome == connectorchecks.Interrupted && attempt.Direction == workexecution.Recovery {
			return nil
		}
		return checkOutcomeError(record)
	}
	journal, err := connectorchecks.NewJournal(executor.repository, record.Owner, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	if record.Phase != connectorchecks.Ready {
		return executor.resumeCheckCleanup(ctx, record, journal, attempt.Direction == workexecution.Recovery)
	}
	if attempt.Direction != workexecution.Forward {
		return errs.New(errs.KindStateConflict, "unstarted connector check has no cleanup to execute")
	}
	if !time.Now().UTC().Before(attempt.Deadline) {
		return finishUnstartedCheck(ctx, journal, context.DeadlineExceeded)
	}
	adapter, err := executor.capturedCheckAdapter(ctx, record)
	if err != nil {
		return finishUnstartedCheck(ctx, journal, err)
	}
	if err := journal.Start(ctx, time.Now().UTC()); err != nil {
		return err
	}
	_, probeErr := adapter.CheckConnection(ctx, record.Owner.OperationID, journal)
	// A disconnected request cannot erase mutation authority. Reload the durable
	// journal; the transport's in-memory cleanup result is not settlement proof.
	settlementCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	latest, err := executor.repository.Load(settlementCtx, item.TaskID)
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
