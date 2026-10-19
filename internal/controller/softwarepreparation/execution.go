package softwarepreparation

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) loadDelivery(
	ctx context.Context,
	delivery workqueue.Delivery,
) (preparationrecord.WorkSnapshot, error) {
	item, attempt := delivery.Item.Item, delivery.Attempt.Attempt
	if workexecution.ValidateDelivery(item, attempt) != nil || item.ActiveAttemptID != attempt.ID ||
		attempt.Executor.Executor != workexecution.Controller || attempt.FinishedAt != nil {
		return preparationrecord.WorkSnapshot{}, errs.New(
			errs.KindStateConflict,
			"software preparation delivery is not its active attempt",
		)
	}
	return service.records.LoadWork(ctx, item, 0)
}

func (service *Service) AuthorizeStart(ctx context.Context, delivery workqueue.Delivery) (workqueue.Change, error) {
	snapshot, err := service.loadDelivery(ctx, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	if delivery.Attempt.Attempt.Direction == workexecution.Forward &&
		snapshot.Record.Record.Progress.Phase != preparation.PhaseAccepted {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"software build has already started; only reconciliation is permitted",
		)
	}
	return snapshot.Guard(), nil
}

func (service *Service) RejectUnstarted(ctx context.Context, delivery workqueue.Delivery) error {
	snapshot, err := service.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt != nil {
		return errs.New(errs.KindStateConflict, "started software preparation requires reconciliation")
	}
	progress := snapshot.Record.Record.Progress
	if progress.Phase != preparation.PhaseFailed {
		progress.Phase, progress.ErrorCode, progress.ErrorDetail = preparation.PhaseFailed, "execution.not_started", "Software preparation stopped before execution started."
	}
	return service.records.RecordOutcome(
		ctx,
		service.queue,
		attempt,
		progress,
		attempt.Direction == workexecution.Forward,
		service.now().UTC(),
	)
}

func (service *Service) Execute(ctx context.Context, delivery workqueue.Delivery) error {
	if delivery.Attempt.Attempt.Direction == workexecution.Recovery {
		return service.Reconcile(ctx, delivery)
	}
	snapshot, err := service.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt == nil {
		return errs.New(errs.KindStateConflict, "software build requires its durable start")
	}
	reporter := func(reportCtx context.Context, progress preparation.Progress) error {
		return service.records.CheckpointWork(reportCtx, service.queue, attempt, progress, service.now().UTC())
	}
	observer := &preparationObserver{repository: service.diagnostics, attempt: attempt}
	result, buildErr := service.preparer.Prepare(ctx, snapshot.Capture.Input, reporter, observer)
	// Success already includes cleanup and registry readback. Failure needs a
	// bounded observation; it must not start a second build or push.
	state := preparation.Reconciliation{Cleaned: buildErr == nil, Verified: buildErr == nil}
	var cleanupErr error
	if buildErr != nil {
		reconcile, finish := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
		state, cleanupErr = service.preparer.Reconcile(reconcile, snapshot.Capture.Input, result)
		finish()
	}
	progress := preparation.Progress{Phase: preparation.PhaseVerified, Result: result}
	if buildErr != nil || cleanupErr != nil || !state.Verified {
		progress.Phase = preparation.PhaseFailed
		cause := buildErr
		if cause == nil {
			cause = cleanupErr
		}
		if cause == nil {
			cause = errs.New(errs.KindStateConflict, "software outputs were not verified")
		}
		progress.ErrorCode, progress.ErrorDetail = publicDiagnostic(cause)
	}
	saving, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := service.records.RecordOutcome(saving, service.queue, attempt, progress, state.Cleaned, service.now().UTC()); err != nil {
		return err
	}
	if buildErr != nil {
		return buildErr
	}
	return cleanupErr
}

// Reconcile never re-enters Prepare. It can only retire captured build resources
// and verify immutable registry outputs that were already durably recorded.
func (service *Service) Reconcile(ctx context.Context, delivery workqueue.Delivery) error {
	snapshot, err := service.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt == nil {
		return errs.New(errs.KindStateConflict, "software reconciliation requires a started attempt")
	}
	saved, found, err := service.records.ReadOutcome(ctx, attempt, 0)
	if err != nil {
		return err
	}
	if found && saved.Record.Cleaned {
		return nil
	}
	progress := snapshot.Record.Record.Progress
	if progress.Phase == preparation.PhaseVerified {
		// This terminal checkpoint already proves cleanup and registry readback;
		// only its native attempt receipt was lost.
		return service.records.RecordOutcome(ctx, service.queue, attempt, progress, true, service.now().UTC())
	}
	state, reconcileErr := service.preparer.Reconcile(ctx, snapshot.Capture.Input, progress.Result)
	if progress.Phase != preparation.PhaseFailed {
		if state.Verified {
			progress.Phase = preparation.PhaseVerified
		} else {
			progress.Phase, progress.ErrorCode, progress.ErrorDetail = preparation.PhaseFailed, "execution.interrupted", "Software preparation was interrupted before all selected artifacts were verified. Submit a new preparation after cleanup."
		}
	}
	saving, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := service.records.RecordOutcome(saving, service.queue, attempt, progress, state.Cleaned, service.now().UTC()); err != nil {
		return err
	}
	return reconcileErr
}

func publicDiagnostic(err error) (string, string) {
	problem := errs.New(errs.KindInternal, "").ToProblem()
	var domainError *errs.Error
	if errors.As(err, &domainError) {
		problem = domainError.ToProblem()
	}
	return string(problem.Code), problem.Detail
}
