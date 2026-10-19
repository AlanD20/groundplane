package blueprintrelease

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// SettleCompleted records the captured successful execution, not a worker's
// claimed status. Runtime input is reconstructed from Controller-owned sources.
// Interruption/failure settlement has its own recovery path.
func (service *Service) SettleCompleted(
	ctx context.Context,
	view workqueue.ResumeView,
	authorization []keyvalue.Condition,
	at time.Time,
) error {
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		return service.settleRecovered(ctx, view, authorization, at)
	}
	input, err := service.LoadCheckpointInput(ctx, view)
	if err != nil {
		return err
	}
	return service.ledger.SettleCompletedBlueprintRelease(ctx, view, input, authorization, at)
}

func (service *Service) settleRecovered(
	ctx context.Context,
	view workqueue.ResumeView,
	authorization []keyvalue.Condition,
	at time.Time,
) error {
	input, err := service.LoadRecoveryInput(ctx, view)
	if err != nil {
		return err
	}
	return service.ledger.SettleRecoveredBlueprintRelease(ctx, view, input, authorization, at)
}

func (service *Service) SettleFailed(
	ctx context.Context,
	view workqueue.ResumeView,
	reason releaseexecution.FailureReason,
	authorization []keyvalue.Condition,
	at time.Time,
) error {
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		input, err := service.LoadRecoveryInput(ctx, view)
		if err != nil {
			return err
		}
		return service.ledger.SettleFailedReleaseRestoration(ctx, view, input, reason, authorization, at)
	}
	delivery, err := service.LoadWorkAssignment(ctx, view)
	if err != nil {
		return err
	}
	_, release, err := workexecution.DecodeReleaseDelivery(delivery, view.Attempt.Attempt.Executor)
	if err != nil {
		return err
	}
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(view.Attempt.Attempt, release)
	if err != nil {
		return err
	}
	input := releaseexecution.Input{Attempt: view.Attempt.Attempt, Release: release, Artifacts: artifacts}
	return service.ledger.SettleFailedBlueprintRelease(ctx, view, input, reason, authorization, at)
}
