package grouprestoration

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (service *Service) SettleCompleted(
	ctx context.Context,
	view workqueue.ResumeView,
	authorization []keyvalue.Condition,
	at time.Time,
) error {
	input, err := service.LoadRecoveryInput(ctx, view)
	if err != nil {
		return err
	}
	return service.ledger.SettleCompletedGroupRestoration(ctx, view, input, authorization, at)
}

func (service *Service) SettleFailed(
	ctx context.Context,
	view workqueue.ResumeView,
	reason releaseexecution.FailureReason,
	authorization []keyvalue.Condition,
	at time.Time,
) error {
	input, err := service.LoadRecoveryInput(ctx, view)
	if err != nil {
		return err
	}
	return service.ledger.SettleFailedReleaseRestoration(ctx, view, input, reason, authorization, at)
}
