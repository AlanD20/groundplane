package grouprestoration

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (service *Service) ReconcileInterrupted(
	ctx context.Context,
	view workqueue.ResumeView,
	authorization agentregistration.WorkAuthorization,
) error {
	input, err := service.LoadRecoveryInput(ctx, view)
	if err != nil {
		return err
	}
	return service.ledger.ReconcileInterruptedGroupRestoration(ctx, view, input, authorization, time.Now().UTC())
}
