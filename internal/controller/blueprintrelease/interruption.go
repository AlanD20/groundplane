package blueprintrelease

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (service *Service) RejectInterruptedBeforeEffects(ctx context.Context, view workqueue.ResumeView, authorization agentregistration.WorkAuthorization) error {
	return service.ledger.RejectBlueprintReleaseBeforeEffects(ctx, view, authorization, time.Now().UTC())
}

func (service *Service) QueueInterruptedRecovery(ctx context.Context, view workqueue.ResumeView, authorization agentregistration.WorkAuthorization) error {
	input, err := service.LoadCheckpointInput(ctx, view)
	if err != nil {
		return err
	}
	return service.ledger.QueueInterruptedBlueprintRecovery(ctx, view, input, authorization, time.Now().UTC())
}

func (service *Service) SettleInterruptedRecovery(ctx context.Context, view workqueue.ResumeView, authorization agentregistration.WorkAuthorization) error {
	input, err := service.LoadRecoveryInput(ctx, view)
	if err != nil {
		return err
	}
	return service.ledger.SettleInterruptedBlueprintRecovery(ctx, view, input, authorization, time.Now().UTC())
}
