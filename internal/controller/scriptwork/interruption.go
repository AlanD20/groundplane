package scriptwork

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (service *Service) ReconcileInterrupted(
	ctx context.Context,
	view workqueue.ResumeView,
	authorization agentregistration.WorkAuthorization,
) error {
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		return service.repository.ReconcileInterruptedScriptRecovery(ctx, view, authorization, time.Now().UTC())
	}
	return service.repository.ReconcileInterruptedScript(ctx, view, authorization, time.Now().UTC())
}
