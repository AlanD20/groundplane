package backupruntime

import (
	"context"
	"crypto/sha256"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/healthcheckfailure"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func waitDatabaseConsumerObservation(
	ctx context.Context,
	input *agentpb.BackupWorkInput,
	ordinal uint32,
	serviceID string,
	observe func(context.Context, string) (*agentpb.ObservedProject, [sha256.Size]byte, error),
) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	_, service, err := executionplan.BackupConsumer(input.EnvironmentId,
		input.Services, input.Artifacts, input.Steps[ordinal], serviceID)
	if err != nil {
		return nil, zero, err
	}
	if !service.HasHealthcheck && service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
		return nil, zero, errs.New(
			errs.KindValidationFailed,
			"database consumer requires a healthcheck before recovery",
		)
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		observed, digest, err := observe(ctx, serviceID)
		if err != nil {
			return nil, zero, err
		}
		ready := uint32(len(observed.GetContainers())) == service.ExpectedReplicas && service.ExpectedReplicas != 0
		for _, container := range observed.GetContainers() {
			if container.ServiceId != serviceID {
				return nil, zero, invalidDatabaseStaging()
			}
			if service.HasHealthcheck {
				if detail := healthcheckfailure.Detail(container.GetHealthcheckDiagnostic()); detail != "" {
					return nil, zero, errs.Newf(
						errs.KindValidationFailed,
						"database consumer healthcheck failed: %s",
						detail,
					)
				}
				ready = ready && container.Health == agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY
			}
			ready = ready && container.State == agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING
		}
		if ready {
			return observed, digest, nil
		}
		select {
		case <-ctx.Done():
			return nil, zero, ctx.Err()
		case <-ticker.C:
		}
	}
}
