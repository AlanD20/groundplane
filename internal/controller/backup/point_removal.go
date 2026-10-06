package backup

import (
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const recoveryPointRemovalRoute = "/environments/{id}/recovery-points/{point}"

func (service *BackupPruneService) RemoveRecoveryPoint(ctx context.Context,
	environmentID, pointID, key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if service == nil || service.runtime == nil || service.idempotency == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"backup prune service is not configured",
		)
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindRecoveryPoint, pointID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"recovery point scope is invalid",
		)
	}
	locator := idempotencyrecord.IdempotencyLocator{
		Method: http.MethodDelete, Route: recoveryPointRemovalRoute, Key: key,
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: environmentID,
	}
	evidence, err := service.idempotency.PreparePointRemoval(ctx, environmentID, pointID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer evidence.candidate.Destroy()
	resolved, exists, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil || exists {
		return resolved.Response, err
	}
	at := service.now().UTC().Truncate(time.Millisecond)
	prepared, err := service.runtime.PrepareRecoveryPointRemoval(ctx, environmentID, pointID,
		ids.New(ids.KindTask), ids.New(ids.KindOperation), at)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer prepared.Publication.Clear()
	// The operator selects the object; deletion uses the same system-owned
	// cleanup Task and bounded retry lifecycle as retention.
	return service.publishPrepared(ctx, prepared, locator, evidence, taskjournal.TaskActorSystem)
}
