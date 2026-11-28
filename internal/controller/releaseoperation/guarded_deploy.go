package releaseoperation

import (
	"context"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnergrants"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// DeployServiceGuarded publishes a native deployment only while its caller's
// captured authorization comparisons still hold.
func (service *Service) DeployServiceGuarded(
	ctx context.Context,
	serviceID string,
	request domain.ServiceDeployInput,
	idempotencyKey string,
	guards []etcdstore.Condition,
) (idempotencyrecord.IdempotencyResponse, error) {
	if len(guards) == 0 {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"guarded deployment requires captured authority",
		)
	}
	return service.deployService(ctx, serviceID, request, idempotencyKey, guards, nil)
}

// DeployServiceForRunnerGuarded publishes the same native Task with its exact
// Runner grant provenance in the acceptance transaction.
func (service *Service) DeployServiceForRunnerGuarded(ctx context.Context, serviceID string,
	request domain.ServiceDeployInput, idempotencyKey string, guards []etcdstore.Condition,
	provenance runnergrants.TaskProvenance,
) (idempotencyrecord.IdempotencyResponse, error) {
	if len(guards) == 0 {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"guarded deployment requires captured authority",
		)
	}
	if err := provenance.ValidateDeploy(serviceID, request.Image); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return service.deployService(ctx, serviceID, request, idempotencyKey, guards, provenance.PublicationForTask)
}
