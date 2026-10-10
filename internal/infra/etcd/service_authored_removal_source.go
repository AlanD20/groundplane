package etcd

import (
	"context"

	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// GetAppliedServiceRemovalSource joins the Service from the exact mutable
// applied projection, independently of the newer desired head that omitted it.
func (repository *ServiceRepository) GetAppliedServiceRemovalSource(
	ctx context.Context,
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	serviceID string,
) (etcdstore.Versioned[servicerecord.ServiceRecord], error) {
	if repository == nil || projection.Revision <= 0 || projection.ReadRevision < projection.Revision ||
		projectionrecord.ValidateEnvironmentComposeProjection(projection.Record) != nil {
		return etcdstore.Versioned[servicerecord.ServiceRecord]{}, errs.New(
			errs.KindValidationFailed, "applied Service removal source is invalid",
		)
	}
	return servicerecord.ReadJoined(ctx, repository.store, servicerecord.DesiredSelection{
		Services: projection.Record.DesiredServices, Revision: projection.Revision,
		ReadRevision: projection.ReadRevision,
	}, serviceID, projectionrecord.EnvironmentComposeProjectionStorageKey(projection.Record.EnvironmentID))
}
