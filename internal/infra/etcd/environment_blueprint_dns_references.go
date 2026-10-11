package etcd

import (
	"context"

	model "github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
)

func (repository *EnvironmentBlueprintRepository) ReadEnvironmentBlueprintDNSBindings(
	ctx context.Context, environmentID string, revision int64,
) ([]model.EnvironmentBinding, error) {
	return dnsrecords.NewReader(repository.store).AppliedEnvironmentBindings(ctx, environmentID, revision)
}
