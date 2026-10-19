package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
)

func (repository *EnvironmentBlueprintRepository) LoadBlueprintAdmissionSnapshot(
	ctx context.Context,
	environmentID string,
	revision int64,
) (blueprintunits.Snapshot, error) {
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return blueprintunits.Snapshot{}, err
	}
	return ledger.LoadAdmission(ctx, environmentID, revision)
}
