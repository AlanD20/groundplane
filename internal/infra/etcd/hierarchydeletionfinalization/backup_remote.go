package hierarchydeletionfinalization

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionbackupcleanup"
)

func (repository *Preparer) prepareHierarchyDeletionBackupRemoteFinalizer(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
) (Effects, error) {
	prepared, err := hierarchydeletionbackupcleanup.NewRepository(repository.store).
		PrepareFinalization(ctx, operation, action)
	if err != nil {
		return Effects{}, err
	}
	return Effects{fixedInputDigest: prepared.FixedInputDigest(), conditions: prepared.Conditions(),
		mutations: prepared.Mutations(), values: prepared.Values()}, nil
}
