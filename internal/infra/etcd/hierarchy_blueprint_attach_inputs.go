package etcd

import (
	"context"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *HierarchyRepository) GetBlueprintAttachInputGeneration(
	ctx context.Context, revisionID, attachID string,
) (etcdstore.Versioned[attachinputs.Generation], bool, error) {
	return attachinputs.Read(ctx, repository.store, revisionID, attachID)
}
