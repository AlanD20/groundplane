package networkwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *InputRepository) RemovalAdmission(
	ctx context.Context,
	view workqueue.View,
) ([]keyvalue.Condition, error) {
	return attachremoval.CascadeBridgeAdmission(ctx, repository.store, view)
}
