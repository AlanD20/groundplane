package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type ServiceRemovalCapture struct {
	Input      serviceremovalwork.Input
	Item       workexecution.Item
	Change     workqueue.Change
	repository *ServiceRepository
}

func (capture ServiceRemovalCapture) Finish(ctx context.Context, applied bool) error {
	if applied || capture.repository == nil {
		return nil
	}
	return capture.repository.abandonRemovalSecretPreparation(ctx, capture.Input, nil)
}
