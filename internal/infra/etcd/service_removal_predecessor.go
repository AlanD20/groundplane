package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *ServiceRepository) GetServiceRemovalPredecessor(ctx context.Context, serviceID string, revision int64) (workqueue.ClaimSnapshot, workexecution.Placement, error) {
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return workqueue.ClaimSnapshot{}, workexecution.Placement{}, err
	}
	return queue.ReadServiceClaim(ctx, serviceID, revision)
}
