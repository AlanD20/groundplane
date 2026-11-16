package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PublishAcceptedAction binds a typed encrypted request and dependency graph to
// one operator-visible Task/replay result without claiming execution resources.
func (repository *IdempotencyRepository) PublishAcceptedAction(ctx context.Context,
	marker idempotency.IdempotencyMarker, changes ...workqueue.Change,
) (IdempotencyTransactionResult, error) {
	joined, err := workqueue.JoinChanges(changes...)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(joined.Mutations)
	plan, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask,
		joined.Conditions, joined.Mutations, resourceWorkPublicationConflict(joined.Conditions))
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return repository.Apply(ctx, marker, plan)
}
