package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ServiceRepository projects head-selected desired Services and joins their
// independently mutable runtime sidecars.
type ServiceRepository struct {
	*environmentqueries.ServiceReader
	store hierarchyStore
}

func existingIdempotencyTransaction(
	ctx context.Context,
	store hierarchyStore,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, bool, error) {
	repository, err := NewIdempotencyRepository(store)
	if err != nil {
		return IdempotencyTransactionResult{}, false, err
	}
	evidence, err := repository.Read(ctx, marker.Locator)
	if err != nil {
		return IdempotencyTransactionResult{}, false, err
	}
	if evidence == nil {
		return IdempotencyTransactionResult{}, false, nil
	}
	existing, err := evidence.Marker()
	if err != nil {
		return IdempotencyTransactionResult{}, false, err
	}
	return IdempotencyTransactionResult{
		kind: idempotencyTransactionExisting, revision: evidence.modRevision, marker: existing,
	}, true, nil
}

func NewServiceRepository(store etcdstore.Store) (*ServiceRepository, error) {
	return newServiceRepository(store)
}

func newServiceRepository(store hierarchyStore) (*ServiceRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Service store is required")
	}
	return &ServiceRepository{store: store, ServiceReader: environmentqueries.NewServiceReader(store)}, nil
}
