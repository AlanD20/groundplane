package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routepersistence"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// RouteRepository owns stable Route records, Environment membership,
// fixed-revision pagination, target-Service fences, and CAS updates.
type RouteRepository struct {
	*routepersistence.Repository
	store hierarchyStore
}

func NewRouteRepository(store etcdstore.Store) (*RouteRepository, error) {
	return newRouteRepository(store)
}

func newRouteRepository(store hierarchyStore) (*RouteRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Route store is required")
	}
	return &RouteRepository{Repository: routepersistence.NewRepository(store), store: store}, nil
}
