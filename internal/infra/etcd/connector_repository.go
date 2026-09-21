package etcd

import (
	connectormutations "github.com/AlanD20/groundplane/internal/infra/etcd/connectormutations"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ConnectorRepository struct {
	*connectormutations.Repository
	store hierarchyStore
}

func NewConnectorRepository(store etcdstore.Store) (*ConnectorRepository, error) {
	return newConnectorRepository(store)
}

func newConnectorRepository(store hierarchyStore) (*ConnectorRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Connector store is required")
	}
	return &ConnectorRepository{store: store, Repository: connectormutations.NewRepository(store)}, nil
}
