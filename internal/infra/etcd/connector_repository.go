package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	connectorEnvironmentIndexPrefix = "/v1/indexes/connectors/by-environment/"
	connectorNameIndexPrefix        = "/v1/indexes/connectors/by-name/environment/"
)

type ConnectorRepository struct {
	store hierarchyStore
}

func NewConnectorRepository(store etcdstore.Store) (*ConnectorRepository, error) {
	return newConnectorRepository(store)
}

func newConnectorRepository(store hierarchyStore) (*ConnectorRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Connector store is required")
	}
	return &ConnectorRepository{store: store}, nil
}
