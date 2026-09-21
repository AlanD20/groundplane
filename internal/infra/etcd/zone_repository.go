package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ZoneRepository owns Zone allocation mutations while its reads join the
// selected immutable Environment desired projection.
type ZoneRepository struct {
	*environmentqueries.ZoneReader
	store hierarchyStore
}

func NewZoneRepository(store etcdstore.Store) (*ZoneRepository, error) {
	return newZoneRepository(store)
}

func newZoneRepository(store hierarchyStore) (*ZoneRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Zone store is required")
	}
	return &ZoneRepository{store: store, ZoneReader: environmentqueries.NewZoneReader(store)}, nil
}
