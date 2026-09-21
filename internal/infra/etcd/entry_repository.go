package etcd

import (
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EntryRepository struct {
	*entryrecord.Repository
	store hierarchyStore
}

func NewEntryRepository(store etcdstore.Store) (*EntryRepository, error) {
	return newEntryRepository(store)
}

func newEntryRepository(store hierarchyStore) (*EntryRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Entry store is required")
	}
	return &EntryRepository{Repository: entryrecord.NewRepository(store), store: store}, nil
}
