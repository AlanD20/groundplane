package etcd

import (
	entryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const entryOwnerPrefix = "/v1/indexes/entries/by-owner/environment/"

const blueprintEntryEnvironmentPrefix = "/v1/indexes/entries/blueprint-environment/"

// EntryValueGeneration is the closed atomic value input for an Entry mutation.
type EntryValueGeneration struct {
	Plain  *entryvalues.PlainGeneration
	Secret *entryvalues.SecretGeneration
}

type EntryRepository struct {
	store hierarchyStore
}

func NewEntryRepository(store etcdstore.Store) (*EntryRepository, error) {
	return newEntryRepository(store)
}

func newEntryRepository(store hierarchyStore) (*EntryRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Entry store is required")
	}
	return &EntryRepository{store: store}, nil
}
