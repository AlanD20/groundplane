package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptmutations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ScriptRepository owns Script records, Environment membership, slug uniqueness, and CAS updates.
type ScriptRepository struct {
	*scriptmutations.Repository
	*scriptsourcequeries.SourceReader
	store hierarchyStore
}

func NewScriptRepository(store etcdstore.Store) (*ScriptRepository, error) {
	return newScriptRepository(store)
}

func newScriptRepository(store hierarchyStore) (*ScriptRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Script store is required")
	}
	return composeScriptRepository(store), nil
}

func composeScriptRepository(store hierarchyStore) *ScriptRepository {
	return &ScriptRepository{
		Repository:   scriptmutations.NewRepository(store),
		SourceReader: scriptsourcequeries.NewSourceReader(store),
		store:        store,
	}
}
