package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ScriptRepository owns Script records, Environment membership, slug uniqueness, and CAS updates.
type ScriptRepository struct{ store hierarchyStore }

func NewScriptRepository(store etcdstore.Store) (*ScriptRepository, error) {
	return newScriptRepository(store)
}

func newScriptRepository(store hierarchyStore) (*ScriptRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Script store is required")
	}
	return &ScriptRepository{store: store}, nil
}
