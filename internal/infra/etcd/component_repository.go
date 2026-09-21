package etcd

import (
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	platformcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ComponentRepository owns Environment-component singleton identity,
// fixed-revision reads, and desired/runtime CAS separation.
type ComponentRepository struct {
	*platformcomponents.Persistence
	*componentrecord.Repository
	store hierarchyStore
}

func NewComponentRepository(store etcdstore.Store) (*ComponentRepository, error) {
	return newComponentRepository(store)
}

func newComponentRepository(store hierarchyStore) (*ComponentRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Component store is required")
	}
	return &ComponentRepository{Persistence: platformcomponents.NewPersistence(store), Repository: componentrecord.NewRepository(store), store: store}, nil
}
