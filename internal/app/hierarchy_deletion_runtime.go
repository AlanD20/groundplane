package app

import (
	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func newHierarchyDeletionRuntime(store keyvalue.Store, authority controllerAuthorityComposition) (*hierarchydeletion.Service, error) {
	return hierarchydeletion.NewRuntime(store, authority.idempotency, authority.intentCoordinator)
}
