package hierarchydeletion

import (
	"github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// NewRuntime constructs admission and captured-plan publication. Execution is
// owned exclusively by native work handlers, never a child-Task dispatcher.
func NewRuntime(store keyvalue.Store, evidence *etcd.IdempotencyRepository,
	coordinator *idempotency.Coordinator,
) (*Service, error) {
	journal, err := etcd.NewHierarchyDeletionRepository(store)
	if err != nil {
		return nil, err
	}
	clock := SystemClock{}
	repository, err := NewEtcdRepository(journal, evidence, coordinator, clock)
	if err != nil {
		return nil, err
	}
	return NewService(repository, StableIDGenerator{}, clock), nil
}
