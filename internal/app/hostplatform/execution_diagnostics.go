package hostplatform

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func NewExecutionDiagnostics(store workqueue.Store) (*executiondiagnostics.Repository, error) {
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		return nil, err
	}
	return executiondiagnostics.New(store, queue)
}
