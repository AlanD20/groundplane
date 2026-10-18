package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReleaseLedger coordinates resource-owned Release history, captured input and
// native work publication. It does not publish generic Agent ExecutionPlans or
// hidden network Tasks; prerequisites belong to the accepted native graph.
type ReleaseLedger struct {
	*networkreservations.Planner
	*releases.Stager
	*releasequeries.Reader
	store etcdstore.Store
	tasks *TaskRepository
}

func NewReleaseLedger(store etcdstore.Store, tasks *TaskRepository) (*ReleaseLedger, error) {
	if store == nil || tasks == nil {
		return nil, errs.New(errs.KindInternal, "release publication dependencies are not configured")
	}
	return &ReleaseLedger{
		Planner: networkreservations.NewPlanner(store),
		Stager:  releases.NewStager(store),
		Reader:  releasequeries.NewReader(store),
		store:   store,
		tasks:   tasks,
	}, nil
}
