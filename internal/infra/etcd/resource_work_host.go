package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
)

func (repository *HierarchyRepository) ResolveResourceHost(ctx context.Context) (workexecution.Placement, error) {
	host, err := hostbindings.ReadLocal(ctx, repository.store, 0)
	if err != nil {
		return workexecution.Placement{}, err
	}
	return workexecution.Placement{Executor: workexecution.Agent, HostID: host.Binding.HostID}, nil
}
