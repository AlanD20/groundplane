package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/etcdconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *TaskRepository) PrepareEtcdConfigurationAuthority(ctx context.Context, input etcdconfiguration.Input,
) (workqueue.Change, error) {
	_, _, conditions, mutations, err := etcdconfiguration.PreparePublication(input)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: conditions, Mutations: mutations}, nil
}
