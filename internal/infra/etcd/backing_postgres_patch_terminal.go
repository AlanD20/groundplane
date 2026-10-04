package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingpostgresruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *TaskRepository) postgresRuntimeAcknowledgement(ctx context.Context,
	runtimeValue []byte, revision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	return backingpostgresruntime.PrepareAcknowledgement(ctx, repository.store, runtimeValue, revision)
}
