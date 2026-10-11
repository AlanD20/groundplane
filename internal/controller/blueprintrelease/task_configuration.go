package blueprintrelease

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func (service *Service) prepareTaskConfiguration(ctx context.Context, task etcd.TaskRecord,
	readRevision int64) (etcd.TaskRecord, error) {
	if len(task.Materializations) == 0 {
		return task, nil
	}
	return service.ledger.PrepareTaskConfigurationAtRevision(ctx, task, readRevision)
}
