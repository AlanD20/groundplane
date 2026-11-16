package workqueue

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskcredentialrefs"
)

func (repository *Repository) PrepareCredentialReferences(
	ctx context.Context,
	task workexecution.Task,
	ids []string,
) (Change, error) {
	owner, err := taskcredentialrefs.NewRepository(repository.store)
	if err != nil {
		return Change{}, err
	}
	publication, err := owner.Stage(ctx, task, ids)
	return Change{Conditions: publication.Conditions, Mutations: publication.Mutations}, err
}

func (repository *Repository) FinishCredentialReferences(ctx context.Context, taskID string, applied bool) error {
	owner, err := taskcredentialrefs.NewRepository(repository.store)
	if err != nil {
		return err
	}
	return owner.Finish(ctx, taskID, applied)
}

func (repository *Repository) CollectCredentialReferences(
	ctx context.Context,
	after string,
	at time.Time,
) (string, error) {
	owner, err := taskcredentialrefs.NewRepository(repository.store)
	if err != nil {
		return after, err
	}
	return owner.Collect(ctx, after, at)
}
