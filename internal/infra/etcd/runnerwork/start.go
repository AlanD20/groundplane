package runnerwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) PrepareStart(
	ctx context.Context,
	attempt workexecution.Attempt,
) (workqueue.Change, error) {
	if attempt.StartedAt != nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner attempt already started")
	}
	_, conditions, err := repository.attemptAuthority(ctx, attempt, false)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: conditions}, nil
}
