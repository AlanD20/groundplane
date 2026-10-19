package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *InputRepository) PrepareAdmission(
	ctx context.Context,
	view workqueue.View,
) (*agentpb.BackupWorkInput, workqueue.Change, error) {
	input, captured, err := repository.Load(ctx, view)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	guards, err := repository.operationGuards(ctx, view, input)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	_, progress, err := repository.LoadProgress(ctx, view, input)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	return input, workqueue.Change{Conditions: append(guards, captured, progress)}, nil
}
