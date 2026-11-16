package workqueue

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/acceptedactions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *Repository) CollectAcceptedInputDrafts(
	ctx context.Context,
	after string,
	at time.Time,
) (string, error) {
	inputs, err := acceptedactions.NewRepository(repository.store)
	if err != nil {
		return "", err
	}
	return inputs.CollectDrafts(ctx, after, at)
}

func (repository *Repository) ReadAcceptedInputKind(
	ctx context.Context,
	task TaskSnapshot,
	revision int64,
) (string, Change, error) {
	if task.Task.AcceptedIntent == nil {
		return "", Change{}, invalid("accepted input selection requires pure Task intent")
	}
	inputs, err := acceptedactions.NewRepository(repository.store)
	if err != nil {
		return "", Change{}, err
	}
	manifest, err := inputs.ReadKind(ctx, task.Task.ID, *task.Task.AcceptedIntent, revision)
	if err != nil {
		return "", Change{}, err
	}
	return manifest.Record.Kind, Change{
		Conditions: []keyvalue.Condition{{Key: acceptedactions.KindKey(task.Task.ID), ModRevision: manifest.Revision}},
	}, nil
}
