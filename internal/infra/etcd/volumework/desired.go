package volumework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func desiredKey(workID string) string { return InputPrefix(workID) + "desired" }

// Direct Volume actions capture an Environment Compose deployment, including
// consumers affected by removal. Retrying that capture must not reinstall an
// older deployment after another action has changed the Environment.
func (repository *InputRepository) guardRetryDesired(ctx context.Context, view workqueue.View) ([]keyvalue.Condition, error) {
	current, guards, err := repository.ReadDesiredMatch(ctx, view)
	if err != nil {
		return nil, err
	}
	if !current {
		return nil, errs.New(errs.KindTaskNotRetryable, "Environment configuration changed after this Volume action; submit a new action instead of retrying its old configuration")
	}
	return guards, nil
}

// ReadDesiredMatch returns comparisons for both match and supersession. The
// caller joins them to assignment or revocation, never acts on an unguarded read.
func (repository *InputRepository) ReadDesiredMatch(ctx context.Context, view workqueue.View) (bool, []keyvalue.Condition, error) {
	keys := []string{desiredKey(view.Item.Item.ID), blueprints.EnvironmentBlueprintHeadKey(view.Task.Task.Scope.EnvironmentID)}
	read, err := repository.read(ctx, keys, view.ReadRevision)
	if err != nil {
		return false, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return false, nil, conflict("Volume desired-state reference is unavailable")
	}
	captured, err := idempotency.DecodeTaskReference(read.Values[0].Value)
	if err != nil {
		return false, nil, err
	}
	if read.Values[1] == nil {
		return false, []keyvalue.Condition{
			{Key: keys[0], ModRevision: read.Values[0].ModRevision},
			{Key: keys[1]},
		}, nil
	}
	current, err := idempotency.DecodeTaskReference(read.Values[1].Value)
	if err != nil {
		return false, nil, err
	}
	return captured == current, []keyvalue.Condition{
		{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: read.Values[1].ModRevision},
	}, nil
}
