package blueprintunits

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadWorkIntentMatch uses the same captured Apply ownership boundary as input
// publication and forward effects. The returned comparison must join the grant
// or supersession transaction; absence/corruption is not proof of supersession.
func (repository *Repository) ReadWorkIntentMatch(ctx context.Context,
	view workqueue.View,
) (bool, []keyvalue.Condition, error) {
	task := view.Task.Task
	if task.Action != workexecution.ActionApply || task.TargetID != task.Scope.EnvironmentID ||
		view.Item.Item.TaskID != task.ID || view.ReadRevision <= 0 {
		return false, nil, errs.New(errs.KindStateConflict, "Blueprint work has no captured desired-state owner")
	}
	key := blueprints.EnvironmentBlueprintHeadKey(task.Scope.EnvironmentID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision},
	)
	if err != nil {
		return false, nil, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return false, nil, errs.New(errs.KindStateConflict, "Blueprint desired-state owner is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
		return false, nil, errs.New(errs.KindStateConflict, "Blueprint desired-state ownership revision is invalid")
	}
	current, err := idempotency.DecodeTaskReference(value.Value)
	if err != nil {
		return false, nil, err
	}
	return current == task.ID, []keyvalue.Condition{{Key: key, ModRevision: value.ModRevision}}, nil
}
