package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// blueprintStagingTaskCondition fences inert staging against either its internal
// pre-publication owner or an existing pure accepted operator Task. Promotion or
// Stop changes the Task revision and prevents late staging from gaining custody.
func blueprintStagingTaskCondition(ctx context.Context, store idempotencyRepositoryStore,
	parent workexecution.Task,
) (keyvalue.Condition, error) {
	key := workqueue.TaskKey(parent.ID)
	condition := keyvalue.Condition{Key: key}
	if parent.AcceptedIntent == nil {
		return condition, nil
	}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}})
	if err != nil {
		return condition, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		return condition, errs.New(errs.KindStateConflict, "Blueprint accepted Task is unavailable during compilation")
	}
	defer keyvalue.ClearValues(read.Values)
	current, err := workqueue.DecodeTask(read.Values[0].Value)
	if err != nil || !current.PendingPreparation() || current.Stop != nil || current.ID != parent.ID ||
		current.Scope != parent.Scope || current.AcceptedIntent == nil || *current.AcceptedIntent != *parent.AcceptedIntent ||
		!current.CreatedAt.Equal(parent.CreatedAt) || !current.Deadline.Equal(parent.Deadline) {
		return condition, errs.New(errs.KindStateConflict, "Blueprint accepted intent changed during compilation")
	}
	condition.ModRevision = read.Values[0].ModRevision
	return condition, nil
}
