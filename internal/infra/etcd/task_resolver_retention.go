package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Current resolver acknowledgements and failed automatic requests retain their
// original Task. Discovery skips held Tasks; final deletion compares the same
// references so an observation or Retry cannot race journal retirement.
func (repository *TaskRepository) resolverTaskPruneAuthority(ctx context.Context,
	task TaskRecord, revision int64,
) (bool, []keyvalue.Condition, error) {
	if task.Owner != taskjournal.PlatformTaskOwner() || ids.Validate(ids.KindComponent, task.Target) != nil {
		return false, nil, nil
	}
	keys := []string{platformcomponents.ComponentObservationKey(task.Target),
		platformcomponents.ResolverConfigurationAttemptKey(task.Target)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return false, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return false, nil, errs.New(errs.KindInternal, "resolver retention authority is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := make([]keyvalue.Condition, len(keys))
	for index, key := range keys {
		conditions[index] = keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])}
	}
	if read.Values[0] != nil {
		observation, err := platformcomponents.DecodeComponentObservation(read.Values[0].Value)
		if err != nil {
			return false, nil, err
		}
		if observation.ComponentID != task.Target {
			return false, nil, errs.New(errs.KindInternal, "resolver retention observation has the wrong owner")
		}
		if observation.TaskID == task.ID {
			return true, conditions, nil
		}
	}
	if read.Values[1] != nil {
		attempt, err := platformcomponents.DecodeResolverConfigurationAttempt(read.Values[1].Value)
		if err != nil {
			return false, nil, err
		}
		if attempt.TaskID == task.ID || attempt.PredecessorTaskID == task.ID {
			return true, conditions, nil
		}
	}
	return false, conditions, nil
}
