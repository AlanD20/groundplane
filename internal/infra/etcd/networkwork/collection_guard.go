package networkwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// CollectionGuard runs before CollectionStartedAt is set. A retained deletion
// fence must preserve Retry eligibility, not merely block input deletion after
// the collector has already made the Task permanently non-retryable.
func (repository *InputRepository) CollectionGuard(ctx context.Context, task workqueue.TaskSnapshot,
	revision int64,
) (bool, workqueue.Change, error) {
	if task.Revision <= 0 || revision < task.Revision || task.Task.Action != workexecution.ActionRemove ||
		ids.Validate(ids.KindNetwork, task.Task.TargetID) != nil {
		return false, workqueue.Change{}, conflict("Zone collection requires its accepted removal Task")
	}
	key := deletions.TombstoneKey(string(deletions.DeletionTargetZone), task.Task.TargetID)
	read, err := repository.read(ctx, []string{key}, revision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] != nil {
		return false, workqueue.Change{}, nil
	}
	return true, workqueue.Change{Conditions: []keyvalue.Condition{{Key: key}}}, nil
}

// PrepareZoneTaskCollection removes the bounded aggregate records only after
// typed owners have retired every work item, its attempts and Secret pins.
func PrepareZoneTaskCollection(task workqueue.TaskSnapshot, guard workqueue.Change) (workqueue.Change, error) {
	key := deletions.TombstoneKey(string(deletions.DeletionTargetZone), task.Task.TargetID)
	if task.Task.Action != workexecution.ActionRemove || ids.Validate(ids.KindNetwork, task.Task.TargetID) != nil ||
		len(
			guard.Conditions,
		) != 1 || guard.Conditions[0] != (keyvalue.Condition{Key: key}) || len(guard.Mutations) != 0 {
		return workqueue.Change{}, conflict("Zone collection requires the deletion fence absence guard")
	}
	change, err := workqueue.PrepareTaskCollection(task)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guard.Conditions...)
	change.Mutations = append(change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: CascadeGraphPrefix(task.Task.ID), Prefix: true},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: attachremoval.CascadeProgressKey(task.Task.ID)})
	return change, nil
}
