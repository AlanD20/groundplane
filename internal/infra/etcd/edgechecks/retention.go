package edgechecks

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PreparePrune joins native Task retention, which owns its expiry and terminal
// proof. It never prunes a running check or helper ownership independently.
// Removing the selected latest result does not fall back to an older pass.
func (repository *Repository) PreparePrune(
	ctx context.Context,
	taskID string,
	revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	current, err := repository.LoadAt(ctx, taskID, revision)
	if err != nil {
		return nil, nil, err
	}
	record := current.Record
	if record.Phase != Finished && record.Phase != Transferred || record.Phase == Finished && record.CleanupPending {
		return nil, nil, errs.New(errs.KindResourceInUse, "edge diagnostic still owns execution or helper cleanup")
	}
	// Transferred is historical evidence, not the current helper owner. Its
	// operation must still be absent below: never prune it during a live Retry.
	keys := append([]string{OperationKey(record.Owner.OperationID), LatestKey(record.Source.Request.TargetID()),
		taskjournal.ControllerTaskClaimKey(
			taskID,
		), taskjournal.TaskAssignmentIndexKey(taskID)}, ReferenceKeys(record)...)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: current.ReadRevision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != current.ReadRevision || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return nil, nil, errs.New(errs.KindInternal, "edge diagnostic retention read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := []keyvalue.Condition{{Key: Key(taskID), ModRevision: current.Revision}}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: Key(taskID)}}
	for index, key := range keys {
		value := read.Values[index]
		if index == 1 {
			condition := keyvalue.Condition{Key: key}
			if value != nil {
				selected := string(value.Value)
				if ids.Validate(ids.KindTask, selected) != nil {
					return nil, nil, errs.New(errs.KindInternal, "edge diagnostic latest index is corrupt")
				}
				condition.ModRevision = value.ModRevision
				if selected == taskID {
					mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
				}
			}
			conditions = append(conditions, condition)
			continue
		}
		if value != nil {
			return nil, nil, errs.New(
				errs.KindResourceInUse,
				"edge diagnostic still has an active assignment or cleanup reference",
			)
		}
		conditions = append(conditions, keyvalue.Condition{Key: key})
	}
	return conditions, mutations, nil
}
