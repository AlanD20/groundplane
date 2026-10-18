package releasegroups

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RemovalRepository struct{ store preparationStore }

func NewRemovalRepository(store preparationStore) (*RemovalRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Release Group removal storage is required")
	}
	return &RemovalRepository{store: store}, nil
}

func (repository *RemovalRepository) Load(ctx context.Context, view workqueue.View) (RemovalInput, keyvalue.Condition, error) {
	if view.ReadRevision <= 0 {
		return RemovalInput{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Release Group removal snapshot is unavailable")
	}
	key := RemovalInputKey(view.Item.Item.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return RemovalInput{}, keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return RemovalInput{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Release Group removal capture is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	input, err := DecodeRemovalInput(read.Values[0].Value)
	if err != nil {
		return RemovalInput{}, keyvalue.Condition{}, err
	}
	if err := input.Match(view.Task.Task, view.Item.Item); err != nil {
		return RemovalInput{}, keyvalue.Condition{}, err
	}
	return input, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}

// PrepareDecision settles the captured definition and its deletion fence in the
// same transaction as the native result. Cancellation preserves the definition.
func (repository *RemovalRepository) PrepareDecision(ctx context.Context, view workqueue.View, outcome workexecution.Outcome, at time.Time) (workqueue.Change, error) {
	input, capture, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	item, task := view.Item.Item, view.Task.Task
	if item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Cleanup != workexecution.CleanupNone ||
		outcome == workexecution.Succeeded && (task.Stop != nil || item.ForwardRevoked || !at.Before(task.Deadline) || !at.Before(item.Deadline)) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Release Group removal is not authorized for atomic settlement")
	}
	tombstoneKey := deletions.TombstoneKey(string(deletions.DeletionTargetReleaseGroup), input.GroupID)
	keys := []string{ReleaseGroupRecordKey(input.GroupID), tombstoneKey,
		ReleaseGroupOwnerKey(input.Scope.EnvironmentID, input.GroupID), ReleaseGroupNameKey(input.Scope.EnvironmentID, input.GroupName),
		hierarchy.EnvironmentMutationEpochKey(input.Scope.EnvironmentID), hierarchy.EnvironmentKey(input.Scope.EnvironmentID),
		deletions.TombstoneKey("environment", input.Scope.EnvironmentID), deletions.TombstoneKey("project", input.Scope.ProjectID),
		deletions.TombstoneKey("tenant", input.Scope.TenantID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Release Group removal state is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	for index := 0; index < 6; index++ {
		if read.Values[index] == nil {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Release Group removal lost its resource or indexes")
		}
	}
	if read.Values[0].ModRevision != input.GroupRevision || string(read.Values[2].Value) != input.GroupID || string(read.Values[3].Value) != input.GroupID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Release Group changed after removal was accepted")
	}
	group, err := DecodeReleaseGroupStored(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	environment, err := hierarchy.DecodeEnvironment(read.Values[5].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	tombstone, err := deletions.DecodeDeletionTombstone(read.Values[1].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if group.ID != input.GroupID || group.Name != input.GroupName || group.EnvironmentID != input.Scope.EnvironmentID ||
		environment.ID != input.Scope.EnvironmentID || environment.ProjectID != input.Scope.ProjectID ||
		tombstone.TargetKind != deletions.DeletionTargetReleaseGroup || tombstone.TargetID != input.GroupID ||
		tombstone.TargetRevision != input.GroupRevision || tombstone.TaskID != input.TaskID || tombstone.Phase != deletions.DeletionPhaseFinalizing {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Release Group removal lost its captured ownership")
	}
	change, err := workqueue.PrepareUnstartedSettlement(view.Task, view.Item, workexecution.Completion{Outcome: outcome, Evidence: *item.Input, EffectsSettled: true}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, capture)
	for index, key := range keys {
		condition := keyvalue.Condition{Key: key}
		if read.Values[index] != nil {
			condition.ModRevision = read.Values[index].ModRevision
		}
		change.Conditions = append(change.Conditions, condition)
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: tombstoneKey})
	if outcome != workexecution.Succeeded {
		return change, nil
	}
	for index := 6; index < len(keys); index++ {
		if read.Values[index] != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, errs.New(errs.KindResourceInUse, "Release Group hierarchy is being removed")
		}
	}
	epoch, err := backupruntime.DecodeEnvironmentMutationEpochRecord(read.Values[4].Value)
	if err != nil || epoch.EnvironmentID != input.Scope.EnvironmentID {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, errs.New(errs.KindInternal, "Release Group Environment mutation epoch is corrupt")
	}
	value, err := backupruntime.EncodeEnvironmentMutationEpochRecord(epoch)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	condition, mutation, err := LoadReleaseGroupCollectionEpoch(ctx, repository.store, input.Scope.EnvironmentID, view.ReadRevision)
	if err != nil {
		clear(value)
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, condition)
	for _, index := range []int{0, 2, 3} {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[index]})
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: keys[4], Value: value}, mutation)
	return change, nil
}
