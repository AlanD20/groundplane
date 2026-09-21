package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
	"time"
)

func (repository *TaskRepository) finalizeEnvironmentBlueprintRevisionBatch(
	ctx context.Context,
	task TaskRecord,
	updatedAt time.Time,
) (bool, error) {
	prefix := environmentBlueprintRevisionsPrefix(task.Target)
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, Limit: environmentBlueprintDeletionBatchSize,
	})
	if err != nil {
		return false, err
	}
	if page == nil || page.ReadRevision <= 0 ||
		len(page.Values) > int(environmentBlueprintDeletionBatchSize) {
		return false, errs.New(errs.KindInternal, "environment Blueprint deletion page is invalid")
	}
	if len(page.Values) == 0 {
		return false, nil
	}
	tombstoneResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), task.Target),
			hierarchyrecord.EnvironmentMutationEpochKey(task.Target),
			hierarchyrecord.EnvironmentOperationLockKey(task.Target),
		},
		Revision: page.ReadRevision,
	})
	if err != nil {
		return false, err
	}
	if tombstoneResult == nil || tombstoneResult.ReadRevision != page.ReadRevision ||
		len(tombstoneResult.Values) != 3 {
		return false, errs.New(
			errs.KindInternal,
			"environment deletion fencing evidence is invalid",
		)
	}
	if tombstoneResult.Values[0] == nil {
		return false, errs.New(errs.KindStateConflict, "environment deletion tombstone is missing")
	}
	if tombstoneResult.Values[1] == nil {
		return false, errs.New(errs.KindInternal, "environment mutation epoch is missing")
	}
	if tombstoneResult.Values[2] == nil {
		return false, errs.New(
			errs.KindStateConflict,
			"environment deletion operation lock is missing",
		)
	}
	tombstoneValue := tombstoneResult.Values[0]
	tombstone, err := deletionrecord.DecodeDeletionTombstone(tombstoneValue.Value)
	if err != nil {
		return false, err
	}
	if tombstone.TargetKind != deletionrecord.DeletionTargetEnvironment || tombstone.TargetID != task.Target ||
		tombstone.TaskID != task.ID ||
		(tombstone.Phase != deletionrecord.DeletionPhaseHostEffects && tombstone.Phase != deletionrecord.DeletionPhaseFinalizing) {
		return false, errs.New(
			errs.KindStateConflict,
			"environment deletion tombstone does not match its Task",
		)
	}
	ownedFence, err := loadOwnedEnvironmentMutationFence(
		ctx,
		repository.store,
		task.Target,
		page.ReadRevision,
		environmentMutationFenceOwner{
			Kind: backupruntime.BackupOperationDeletion, OperationID: task.OperationID, TaskID: task.ID,
		},
	)
	if err != nil {
		return false, err
	}
	revisionID, err := environmentBlueprintRevisionIDFromKey(
		prefix,
		page.Values[len(page.Values)-1].Key,
	)
	if err != nil {
		return false, errs.New(errs.KindStateConflict,
			"environment deletion retained malformed published Blueprint revision evidence")
	}
	tombstone.Phase = deletionrecord.DeletionPhaseFinalizing
	tombstone.Checkpoint = deletionrecord.DeletionCheckpoint{
		ResourceKind: "blueprint_revision",
		StableID:     revisionID,
	}
	tombstone.UpdatedAt = updatedAt
	encodedTombstone, err := deletionrecord.EncodeDeletionTombstone(tombstone)
	if err != nil {
		return false, err
	}
	defer clear(encodedTombstone)

	conditions := make([]etcdstore.Condition, 0, len(page.Values)+3)
	mutations := make([]etcdstore.Mutation, 0, len(page.Values)+2)
	for _, value := range page.Values {
		conditions = append(conditions, etcdstore.Condition{Key: value.Key, ModRevision: value.ModRevision})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: value.Key})
	}
	conditions = append(conditions, etcdstore.Condition{
		Key:         deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), task.Target),
		ModRevision: tombstoneValue.ModRevision,
	})
	mutations = append(mutations, etcdstore.Mutation{
		Type:  etcdstore.MutationPut,
		Key:   deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), task.Target),
		Value: encodedTombstone,
	})
	conditions, err = appendEnvironmentMutationFenceConditions(conditions, ownedFence)
	if err != nil {
		return false, err
	}
	epochMutation, err := ownedFence.epochRewriteMutation()
	if err != nil {
		return false, err
	}
	defer clear(epochMutation.Value)
	mutations = append(mutations, epochMutation)
	if err := validateEnvironmentMutationTransactionBudget(conditions, mutations); err != nil {
		return false, err
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return false, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(
			errs.KindStateConflict,
			"environment Blueprint finalization state changed",
		)
	}
	return true, nil
}

func environmentBlueprintRevisionIDFromKey(prefix string, key string) (string, error) {
	remainder := strings.TrimPrefix(key, prefix)
	separator := strings.IndexByte(remainder, '/')
	if remainder == key || separator <= 0 ||
		recordcodec.ValidateID(ids.KindTask, remainder[:separator]) != nil {
		return "", errs.New(errs.KindInternal, "environment Blueprint revision key is corrupt")
	}
	return remainder[:separator], nil
}
