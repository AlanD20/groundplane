package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionReleaseGroupFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, releaseGroupRecordKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := decodeReleaseGroupStored(primary.Value)
	if err != nil || record.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	return repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, []string{
		releaseGroupOwnerKey(record.EnvironmentID, record.ID),
		releaseGroupNameKey(record.EnvironmentID, record.Name),
	})
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionEnvironmentFinalizer(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	if err := cleanupEnvironmentDeletionScriptLocators(
		ctx, repository.store, action.TargetID, Condition{
			Key:         HierarchyDeletionTombstoneKey(string(operation.Tombstone.TargetKind), action.TargetID),
			ModRevision: operation.TombstoneRevision,
		},
	); err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	primary, err := repository.readHierarchyDeletionPrimary(ctx, environmentKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := decodeEnvironment(primary.Value)
	if err != nil || record.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	indexes, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		environmentNameKey(record.ProjectID, record.Name), environmentOwnerKey(record.ProjectID, record.ID),
		environmentBlueprintHeadKey(record.ID), environmentComposeProjectionKey(record.ID),
		releaseGroupCollectionEpochKey(record.ID),
	}})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if indexes == nil || len(indexes.Values) != 5 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != record.ID || string(indexes.Values[1].Value) != record.ID {
		if indexes != nil {
			clearKeyValues(indexes.Values)
		}
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	defer clearKeyValues(indexes.Values)
	if err := requireEnvironmentDeletionLiveAuthorityEmpty(
		ctx, repository.store, record.ID, operation.Tombstone.OperationID, indexes.ReadRevision,
	); err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	revisions, err := repository.store.Range(ctx, RangeRequest{
		Prefix: environmentBlueprintRevisionsPrefix(record.ID), Limit: 1, Revision: indexes.ReadRevision,
	})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if revisions == nil || revisions.ReadRevision != indexes.ReadRevision || len(revisions.Values) != 0 {
		if revisions != nil {
			clearRangeValues(revisions.Values)
		}
		return hierarchyDeletionControllerEffects{}, errs.New(errs.KindStateConflict, "hierarchy deletion Environment retained Blueprint revisions")
	}
	conditions := []Condition{
		{Key: primary.Key, ModRevision: primary.ModRevision},
		{Key: indexes.Values[0].Key, ModRevision: indexes.Values[0].ModRevision},
		{Key: indexes.Values[1].Key, ModRevision: indexes.Values[1].ModRevision},
		{Key: environmentBlueprintHeadKey(record.ID), ModRevision: keyValueRevision(indexes.Values[2])},
		{Key: environmentComposeProjectionKey(record.ID), ModRevision: keyValueRevision(indexes.Values[3])},
		{Key: releaseGroupCollectionEpochKey(record.ID), ModRevision: keyValueRevision(indexes.Values[4])},
	}
	conditions = append(conditions, environmentDeletionLiveAuthorityConditions(record.ID, operation.Tombstone.OperationID)...)
	conditions = append(conditions, Condition{Key: environmentBlueprintRevisionsPrefix(record.ID), Prefix: true})
	conditions = append(conditions, Condition{Key: scriptEnvironmentLocatorPrefixFor(record.ID), Prefix: true})
	mutations := []Mutation{
		{Type: MutationDelete, Key: environmentBlueprintHeadKey(record.ID)},
		{Type: MutationDelete, Key: environmentComposeProjectionKey(record.ID)},
		{Type: MutationDelete, Key: releaseGroupCollectionEpochKey(record.ID)},
		{Type: MutationDelete, Key: environmentNameKey(record.ProjectID, record.Name)},
		{Type: MutationDelete, Key: environmentOwnerKey(record.ProjectID, record.ID)},
		{Type: MutationDelete, Key: environmentKey(record.ID)},
		{Type: MutationDelete, Key: scriptSetEnvironmentPrefix(record.ID), Prefix: true},
		{Type: MutationDelete, Key: scriptEnvironmentLocatorPrefixFor(record.ID), Prefix: true},
	}
	return hierarchyDeletionControllerEffects{
		fixedInputDigest: hierarchyDeletionBytesDigest(primary.Value), conditions: conditions, mutations: mutations,
	}, nil
}
