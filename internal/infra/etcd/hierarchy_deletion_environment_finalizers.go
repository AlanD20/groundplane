package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	groupstore "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionReleaseGroupFinalizer(
	ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, groupstore.ReleaseGroupRecordKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := groupstore.DecodeReleaseGroupStored(primary.Value)
	if err != nil || record.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, []string{
		groupstore.ReleaseGroupOwnerKey(record.EnvironmentID, record.ID),
		groupstore.ReleaseGroupNameKey(record.EnvironmentID, record.Name),
	})
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionEnvironmentFinalizer(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	if err := cleanupEnvironmentDeletionScriptLocators(
		ctx, repository.store, action.TargetID, etcdstore.Condition{
			Key:         hierarchydeletion.HierarchyDeletionTombstoneKey(string(operation.Tombstone.TargetKind), action.TargetID),
			ModRevision: operation.TombstoneRevision,
		},
	); err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	primary, err := repository.readHierarchyDeletionPrimary(ctx, hierarchyrecord.EnvironmentKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := hierarchyrecord.DecodeEnvironment(primary.Value)
	if err != nil || record.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		hierarchyrecord.EnvironmentNameKey(record.ProjectID, record.Name), hierarchyrecord.EnvironmentOwnerKey(record.ProjectID, record.ID),
		blueprints.EnvironmentBlueprintHeadKey(record.ID), projectionrecord.EnvironmentComposeProjectionStorageKey(record.ID),
		groupstore.ReleaseGroupCollectionEpochKey(record.ID),
	}})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if indexes == nil || len(indexes.Values) != 5 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != record.ID || string(indexes.Values[1].Value) != record.ID {
		if indexes != nil {
			etcdstore.ClearValues(indexes.Values)
		}
		return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer etcdstore.ClearValues(indexes.Values)
	if err := requireEnvironmentDeletionLiveAuthorityEmpty(
		ctx, repository.store, record.ID, operation.Tombstone.OperationID, indexes.ReadRevision,
	); err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	revisions, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: blueprints.EnvironmentBlueprintRevisionsPrefix(record.ID), Limit: 1, Revision: indexes.ReadRevision,
	})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if revisions == nil || revisions.ReadRevision != indexes.ReadRevision || len(revisions.Values) != 0 {
		if revisions != nil {
			etcdstore.ClearRangeValues(revisions.Values)
		}
		return hierarchyDeletionControllerEffects{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion Environment retained Blueprint revisions",
		)
	}
	conditions := []etcdstore.Condition{
		{Key: primary.Key, ModRevision: primary.ModRevision},
		{Key: indexes.Values[0].Key, ModRevision: indexes.Values[0].ModRevision},
		{Key: indexes.Values[1].Key, ModRevision: indexes.Values[1].ModRevision},
		{Key: blueprints.EnvironmentBlueprintHeadKey(record.ID), ModRevision: etcdstore.RevisionOf(indexes.Values[2])},
		{Key: projectionrecord.EnvironmentComposeProjectionStorageKey(record.ID), ModRevision: etcdstore.RevisionOf(indexes.Values[3])},
		{Key: groupstore.ReleaseGroupCollectionEpochKey(record.ID), ModRevision: etcdstore.RevisionOf(indexes.Values[4])},
	}
	conditions = append(
		conditions,
		environmentDeletionLiveAuthorityConditions(record.ID, operation.Tombstone.OperationID)...)
	conditions = append(conditions, etcdstore.Condition{Key: blueprints.EnvironmentBlueprintRevisionsPrefix(record.ID), Prefix: true})
	conditions = append(conditions, etcdstore.Condition{Key: scriptrecord.ScriptEnvironmentLocatorPrefixFor(record.ID), Prefix: true})
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: blueprints.EnvironmentBlueprintHeadKey(record.ID)},
		{Type: etcdstore.MutationDelete, Key: projectionrecord.EnvironmentComposeProjectionStorageKey(record.ID)},
		{Type: etcdstore.MutationDelete, Key: groupstore.ReleaseGroupCollectionEpochKey(record.ID)},
		{Type: etcdstore.MutationDelete, Key: hierarchyrecord.EnvironmentNameKey(record.ProjectID, record.Name)},
		{Type: etcdstore.MutationDelete, Key: hierarchyrecord.EnvironmentOwnerKey(record.ProjectID, record.ID)},
		{Type: etcdstore.MutationDelete, Key: hierarchyrecord.EnvironmentKey(record.ID)},
		{Type: etcdstore.MutationDelete, Key: scriptrecord.ScriptSetEnvironmentPrefix(record.ID), Prefix: true},
		{Type: etcdstore.MutationDelete, Key: scriptrecord.ScriptEnvironmentLocatorPrefixFor(record.ID), Prefix: true},
	}
	return hierarchyDeletionControllerEffects{
		fixedInputDigest: hierarchyDeletionBytesDigest(primary.Value), conditions: conditions, mutations: mutations,
	}, nil
}
