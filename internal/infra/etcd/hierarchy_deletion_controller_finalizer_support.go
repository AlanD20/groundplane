package etcd

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) readHierarchyDeletionPrimary(
	ctx context.Context,
	key string,
	action hierarchydeletion.HierarchyDeletionAction,
) (*etcdstore.KeyValue, error) {
	result, err := repository.store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if result == nil || result.Entry == nil || result.Entry.Key != key {
		if result != nil && result.Entry != nil {
			clear(result.Entry.Value)
		}
		return nil, errs.New(errs.KindStateConflict, "hierarchy deletion finalizer target changed")
	}
	if result.Entry.ModRevision != action.TargetRevision {
		original, err := hierarchyDeletionOriginalRootValue(action, result.Entry.Value)
		if err != nil {
			clear(result.Entry.Value)
			return nil, errs.New(errs.KindStateConflict, "hierarchy deletion finalizer target changed")
		}
		clear(result.Entry.Value)
		result.Entry.Value = original
	}
	return result.Entry, nil
}

func hierarchyDeletionOriginalRootValue(action hierarchydeletion.HierarchyDeletionAction, value []byte) ([]byte, error) {
	switch action.ActionKind {
	case hierarchydeletion.HierarchyDeletionTenantFinalize:
		record, err := hierarchyrecord.DecodeTenant(value)
		if err != nil || record.ID != action.TargetID || record.DeletionTaskID == "" {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		record.DeletionTaskID = ""
		return hierarchyrecord.EncodeTenant(record)
	case hierarchydeletion.HierarchyDeletionProjectFinalize, hierarchydeletion.HierarchyDeletionBackingServiceFinalize:
		record, err := hierarchyrecord.DecodeProject(value)
		if err != nil || record.ID != action.TargetID || record.DeletionTaskID == "" {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		record.DeletionTaskID = ""
		return hierarchyrecord.EncodeProject(record)
	case hierarchydeletion.HierarchyDeletionEnvironmentFinalize:
		record, err := hierarchyrecord.DecodeEnvironment(value)
		if err != nil || record.ID != action.TargetID || record.DeletionTaskID == "" {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		record.DeletionTaskID = ""
		return hierarchyrecord.EncodeEnvironment(record)
	default:
		return nil, errs.New(errs.KindStateConflict, "hierarchy deletion finalizer target changed")
	}
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionIndexedDelete(
	ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
	primary *etcdstore.KeyValue,
	indexKeys []string,
) (hierarchyDeletionControllerEffects, error) {
	conditions := []etcdstore.Condition{{Key: primary.Key, ModRevision: primary.ModRevision}}
	mutations := make([]etcdstore.Mutation, 0, len(indexKeys)+1)
	if len(indexKeys) == 0 {
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: primary.Key})
		return hierarchyDeletionControllerEffects{
			fixedInputDigest: hierarchyDeletionBytesDigest(primary.Value),
			conditions:       conditions,
			mutations:        mutations,
		}, nil
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: indexKeys})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if indexes == nil || len(indexes.Values) != len(indexKeys) {
		if indexes != nil {
			clearKeyValues(indexes.Values)
		}
		return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer clearKeyValues(indexes.Values)
	for index, key := range indexKeys {
		value := indexes.Values[index]
		if value == nil || value.Key != key || (index < 2 && string(value.Value) != action.TargetID) {
			return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: value.ModRevision})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
	}
	mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: primary.Key})
	return hierarchyDeletionControllerEffects{
		fixedInputDigest: hierarchyDeletionBytesDigest(primary.Value), conditions: conditions, mutations: mutations,
	}, nil
}

func (repository *HierarchyDeletionRepository) requireHierarchyDeletionPrefixesEmpty(
	ctx context.Context,
	prefixes []string,
) (int64, error) {
	revision := int64(0)
	for _, prefix := range prefixes {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{Prefix: prefix, Limit: 1, Revision: revision})
		if err != nil {
			return 0, err
		}
		if page == nil || len(page.Values) != 0 || (revision != 0 && page.ReadRevision != revision) {
			if page != nil {
				clearRangeValues(page.Values)
			}
			return 0, errs.New(errs.KindStateConflict, "hierarchy deletion finalizer retained descendants")
		}
		revision = page.ReadRevision
	}
	return revision, nil
}

func hierarchyDeletionConnectorReferencePrefixes(connectorID string) []string {
	return []string{
		backuppolicy.BackupPolicyConnectorReferencePrefix(connectorID),
		backupruntime.BackupRecoveryPointConnectorPrefix + connectorID + "/",
		backupruntime.BackupOrphanConnectorPrefix + connectorID + "/",
	}
}

func clearRunnerAllocationEvidence(evidence runnerAllocationEvidence) {
	for _, value := range []*etcdstore.KeyValue{evidence.owner, evidence.slug, evidence.quota, evidence.host, evidence.system} {
		if value != nil {
			clear(value.Value)
		}
	}
}
