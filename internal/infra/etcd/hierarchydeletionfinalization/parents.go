package hierarchydeletionfinalization

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	secrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
)

func (repository *Preparer) prepareHierarchyDeletionTenantFinalizer(
	ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (Effects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, hierarchyrecord.TenantKey(action.TargetID), action)
	if err != nil {
		return Effects{}, err
	}
	defer clear(primary.Value)
	record, err := hierarchyrecord.DecodeTenant(primary.Value)
	if err != nil || record.ID != action.TargetID {
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	prefixes := []string{
		hierarchyrecord.ProjectTenantOwnerPrefix(record.ID), runnerrecord.RunnerOwnerPrefix(runnerrecord.RunnerOwnerTenant, record.ID),
	}
	if _, err := repository.requireHierarchyDeletionPrefixesEmpty(ctx, prefixes); err != nil {
		return Effects{}, err
	}
	slugKey := hierarchyrecord.TenantSlugKey(record.Slug)
	slug, err := repository.store.Get(ctx, slugKey)
	if err != nil {
		return Effects{}, err
	}
	if slug.Entry == nil || string(slug.Entry.Value) != record.ID {
		if slug.Entry != nil {
			clear(slug.Entry.Value)
		}
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer clear(slug.Entry.Value)
	conditions := []etcdstore.Condition{
		{Key: primary.Key, ModRevision: primary.ModRevision},
		{Key: slugKey, ModRevision: slug.Entry.ModRevision},
	}
	for _, prefix := range prefixes {
		conditions = append(conditions, etcdstore.Condition{Key: prefix, Prefix: true})
	}
	return Effects{
		fixedInputDigest: hierarchydeletion.HierarchyDeletionBytesDigest(primary.Value), conditions: conditions,
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationDelete, Key: slugKey},
			{Type: etcdstore.MutationDelete, Key: hierarchyrecord.TenantKey(record.ID)},
		},
	}, nil
}

func (repository *Preparer) prepareHierarchyDeletionProjectFinalizer(
	ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (Effects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, hierarchyrecord.ProjectKey(action.TargetID), action)
	if err != nil {
		return Effects{}, err
	}
	defer clear(primary.Value)
	record, err := hierarchyrecord.DecodeProject(primary.Value)
	if err != nil || record.ID != action.TargetID {
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	prefixes := []string{
		hierarchyrecord.EnvironmentOwnerPrefix(record.ID), runnerrecord.RunnerOwnerPrefix(runnerrecord.RunnerOwnerProject, record.ID),
		secrets.SecretOwnerCollectionPrefix(core.SecretScopeProject, record.ID),
	}
	if _, err := repository.requireHierarchyDeletionPrefixesEmpty(ctx, prefixes); err != nil {
		return Effects{}, err
	}
	indexes, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{hierarchyrecord.ProjectSlugKey(record), hierarchyrecord.ProjectOwnerKey(record)}},
	)
	if err != nil {
		return Effects{}, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != record.ID || string(indexes.Values[1].Value) != record.ID {
		if indexes != nil {
			etcdstore.ClearValues(indexes.Values)
		}
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer etcdstore.ClearValues(indexes.Values)
	conditions := []etcdstore.Condition{
		{Key: primary.Key, ModRevision: primary.ModRevision},
		{Key: indexes.Values[0].Key, ModRevision: indexes.Values[0].ModRevision},
		{Key: indexes.Values[1].Key, ModRevision: indexes.Values[1].ModRevision},
	}
	for _, prefix := range prefixes {
		conditions = append(conditions, etcdstore.Condition{Key: prefix, Prefix: true})
	}
	return Effects{
		fixedInputDigest: hierarchydeletion.HierarchyDeletionBytesDigest(primary.Value), conditions: conditions,
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationDelete, Key: hierarchyrecord.ProjectSlugKey(record)},
			{Type: etcdstore.MutationDelete, Key: hierarchyrecord.ProjectOwnerKey(record)},
			{Type: etcdstore.MutationDelete, Key: hierarchyrecord.ProjectKey(record.ID)},
		},
	}, nil
}
