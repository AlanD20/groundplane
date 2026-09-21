package hierarchymutations

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

func (repository *Repository) CreateProject(
	ctx context.Context,
	record hierarchyrecord.ProjectRecord,
) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	if err := hierarchyrecord.ValidateProject(record); err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: hierarchyrecord.ProjectKey(record.ID)},
		{Key: hierarchyrecord.ProjectSlugKey(record)},
		{Key: hierarchyrecord.ProjectOwnerKey(record)},
	}
	if record.Kind == hierarchyrecord.ProjectKindTenant {
		owner, err := repository.reader.GetTenant(ctx, record.TenantID)
		if err != nil {
			return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
		}
		conditions = append(conditions, etcdstore.Condition{Key: hierarchyrecord.TenantKey(record.TenantID), ModRevision: owner.Revision})
	}
	value, err := recordcodec.Encode("project", record)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	coordinationTarget := hierarchydeletion.HierarchyDeletionTargetProject
	coordinationValue, err := hierarchydeletion.EncodeInitialCoordination(coordinationTarget, record.ID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	coordinationKey := hierarchydeletion.HierarchyCoordinationKey(string(coordinationTarget), record.ID)
	conditions = append(conditions, etcdstore.Condition{Key: coordinationKey})
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.ProjectKey(record.ID), Value: value},
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.ProjectSlugKey(record), Value: []byte(record.ID)},
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.ProjectOwnerKey(record), Value: []byte(record.ID)},
		{Type: etcdstore.MutationPut, Key: coordinationKey, Value: coordinationValue},
	})
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, repository.diagnoseCreate(ctx, hierarchyrecord.ProjectKey(record.ID), hierarchyrecord.ProjectSlugKey(record))
	}
	return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{Record: record, Revision: result.Revision, ReadRevision: result.Revision}, nil
}
