package hierarchymutations

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

func (repository *Repository) CreateTenant(
	ctx context.Context,
	record hierarchyrecord.TenantRecord,
) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[hierarchyrecord.TenantRecord]{}, err
	}
	if err := hierarchyrecord.ValidateTenant(record); err != nil {
		return etcdstore.Versioned[hierarchyrecord.TenantRecord]{}, err
	}
	value, err := recordcodec.Encode("tenant", record)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.TenantRecord]{}, err
	}
	coordinationValue, err := hierarchydeletion.EncodeInitialCoordination(
		hierarchydeletion.HierarchyDeletionTargetTenant,
		record.ID,
	)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.TenantRecord]{}, err
	}
	primary := hierarchyrecord.TenantKey(record.ID)
	slug := hierarchyrecord.TenantSlugKey(record.Slug)
	coordinationKey := hierarchydeletion.HierarchyCoordinationKey(
		string(hierarchydeletion.HierarchyDeletionTargetTenant),
		record.ID,
	)
	result, err := repository.store.Transact(ctx,
		[]etcdstore.Condition{{Key: primary}, {Key: slug}, {Key: coordinationKey}},
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: primary, Value: value},
			{Type: etcdstore.MutationPut, Key: slug, Value: []byte(record.ID)},
			{Type: etcdstore.MutationPut, Key: coordinationKey, Value: coordinationValue},
		},
	)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.TenantRecord]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[hierarchyrecord.TenantRecord]{}, repository.diagnoseCreate(ctx, primary, slug)
	}
	return etcdstore.Versioned[hierarchyrecord.TenantRecord]{
		Record:       record,
		Revision:     result.Revision,
		ReadRevision: result.Revision,
	}, nil
}
