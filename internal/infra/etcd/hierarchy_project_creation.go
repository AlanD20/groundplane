package etcd

import (
	"context"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateProjectIdempotent atomically creates one tenant-owned Project, its
// scoped indexes, and the exact replay marker while fencing the owning Tenant
// and its deletion tombstone.
func (repository *HierarchyRepository) CreateProjectIdempotent(
	ctx context.Context,
	record hierarchyrecord.ProjectRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := hierarchyrecord.ValidateProject(record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if record.Kind != hierarchyrecord.ProjectKindTenant {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "direct Project creation requires a tenant-owned Project",
		)
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Project creation marker must be a completed direct mutation",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	owner, err := repository.GetTenant(ctx, record.TenantID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	tombstoneKey := deletions.TombstoneKey("tenant", record.TenantID)
	ownerState, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{tombstoneKey}, Revision: owner.ReadRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if len(ownerState.Values) != 1 {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Tenant deletion evidence is incomplete")
	}
	if ownerState.Values[0] != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
	}
	value, err := hierarchyrecord.EncodeProject(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	coordinationValue, err := encodeInitialHierarchyCoordination(hierarchydeletion.HierarchyDeletionTargetProject, record.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(coordinationValue)
	coordinationKey := hierarchydeletion.HierarchyCoordinationKey(string(hierarchydeletion.HierarchyDeletionTargetProject), record.ID)
	plan, err := NewIdempotencyMutationPlan(
		[]etcdstore.Condition{
			{Key: hierarchyrecord.ProjectKey(record.ID)},
			{Key: hierarchyrecord.ProjectSlugKey(record)},
			{Key: hierarchyrecord.ProjectOwnerKey(record)},
			{Key: hierarchyrecord.TenantKey(record.TenantID), ModRevision: owner.Revision},
			{Key: tombstoneKey},
			{Key: coordinationKey},
		},
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: hierarchyrecord.ProjectKey(record.ID), Value: value},
			{Type: etcdstore.MutationPut, Key: hierarchyrecord.ProjectSlugKey(record), Value: []byte(record.ID)},
			{Type: etcdstore.MutationPut, Key: hierarchyrecord.ProjectOwnerKey(record), Value: []byte(record.ID)},
			{Type: etcdstore.MutationPut, Key: coordinationKey, Value: coordinationValue},
		},
		classifyProjectCreateConflict(record, owner.Revision),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func classifyProjectCreateConflict(record hierarchyrecord.ProjectRecord, ownerRevision int64) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != 6 {
			return errs.New(errs.KindInternal, "Project creation compare evidence is incomplete")
		}
		if values[0] != nil {
			return errs.New(errs.KindInternal, "generated Project id collided with durable state")
		}
		if values[1] != nil {
			return errs.Newf(errs.KindSlugConflict, "project slug %q already exists", record.Slug)
		}
		if values[2] != nil {
			return errs.New(errs.KindInternal, "generated Project owner index collided with durable state")
		}
		if values[3] == nil {
			return errs.New(errs.KindTenantNotFound, "Tenant was not found")
		}
		if values[4] != nil {
			return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
		}
		if values[5] != nil {
			return errs.New(errs.KindInternal, "generated Project coordination identity collided with durable state")
		}
		if values[3].ModRevision != ownerRevision {
			return recordcodec.StateConflict("tenant", record.TenantID)
		}
		return errs.New(errs.KindInternal, "Project creation compare failure was not classified")
	}
}

func (repository *HierarchyRepository) CreateProject(
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
		owner, err := repository.GetTenant(ctx, record.TenantID)
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
	coordinationValue, err := encodeInitialHierarchyCoordination(coordinationTarget, record.ID)
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
