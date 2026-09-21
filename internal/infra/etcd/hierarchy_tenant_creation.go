package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateTenantIdempotent atomically claims the direct HTTP marker and creates
// the Tenant primary plus globally unique slug index.
func (repository *HierarchyRepository) CreateTenantIdempotent(
	ctx context.Context,
	record hierarchyrecord.TenantRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := hierarchyrecord.ValidateTenant(record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect ||
		marker.State != idempotencyrecord.IdempotencyMarkerCompleted {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Tenant creation marker must be a completed direct mutation",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	value, err := hierarchyrecord.EncodeTenant(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	coordinationValue, err := hierarchydeletion.EncodeInitialCoordination(
		hierarchydeletion.HierarchyDeletionTargetTenant,
		record.ID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(coordinationValue)
	coordinationKey := hierarchydeletion.HierarchyCoordinationKey(
		string(hierarchydeletion.HierarchyDeletionTargetTenant),
		record.ID,
	)
	plan, err := NewIdempotencyMutationPlan(
		[]etcdstore.Condition{
			{Key: hierarchyrecord.TenantKey(record.ID)},
			{Key: hierarchyrecord.TenantSlugKey(record.Slug)},
			{Key: coordinationKey},
		},
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: hierarchyrecord.TenantKey(record.ID), Value: value},
			{Type: etcdstore.MutationPut, Key: hierarchyrecord.TenantSlugKey(record.Slug), Value: []byte(record.ID)},
			{Type: etcdstore.MutationPut, Key: coordinationKey, Value: coordinationValue},
		},
		classifyTenantCreateConflict(record.Slug),
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

func classifyTenantCreateConflict(slug string) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != 3 {
			return errs.New(errs.KindInternal, "Tenant creation compare evidence is incomplete")
		}
		if values[0] != nil {
			return errs.New(errs.KindInternal, "generated Tenant id collided with durable state")
		}
		if values[1] != nil {
			return errs.Newf(errs.KindSlugConflict, "tenant slug %q already exists", slug)
		}
		if values[2] != nil {
			return errs.New(errs.KindInternal, "generated Tenant coordination identity collided with durable state")
		}
		return errs.New(errs.KindInternal, "Tenant creation compare failure was not classified")
	}
}
