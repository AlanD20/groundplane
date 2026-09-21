package etcd

import (
	"context"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// MutateTenantIdempotent atomically commits a Tenant field edit or slug move
// with its completed direct idempotency marker and exact public response.
func (repository *HierarchyRepository) MutateTenantIdempotent(
	ctx context.Context,
	current etcdstore.Versioned[hierarchyrecord.TenantRecord],
	replacement hierarchyrecord.TenantRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := hierarchyrecord.ValidateTenant(current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := hierarchyrecord.ValidateTenant(replacement); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if current.Record.ID != replacement.ID || current.Revision <= 0 || current.ReadRevision <= 0 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Tenant mutation revision is invalid",
		)
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Tenant mutation marker must be a completed direct mutation",
		)
	}
	secondaryKeys := []string{
		hierarchyrecord.TenantSlugKey(current.Record.Slug),
		deletions.TombstoneKey("tenant", current.Record.ID),
	}
	renaming := current.Record.Slug != replacement.Slug
	if renaming {
		secondaryKeys = append(secondaryKeys, hierarchyrecord.TenantSlugKey(replacement.Slug))
	}
	secondary, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: secondaryKeys, Revision: current.ReadRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if len(secondary.Values) != len(secondaryKeys) || secondary.Values[0] == nil ||
		string(secondary.Values[0].Value) != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Tenant slug index is missing or mismatched")
	}
	if secondary.Values[1] != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
	}
	if renaming && secondary.Values[2] != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindSlugConflict, "Tenant slug is already in use")
	}
	value, err := hierarchyrecord.EncodeTenant(replacement)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	conditions := []etcdstore.Condition{
		{Key: hierarchyrecord.TenantKey(current.Record.ID), ModRevision: current.Revision},
		{Key: hierarchyrecord.TenantSlugKey(current.Record.Slug), ModRevision: secondary.Values[0].ModRevision},
		{Key: deletions.TombstoneKey("tenant", current.Record.ID)},
	}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: hierarchyrecord.TenantKey(current.Record.ID), Value: value}}
	if renaming {
		conditions = append(conditions, etcdstore.Condition{Key: hierarchyrecord.TenantSlugKey(replacement.Slug)})
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: hierarchyrecord.TenantSlugKey(current.Record.Slug)},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: hierarchyrecord.TenantSlugKey(replacement.Slug), Value: []byte(current.Record.ID)},
		)
	}
	plan, err := NewIdempotencyMutationPlan(
		conditions,
		mutations,
		classifyTenantMutationConflict(current, replacement, renaming),
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

func classifyTenantMutationConflict(
	current etcdstore.Versioned[hierarchyrecord.TenantRecord],
	replacement hierarchyrecord.TenantRecord,
	renaming bool,
) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		expected := 3
		if renaming {
			expected++
		}
		if len(values) != expected {
			return errs.New(errs.KindInternal, "Tenant mutation compare evidence is incomplete")
		}
		if values[0] == nil {
			return errs.New(errs.KindTenantNotFound, "Tenant was not found")
		}
		if values[2] != nil {
			return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
		}
		if renaming && values[3] != nil {
			return errs.Newf(errs.KindSlugConflict, "Tenant slug %q already exists", replacement.Slug)
		}
		if values[0].ModRevision != current.Revision {
			return recordcodec.StateConflict("tenant", current.Record.ID)
		}
		if values[1] == nil || string(values[1].Value) != current.Record.ID {
			return errs.New(errs.KindInternal, "Tenant slug index is missing or mismatched")
		}
		return recordcodec.StateConflict("tenant", current.Record.ID)
	}
}
