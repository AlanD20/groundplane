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

// MutateProjectIdempotent atomically commits the only tenant-Project direct
// mutations: display-name edit or scoped slug rename. Stable ownership,
// description, kind, descendants, and materialized state cannot move here.
func (repository *HierarchyRepository) MutateProjectIdempotent(
	ctx context.Context,
	current etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	replacement hierarchyrecord.ProjectRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := hierarchyrecord.ValidateProject(current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := hierarchyrecord.ValidateProject(replacement); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if current.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		replacement.Kind != hierarchyrecord.ProjectKindTenant {
		return IdempotencyTransactionResult{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	if current.Record.ID != replacement.ID || current.Record.TenantID != replacement.TenantID ||
		current.Record.Description != replacement.Description || current.Revision <= 0 ||
		current.ReadRevision < current.Revision {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Project mutation identity is invalid",
		)
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect ||
		marker.State != idempotencyrecord.IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeProject ||
		marker.Locator.ScopeID != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Project mutation marker must be a completed Project-scoped direct mutation",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	renaming := current.Record.Slug != replacement.Slug
	secondaryKeys := []string{
		hierarchyrecord.ProjectSlugKey(current.Record),
		hierarchyrecord.ProjectOwnerKey(current.Record),
		deletions.TombstoneKey("project", current.Record.ID),
		deletions.TombstoneKey("tenant", current.Record.TenantID),
	}
	if renaming {
		secondaryKeys = append(secondaryKeys, hierarchyrecord.ProjectSlugKey(replacement))
	}
	secondary, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: secondaryKeys, Revision: current.ReadRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if len(secondary.Values) != len(secondaryKeys) || secondary.Values[0] == nil ||
		string(secondary.Values[0].Value) != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"Project slug index is missing or mismatched",
		)
	}
	if secondary.Values[1] == nil || string(secondary.Values[1].Value) != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"Project owner index is missing or mismatched",
		)
	}
	if secondary.Values[2] != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindResourceInUse, "Project deletion is in progress")
	}
	if secondary.Values[3] != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
	}
	if renaming && secondary.Values[4] != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindSlugConflict, "Project slug is already in use")
	}
	value, err := hierarchyrecord.EncodeProject(replacement)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	conditions := []etcdstore.Condition{
		{Key: hierarchyrecord.ProjectKey(current.Record.ID), ModRevision: current.Revision},
		{Key: hierarchyrecord.ProjectSlugKey(current.Record), ModRevision: secondary.Values[0].ModRevision},
		{Key: hierarchyrecord.ProjectOwnerKey(current.Record), ModRevision: secondary.Values[1].ModRevision},
		{Key: deletions.TombstoneKey("project", current.Record.ID)},
		{Key: deletions.TombstoneKey("tenant", current.Record.TenantID)},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.ProjectKey(current.Record.ID), Value: value},
	}
	if renaming {
		conditions = append(conditions, etcdstore.Condition{Key: hierarchyrecord.ProjectSlugKey(replacement)})
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: hierarchyrecord.ProjectSlugKey(current.Record)},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   hierarchyrecord.ProjectSlugKey(replacement),
				Value: []byte(current.Record.ID),
			},
		)
	}
	plan, err := NewIdempotencyMutationPlan(
		conditions,
		mutations,
		classifyProjectMutationConflict(current, replacement, renaming),
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

func classifyProjectMutationConflict(
	current etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	replacement hierarchyrecord.ProjectRecord,
	renaming bool,
) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		expected := 5
		if renaming {
			expected++
		}
		if len(values) != expected {
			return errs.New(errs.KindInternal, "Project mutation compare evidence is incomplete")
		}
		if values[0] == nil {
			return errs.New(errs.KindProjectNotFound, "project was not found")
		}
		if values[3] != nil {
			return errs.New(errs.KindResourceInUse, "Project deletion is in progress")
		}
		if values[4] != nil {
			return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
		}
		if renaming && values[5] != nil {
			return errs.Newf(errs.KindSlugConflict, "project slug %q already exists", replacement.Slug)
		}
		if values[0].ModRevision != current.Revision {
			return recordcodec.StateConflict("project", current.Record.ID)
		}
		if values[1] == nil || string(values[1].Value) != current.Record.ID {
			return errs.New(errs.KindInternal, "Project slug index is missing or mismatched")
		}
		if values[2] == nil || string(values[2].Value) != current.Record.ID {
			return errs.New(errs.KindInternal, "Project owner index is missing or mismatched")
		}
		return recordcodec.StateConflict("project", current.Record.ID)
	}
}
