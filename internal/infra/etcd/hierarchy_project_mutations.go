package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// MutateProjectIdempotent atomically commits the only tenant-Project direct
// mutations: display-name edit or scoped slug rename. Stable ownership,
// description, kind, descendants, and materialized state cannot move here.
func (repository *HierarchyRepository) MutateProjectIdempotent(
	ctx context.Context,
	current Versioned[ProjectRecord],
	replacement ProjectRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateProject(current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateProject(replacement); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if current.Record.Kind != ProjectKindTenant || replacement.Kind != ProjectKindTenant {
		return IdempotencyTransactionResult{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	if current.Record.ID != replacement.ID || current.Record.TenantID != replacement.TenantID ||
		current.Record.Description != replacement.Description || current.Revision <= 0 ||
		current.ReadRevision < current.Revision {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Project mutation identity is invalid")
	}
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != IdempotencyScopeProject || marker.Locator.ScopeID != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Project mutation marker must be a completed Project-scoped direct mutation",
		)
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	renaming := current.Record.Slug != replacement.Slug
	secondaryKeys := []string{
		projectSlugKey(current.Record),
		projectOwnerKey(current.Record),
		deletionTombstoneKey("project", current.Record.ID),
		deletionTombstoneKey("tenant", current.Record.TenantID),
	}
	if renaming {
		secondaryKeys = append(secondaryKeys, projectSlugKey(replacement))
	}
	secondary, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: secondaryKeys, Revision: current.ReadRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if len(secondary.Values) != len(secondaryKeys) || secondary.Values[0] == nil ||
		string(secondary.Values[0].Value) != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Project slug index is missing or mismatched")
	}
	if secondary.Values[1] == nil || string(secondary.Values[1].Value) != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Project owner index is missing or mismatched")
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
	value, err := encodeProject(replacement)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	conditions := []Condition{
		{Key: projectKey(current.Record.ID), ModRevision: current.Revision},
		{Key: projectSlugKey(current.Record), ModRevision: secondary.Values[0].ModRevision},
		{Key: projectOwnerKey(current.Record), ModRevision: secondary.Values[1].ModRevision},
		{Key: deletionTombstoneKey("project", current.Record.ID)},
		{Key: deletionTombstoneKey("tenant", current.Record.TenantID)},
	}
	mutations := []Mutation{{Type: MutationPut, Key: projectKey(current.Record.ID), Value: value}}
	if renaming {
		conditions = append(conditions, Condition{Key: projectSlugKey(replacement)})
		mutations = append(mutations,
			Mutation{Type: MutationDelete, Key: projectSlugKey(current.Record)},
			Mutation{Type: MutationPut, Key: projectSlugKey(replacement), Value: []byte(current.Record.ID)},
		)
	}
	plan, err := newIdempotencyMutationPlan(
		conditions,
		mutations,
		classifyProjectMutationConflict(current, replacement, renaming),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func classifyProjectMutationConflict(
	current Versioned[ProjectRecord],
	replacement ProjectRecord,
	renaming bool,
) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
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
			return stateConflict("project", current.Record.ID)
		}
		if values[1] == nil || string(values[1].Value) != current.Record.ID {
			return errs.New(errs.KindInternal, "Project slug index is missing or mismatched")
		}
		if values[2] == nil || string(values[2].Value) != current.Record.ID {
			return errs.New(errs.KindInternal, "Project owner index is missing or mismatched")
		}
		return stateConflict("project", current.Record.ID)
	}
}
