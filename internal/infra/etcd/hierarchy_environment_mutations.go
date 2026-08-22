package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// MutateEnvironmentIdempotent atomically commits the sole direct Environment
// mutation: changing its scoped name label. Identity, ownership, directory,
// provisioning, and create-Task state cannot move through this seam.
func (repository *HierarchyRepository) MutateEnvironmentIdempotent(
	ctx context.Context,
	current Versioned[EnvironmentRecord],
	replacement EnvironmentRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEnvironment(current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEnvironment(replacement); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if current.Record.ID != replacement.ID ||
		current.Record.ProjectID != replacement.ProjectID ||
		current.Record.VolumeDir != replacement.VolumeDir ||
		current.Record.ProvisioningState != replacement.ProvisioningState ||
		current.Record.CreateTaskID != replacement.CreateTaskID ||
		current.Record.CreatedAt != replacement.CreatedAt ||
		current.Revision <= 0 || current.ReadRevision < current.Revision {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Environment mutation identity is invalid",
		)
	}
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Environment mutation marker must be a completed Environment-scoped direct mutation",
		)
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	renaming := current.Record.Name != replacement.Name
	secondaryKeys := []string{
		environmentNameKey(current.Record.ProjectID, current.Record.Name),
		environmentOwnerKey(current.Record.ProjectID, current.Record.ID),
		deletionTombstoneKey("environment", current.Record.ID),
		deletionTombstoneKey("project", current.Record.ProjectID),
	}
	if renaming {
		secondaryKeys = append(secondaryKeys, environmentNameKey(replacement.ProjectID, replacement.Name))
	}
	secondary, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: secondaryKeys, Revision: current.ReadRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if len(secondary.Values) != len(secondaryKeys) || secondary.Values[0] == nil ||
		string(secondary.Values[0].Value) != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"Environment name index is missing or mismatched",
		)
	}
	if secondary.Values[1] == nil || string(secondary.Values[1].Value) != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"Environment owner index is missing or mismatched",
		)
	}
	if secondary.Values[2] != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindResourceInUse, "Environment deletion is in progress")
	}
	if secondary.Values[3] != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindResourceInUse, "Project deletion is in progress")
	}
	if renaming && secondary.Values[4] != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindSlugConflict, "Environment name is already in use")
	}
	value, err := encodeEnvironment(replacement)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	conditions := []Condition{
		{Key: environmentKey(current.Record.ID), ModRevision: current.Revision},
		{
			Key:         environmentNameKey(current.Record.ProjectID, current.Record.Name),
			ModRevision: secondary.Values[0].ModRevision,
		},
		{
			Key:         environmentOwnerKey(current.Record.ProjectID, current.Record.ID),
			ModRevision: secondary.Values[1].ModRevision,
		},
		{Key: deletionTombstoneKey("environment", current.Record.ID)},
		{Key: deletionTombstoneKey("project", current.Record.ProjectID)},
	}
	mutations := []Mutation{{Type: MutationPut, Key: environmentKey(current.Record.ID), Value: value}}
	if renaming {
		conditions = append(conditions, Condition{Key: environmentNameKey(replacement.ProjectID, replacement.Name)})
		mutations = append(
			mutations,
			Mutation{Type: MutationDelete, Key: environmentNameKey(current.Record.ProjectID, current.Record.Name)},
			Mutation{
				Type:  MutationPut,
				Key:   environmentNameKey(replacement.ProjectID, replacement.Name),
				Value: []byte(current.Record.ID),
			},
		)
	}
	plan, err := newIdempotencyMutationPlan(
		conditions,
		mutations,
		classifyEnvironmentMutationConflict(current, replacement, renaming),
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

func classifyEnvironmentMutationConflict(
	current Versioned[EnvironmentRecord],
	replacement EnvironmentRecord,
	renaming bool,
) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		expected := 5
		if renaming {
			expected++
		}
		if len(values) != expected {
			return errs.New(errs.KindInternal, "Environment mutation compare evidence is incomplete")
		}
		if values[0] == nil {
			return errs.New(errs.KindEnvironmentNotFound, "environment was not found")
		}
		if values[3] != nil {
			return errs.New(errs.KindResourceInUse, "Environment deletion is in progress")
		}
		if values[4] != nil {
			return errs.New(errs.KindResourceInUse, "Project deletion is in progress")
		}
		if renaming && values[5] != nil {
			return errs.Newf(errs.KindSlugConflict, "environment name %q already exists", replacement.Name)
		}
		if values[0].ModRevision != current.Revision {
			return stateConflict("environment", current.Record.ID)
		}
		if values[1] == nil || string(values[1].Value) != current.Record.ID {
			return errs.New(errs.KindInternal, "Environment name index is missing or mismatched")
		}
		if values[2] == nil || string(values[2].Value) != current.Record.ID {
			return errs.New(errs.KindInternal, "Environment owner index is missing or mismatched")
		}
		return stateConflict("environment", current.Record.ID)
	}
}
