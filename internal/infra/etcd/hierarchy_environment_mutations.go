package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

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
		current.Record.NetworkPool != replacement.NetworkPool ||
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
	projectAuthority, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{projectKey(current.Record.ProjectID)}, Revision: current.ReadRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if len(projectAuthority.Values) != 1 || projectAuthority.Values[0] == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	project, err := decodeProject(projectAuthority.Values[0].Value)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if project.ID != current.Record.ProjectID || project.Kind != ProjectKindTenant {
		return IdempotencyTransactionResult{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	renaming := current.Record.Name != replacement.Name
	secondaryKeys := []string{
		environmentNameKey(current.Record.ProjectID, current.Record.Name),
		environmentOwnerKey(current.Record.ProjectID, current.Record.ID),
		deletionTombstoneKey("environment", current.Record.ID),
		deletionTombstoneKey("project", current.Record.ProjectID),
		tenantKey(project.TenantID),
		deletionTombstoneKey("tenant", project.TenantID),
	}
	if renaming {
		secondaryKeys = append(secondaryKeys, environmentNameKey(replacement.ProjectID, replacement.Name))
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
	if secondary.Values[4] == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindTenantNotFound, "tenant was not found")
	}
	tenant, err := decodeTenant(secondary.Values[4].Value)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if tenant.ID != project.TenantID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "environment Tenant authority is mismatched")
	}
	if secondary.Values[5] != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
	}
	if renaming && secondary.Values[6] != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindNameConflict, "Environment name is already in use")
	}
	value, err := encodeEnvironment(replacement)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	conditions := []etcdstore.Condition{
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
		{Key: projectKey(project.ID), ModRevision: projectAuthority.Values[0].ModRevision},
		{Key: tenantKey(tenant.ID), ModRevision: secondary.Values[4].ModRevision},
		{Key: deletionTombstoneKey("tenant", tenant.ID)},
	}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: environmentKey(current.Record.ID), Value: value}}
	if renaming {
		conditions = append(conditions, etcdstore.Condition{Key: environmentNameKey(replacement.ProjectID, replacement.Name)})
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: environmentNameKey(current.Record.ProjectID, current.Record.Name)},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   environmentNameKey(replacement.ProjectID, replacement.Name),
				Value: []byte(current.Record.ID),
			},
		)
	}
	plan, err := newIdempotencyMutationPlan(
		conditions,
		mutations,
		classifyEnvironmentMutationConflict(
			current,
			replacement,
			projectAuthority.Values[0].ModRevision,
			secondary.Values[4].ModRevision,
			renaming,
		),
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
	projectRevision int64,
	tenantRevision int64,
	renaming bool,
) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		expected := 8
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
		if values[7] != nil {
			return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
		}
		if renaming && values[8] != nil {
			return errs.Newf(errs.KindNameConflict, "environment name %q already exists", replacement.Name)
		}
		if values[5] == nil || values[5].ModRevision != projectRevision ||
			values[6] == nil || values[6].ModRevision != tenantRevision {
			return errs.New(errs.KindStateConflict, "environment owning authority changed concurrently")
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
