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

// MutateEnvironmentIdempotent atomically commits the sole direct Environment
// mutation: changing its scoped name label. Identity, ownership, directory,
// provisioning, and create-Task state cannot move through this seam.
func (repository *HierarchyRepository) MutateEnvironmentIdempotent(
	ctx context.Context,
	current etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	replacement hierarchyrecord.EnvironmentRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := hierarchyrecord.ValidateEnvironment(current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := hierarchyrecord.ValidateEnvironment(replacement); err != nil {
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
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Environment mutation marker must be a completed Environment-scoped direct mutation",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	projectAuthority, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{hierarchyrecord.ProjectKey(current.Record.ProjectID)}, Revision: current.ReadRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if len(projectAuthority.Values) != 1 || projectAuthority.Values[0] == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	project, err := hierarchyrecord.DecodeProject(projectAuthority.Values[0].Value)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if project.ID != current.Record.ProjectID || project.Kind != hierarchyrecord.ProjectKindTenant {
		return IdempotencyTransactionResult{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	renaming := current.Record.Name != replacement.Name
	secondaryKeys := []string{
		hierarchyrecord.EnvironmentNameKey(current.Record.ProjectID, current.Record.Name),
		hierarchyrecord.EnvironmentOwnerKey(current.Record.ProjectID, current.Record.ID),
		deletions.TombstoneKey("environment", current.Record.ID),
		deletions.TombstoneKey("project", current.Record.ProjectID),
		hierarchyrecord.TenantKey(project.TenantID),
		deletions.TombstoneKey("tenant", project.TenantID),
	}
	if renaming {
		secondaryKeys = append(secondaryKeys, hierarchyrecord.EnvironmentNameKey(replacement.ProjectID, replacement.Name))
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
	tenant, err := hierarchyrecord.DecodeTenant(secondary.Values[4].Value)
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
	value, err := hierarchyrecord.EncodeEnvironment(replacement)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	conditions := []etcdstore.Condition{
		{Key: hierarchyrecord.EnvironmentKey(current.Record.ID), ModRevision: current.Revision},
		{
			Key:         hierarchyrecord.EnvironmentNameKey(current.Record.ProjectID, current.Record.Name),
			ModRevision: secondary.Values[0].ModRevision,
		},
		{
			Key:         hierarchyrecord.EnvironmentOwnerKey(current.Record.ProjectID, current.Record.ID),
			ModRevision: secondary.Values[1].ModRevision,
		},
		{Key: deletions.TombstoneKey("environment", current.Record.ID)},
		{Key: deletions.TombstoneKey("project", current.Record.ProjectID)},
		{Key: hierarchyrecord.ProjectKey(project.ID), ModRevision: projectAuthority.Values[0].ModRevision},
		{Key: hierarchyrecord.TenantKey(tenant.ID), ModRevision: secondary.Values[4].ModRevision},
		{Key: deletions.TombstoneKey("tenant", tenant.ID)},
	}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentKey(current.Record.ID), Value: value}}
	if renaming {
		conditions = append(conditions, etcdstore.Condition{Key: hierarchyrecord.EnvironmentNameKey(replacement.ProjectID, replacement.Name)})
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: hierarchyrecord.EnvironmentNameKey(current.Record.ProjectID, current.Record.Name)},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   hierarchyrecord.EnvironmentNameKey(replacement.ProjectID, replacement.Name),
				Value: []byte(current.Record.ID),
			},
		)
	}
	plan, err := NewIdempotencyMutationPlan(
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
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func classifyEnvironmentMutationConflict(
	current etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	replacement hierarchyrecord.EnvironmentRecord,
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
			return recordcodec.StateConflict("environment", current.Record.ID)
		}
		if values[1] == nil || string(values[1].Value) != current.Record.ID {
			return errs.New(errs.KindInternal, "Environment name index is missing or mismatched")
		}
		if values[2] == nil || string(values[2].Value) != current.Record.ID {
			return errs.New(errs.KindInternal, "Environment owner index is missing or mismatched")
		}
		return recordcodec.StateConflict("environment", current.Record.ID)
	}
}
