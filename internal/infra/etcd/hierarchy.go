package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ProjectFilter struct {
	TenantID string
	Kind     hierarchyrecord.ProjectKind
}

type hierarchyStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	MeasureTransaction(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionBudget, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// HierarchyRepository owns durable Tenant, Project, and Environment persistence.
type HierarchyRepository struct {
	store hierarchyStore
}

// EnvironmentBlueprintRepository owns final Environment Blueprint publication.
// Its dedicated transaction executor is mandatory and is not available to
// ordinary hierarchy persistence paths.
type EnvironmentBlueprintRepository struct {
	*HierarchyRepository
	transactions environmentBlueprintTransactionStore
}

func NewHierarchyRepository(store etcdstore.Store) (*HierarchyRepository, error) {
	return newHierarchyRepository(store)
}

// NewEnvironmentBlueprintRepository constructs the only repository authorized
// to publish a final Environment Blueprint transaction.
func NewEnvironmentBlueprintRepository(store EnvironmentBlueprintStore) (*EnvironmentBlueprintRepository, error) {
	return newEnvironmentBlueprintRepository(store, store)
}

func newHierarchyRepository(store hierarchyStore) (*HierarchyRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "hierarchy store is required")
	}
	return &HierarchyRepository{store: store}, nil
}

func newEnvironmentBlueprintRepository(
	store hierarchyStore,
	transactions environmentBlueprintTransactionStore,
) (*EnvironmentBlueprintRepository, error) {
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		return nil, err
	}
	if transactions == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint transaction executor is required")
	}
	return &EnvironmentBlueprintRepository{
		HierarchyRepository: hierarchy,
		transactions:        transactions,
	}, nil
}

func (repository *HierarchyRepository) CreateTenant(
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
	coordinationValue, err := encodeInitialHierarchyCoordination(HierarchyDeletionTargetTenant, record.ID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.TenantRecord]{}, err
	}
	primary := hierarchyrecord.TenantKey(record.ID)
	slug := hierarchyrecord.TenantSlugKey(record.Slug)
	coordinationKey := HierarchyCoordinationKey(string(HierarchyDeletionTargetTenant), record.ID)
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
	return etcdstore.Versioned[hierarchyrecord.TenantRecord]{Record: record, Revision: result.Revision, ReadRevision: result.Revision}, nil
}

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
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted {
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
	coordinationValue, err := encodeInitialHierarchyCoordination(HierarchyDeletionTargetTenant, record.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(coordinationValue)
	coordinationKey := HierarchyCoordinationKey(string(HierarchyDeletionTargetTenant), record.ID)
	plan, err := newIdempotencyMutationPlan(
		[]etcdstore.Condition{{Key: hierarchyrecord.TenantKey(record.ID)}, {Key: hierarchyrecord.TenantSlugKey(record.Slug)}, {Key: coordinationKey}},
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
	idempotency, err := newIdempotencyRepository(repository.store)
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
	tombstoneKey := deletionTombstoneKey("tenant", record.TenantID)
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
	coordinationValue, err := encodeInitialHierarchyCoordination(HierarchyDeletionTargetProject, record.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(coordinationValue)
	coordinationKey := HierarchyCoordinationKey(string(HierarchyDeletionTargetProject), record.ID)
	plan, err := newIdempotencyMutationPlan(
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
	idempotency, err := newIdempotencyRepository(repository.store)
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
			return stateConflict("tenant", record.TenantID)
		}
		return errs.New(errs.KindInternal, "Project creation compare failure was not classified")
	}
}

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
		deletionTombstoneKey("tenant", current.Record.ID),
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
		{Key: deletionTombstoneKey("tenant", current.Record.ID)},
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
	plan, err := newIdempotencyMutationPlan(
		conditions,
		mutations,
		classifyTenantMutationConflict(current, replacement, renaming),
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
			return stateConflict("tenant", current.Record.ID)
		}
		if values[1] == nil || string(values[1].Value) != current.Record.ID {
			return errs.New(errs.KindInternal, "Tenant slug index is missing or mismatched")
		}
		return stateConflict("tenant", current.Record.ID)
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
	coordinationTarget := HierarchyDeletionTargetProject
	coordinationValue, err := encodeInitialHierarchyCoordination(coordinationTarget, record.ID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	coordinationKey := HierarchyCoordinationKey(string(coordinationTarget), record.ID)
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

func (repository *HierarchyRepository) CreateEnvironment(
	ctx context.Context,
	record hierarchyrecord.EnvironmentRecord,
) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	if err := hierarchyrecord.ValidateEnvironment(record); err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	owner, err := repository.GetProject(ctx, record.ProjectID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	if owner.Record.Kind == hierarchyrecord.ProjectKindBacking {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backing project environments are created by the backing-service workflow",
		)
	}
	value, err := hierarchyrecord.EncodeEnvironment(record)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	epochValue, err := backupruntime.EncodeEnvironmentMutationEpochRecord(backupruntime.EnvironmentMutationEpochRecord{
		EnvironmentID: record.ID,
	})
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	coordinationValue, err := encodeInitialHierarchyCoordination(HierarchyDeletionTargetEnvironment, record.ID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	scriptSetValue, err := scriptrecord.EncodeScriptSetGeneration(scriptrecord.SetGenerationRecord{
		EnvironmentID: record.ID, GenerationID: record.ID,
	})
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	defer clear(scriptSetValue)
	primary := hierarchyrecord.EnvironmentKey(record.ID)
	label := hierarchyrecord.EnvironmentNameKey(record.ProjectID, record.Name)
	ownerIndex := hierarchyrecord.EnvironmentOwnerKey(record.ProjectID, record.ID)
	epochKey := hierarchyrecord.EnvironmentMutationEpochKey(record.ID)
	coordinationKey := HierarchyCoordinationKey(string(HierarchyDeletionTargetEnvironment), record.ID)
	scriptSetKey := scriptrecord.ScriptSetActiveKey(record.ID)
	result, err := repository.store.Transact(ctx,
		[]etcdstore.Condition{
			{Key: primary},
			{Key: label},
			{Key: ownerIndex},
			{Key: hierarchyrecord.ProjectKey(record.ProjectID), ModRevision: owner.Revision},
			{Key: epochKey},
			{Key: coordinationKey},
			{Key: scriptSetKey},
		},
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: primary, Value: value},
			{Type: etcdstore.MutationPut, Key: label, Value: []byte(record.ID)},
			{Type: etcdstore.MutationPut, Key: ownerIndex, Value: []byte(record.ID)},
			{Type: etcdstore.MutationPut, Key: epochKey, Value: epochValue},
			{Type: etcdstore.MutationPut, Key: coordinationKey, Value: coordinationValue},
			{Type: etcdstore.MutationPut, Key: scriptSetKey, Value: scriptSetValue},
		},
	)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	if !result.Succeeded {
		if len(result.FailureReads) != 7 {
			return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, errs.New(
				errs.KindInternal,
				"environment creation compare evidence is incomplete",
			)
		}
		if result.FailureReads[4] != nil {
			return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, errs.New(
				errs.KindInternal,
				"environment creation collided with mutation epoch state",
			)
		}
		if result.FailureReads[5] != nil {
			return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, errs.New(
				errs.KindInternal,
				"environment creation collided with hierarchy coordination state",
			)
		}
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, repository.diagnoseCreate(ctx, primary, label)
	}
	return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{Record: record, Revision: result.Revision, ReadRevision: result.Revision}, nil
}

func (repository *HierarchyRepository) GetTenant(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[hierarchyrecord.TenantRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindTenant, id); err != nil {
		return etcdstore.Versioned[hierarchyrecord.TenantRecord]{}, err
	}
	return getRecord(
		ctx, repository.store, hierarchyrecord.TenantKey(id), id, errs.KindTenantNotFound, hierarchyrecord.DecodeTenant,
		func(record hierarchyrecord.TenantRecord) string { return record.ID },
	)
}

func (repository *HierarchyRepository) GetProject(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindProject, id); err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	return getRecord(
		ctx, repository.store, hierarchyrecord.ProjectKey(id), id, errs.KindProjectNotFound, hierarchyrecord.DecodeProject,
		func(record hierarchyrecord.ProjectRecord) string { return record.ID },
	)
}

func (repository *HierarchyRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, id); err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	return getRecord(
		ctx, repository.store, hierarchyrecord.EnvironmentKey(id), id, errs.KindEnvironmentNotFound, hierarchyrecord.DecodeEnvironment,
		func(record hierarchyrecord.EnvironmentRecord) string { return record.ID },
	)
}

func (repository *HierarchyRepository) ResolveTenant(
	ctx context.Context,
	slug string,
) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[hierarchyrecord.TenantRecord]{}, err
	}
	if err := recordcodec.ValidateLabel("tenant slug", slug); err != nil {
		return etcdstore.Versioned[hierarchyrecord.TenantRecord]{}, err
	}
	return resolveRecord(
		ctx,
		repository.store,
		hierarchyrecord.TenantSlugKey(slug),
		hierarchyrecord.TenantKey,
		ids.KindTenant,
		errs.KindTenantNotFound,
		hierarchyrecord.DecodeTenant,
		func(record hierarchyrecord.TenantRecord) string { return record.ID },
		func(record hierarchyrecord.TenantRecord) bool { return record.Slug == slug },
	)
}

func (repository *HierarchyRepository) ResolveTenantProject(
	ctx context.Context,
	tenantID string,
	slug string,
) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindTenant, tenantID); err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	if err := recordcodec.ValidateLabel("project slug", slug); err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	return resolveRecord(
		ctx,
		repository.store,
		hierarchyrecord.ProjectTenantSlugKey(tenantID, slug),
		hierarchyrecord.ProjectKey,
		ids.KindProject,
		errs.KindProjectNotFound,
		hierarchyrecord.DecodeProject,
		func(record hierarchyrecord.ProjectRecord) string { return record.ID },
		func(record hierarchyrecord.ProjectRecord) bool {
			return record.Kind == hierarchyrecord.ProjectKindTenant && record.TenantID == tenantID && record.Slug == slug
		},
	)
}

func (repository *HierarchyRepository) ResolveBackingProject(
	ctx context.Context,
	slug string,
) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	if err := recordcodec.ValidateLabel("project slug", slug); err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	return resolveRecord(
		ctx,
		repository.store,
		hierarchyrecord.ProjectPlatformSlugKey(slug),
		hierarchyrecord.ProjectKey,
		ids.KindProject,
		errs.KindProjectNotFound,
		hierarchyrecord.DecodeProject,
		func(record hierarchyrecord.ProjectRecord) string { return record.ID },
		func(record hierarchyrecord.ProjectRecord) bool {
			return record.Kind == hierarchyrecord.ProjectKindBacking && record.TenantID == "" && record.Slug == slug
		},
	)
}

func (repository *HierarchyRepository) ResolveEnvironment(
	ctx context.Context,
	projectID string,
	name string,
) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindProject, projectID); err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	if err := recordcodec.ValidateLabel("environment name", name); err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	return resolveRecord(
		ctx,
		repository.store,
		hierarchyrecord.EnvironmentNameKey(projectID, name),
		hierarchyrecord.EnvironmentKey,
		ids.KindEnvironment,
		errs.KindEnvironmentNotFound,
		hierarchyrecord.DecodeEnvironment,
		func(record hierarchyrecord.EnvironmentRecord) string { return record.ID },
		func(record hierarchyrecord.EnvironmentRecord) bool {
			return record.ProjectID == projectID && record.Name == name
		},
	)
}

func (repository *HierarchyRepository) RenameTenant(
	ctx context.Context,
	id string,
	expectedRevision int64,
	slug string,
) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error) {
	current, err := repository.GetTenant(ctx, id)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.TenantRecord]{}, err
	}
	if current.Revision != expectedRevision {
		return etcdstore.Versioned[hierarchyrecord.TenantRecord]{}, stateConflict("tenant", id)
	}
	replacement := current.Record
	replacement.Slug = slug
	if err := hierarchyrecord.ValidateTenant(replacement); err != nil {
		return etcdstore.Versioned[hierarchyrecord.TenantRecord]{}, err
	}
	return renameRecord(
		ctx,
		repository.store,
		current,
		replacement,
		hierarchyrecord.TenantKey(id),
		hierarchyrecord.TenantSlugKey(current.Record.Slug),
		hierarchyrecord.TenantSlugKey(slug),
		nil,
		deletionTombstoneKey("tenant", id),
		"tenant",
		id,
		errs.KindTenantNotFound,
		hierarchyrecord.EncodeTenant,
	)
}

// RenameTenantProject renames only ordinary tenant-owned Projects. Backing
// Project lifecycle is owned by its facade and is not exposed through this seam.
func (repository *HierarchyRepository) RenameTenantProject(
	ctx context.Context,
	id string,
	expectedRevision int64,
	slug string,
) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error) {
	current, err := repository.GetProject(ctx, id)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	if current.Revision != expectedRevision {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, stateConflict("project", id)
	}
	if current.Record.Kind != hierarchyrecord.ProjectKindTenant {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	replacement := current.Record
	replacement.Slug = slug
	if err := hierarchyrecord.ValidateProject(replacement); err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	return renameRecord(
		ctx,
		repository.store,
		current,
		replacement,
		hierarchyrecord.ProjectKey(id),
		hierarchyrecord.ProjectSlugKey(current.Record),
		hierarchyrecord.ProjectSlugKey(replacement),
		[]string{hierarchyrecord.ProjectOwnerKey(current.Record)},
		deletionTombstoneKey("project", id),
		"project",
		id,
		errs.KindProjectNotFound,
		hierarchyrecord.EncodeProject,
	)
}

func (repository *HierarchyRepository) ListTenants(
	ctx context.Context,
	request etcdstore.PageRequest,
) (etcdstore.Page[hierarchyrecord.TenantRecord], error) {
	return listPrimaryPage(
		ctx,
		repository.store,
		"tenants",
		"global",
		"-",
		hierarchyrecord.TenantPrefix,
		ids.KindTenant,
		request,
		hierarchyrecord.DecodeTenant,
		func(record hierarchyrecord.TenantRecord) string { return record.ID },
		func(hierarchyrecord.TenantRecord) bool { return true },
	)
}

func (repository *HierarchyRepository) ListTenantProjects(
	ctx context.Context,
	tenantID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[hierarchyrecord.ProjectRecord], error) {
	if err := recordcodec.ValidateID(ids.KindTenant, tenantID); err != nil {
		return etcdstore.Page[hierarchyrecord.ProjectRecord]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"projects",
		"tenant",
		tenantID,
		hierarchyrecord.ProjectTenantOwnerPrefix(tenantID),
		hierarchyrecord.ProjectKey,
		ids.KindProject,
		request,
		hierarchyrecord.DecodeProject,
		func(record hierarchyrecord.ProjectRecord) string { return record.ID },
		func(record hierarchyrecord.ProjectRecord) bool {
			return record.Kind == hierarchyrecord.ProjectKindTenant && record.TenantID == tenantID
		},
	)
}

func (repository *HierarchyRepository) ListProjects(
	ctx context.Context,
	filter ProjectFilter,
	request etcdstore.PageRequest,
) (etcdstore.Page[hierarchyrecord.ProjectRecord], error) {
	if filter.Kind != "" && filter.Kind != hierarchyrecord.ProjectKindTenant && filter.Kind != hierarchyrecord.ProjectKindBacking {
		return etcdstore.Page[hierarchyrecord.ProjectRecord]{}, errs.New(errs.KindValidationFailed, "project kind must be tenant or backing")
	}
	if filter.TenantID != "" {
		if err := recordcodec.ValidateID(ids.KindTenant, filter.TenantID); err != nil {
			return etcdstore.Page[hierarchyrecord.ProjectRecord]{}, err
		}
		if filter.Kind == hierarchyrecord.ProjectKindBacking {
			return etcdstore.Page[hierarchyrecord.ProjectRecord]{}, errs.New(
				errs.KindValidationFailed,
				"backing projects cannot have a tenant filter",
			)
		}
	}
	ownerID := filter.TenantID
	if ownerID == "" {
		ownerID = "-"
	}
	return listFilteredPrimaryPage(
		ctx,
		repository.store,
		"projects",
		"filter:"+string(filter.Kind),
		ownerID,
		hierarchyrecord.ProjectPrefix,
		ids.KindProject,
		request,
		hierarchyrecord.DecodeProject,
		func(record hierarchyrecord.ProjectRecord) string { return record.ID },
		func(record hierarchyrecord.ProjectRecord) bool {
			return (filter.Kind == "" || record.Kind == filter.Kind) &&
				(filter.TenantID == "" || record.TenantID == filter.TenantID)
		},
	)
}

func (repository *HierarchyRepository) ListBackingProjects(
	ctx context.Context,
	request etcdstore.PageRequest,
) (etcdstore.Page[hierarchyrecord.ProjectRecord], error) {
	return listIndexPage(
		ctx,
		repository.store,
		"projects",
		"platform",
		"-",
		hierarchyrecord.ProjectPlatformOwnerPrefix,
		hierarchyrecord.ProjectKey,
		ids.KindProject,
		request,
		hierarchyrecord.DecodeProject,
		func(record hierarchyrecord.ProjectRecord) string { return record.ID },
		func(record hierarchyrecord.ProjectRecord) bool {
			return record.Kind == hierarchyrecord.ProjectKindBacking && record.TenantID == ""
		},
	)
}

func (repository *HierarchyRepository) ListEnvironments(
	ctx context.Context,
	projectID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[hierarchyrecord.EnvironmentRecord], error) {
	if err := recordcodec.ValidateID(ids.KindProject, projectID); err != nil {
		return etcdstore.Page[hierarchyrecord.EnvironmentRecord]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"environments",
		"project",
		projectID,
		hierarchyrecord.EnvironmentOwnerPrefix(projectID),
		hierarchyrecord.EnvironmentKey,
		ids.KindEnvironment,
		request,
		hierarchyrecord.DecodeEnvironment,
		func(record hierarchyrecord.EnvironmentRecord) string { return record.ID },
		func(record hierarchyrecord.EnvironmentRecord) bool { return record.ProjectID == projectID },
	)
}

func (repository *HierarchyRepository) diagnoseCreate(ctx context.Context, primary string, slug string) error {
	primaryResult, err := repository.store.Get(ctx, primary)
	if err != nil {
		return err
	}
	slugResult, err := repository.store.Get(ctx, slug)
	if err != nil {
		return err
	}
	if slugResult.Entry != nil {
		return errs.New(errs.KindSlugConflict, "slug is already in use")
	}
	if primaryResult.Entry != nil {
		return errs.New(errs.KindStateConflict, "stable id is already in use")
	}
	return errs.New(errs.KindStateConflict, "hierarchy changed during create")
}
