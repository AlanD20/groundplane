package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	DefaultPageLimit = 50
	MaximumPageLimit = 200
)

type ProjectKind string

const (
	ProjectKindTenant  ProjectKind = "tenant"
	ProjectKindBacking ProjectKind = "backing"
)

type Versioned[T any] struct {
	Record       T
	Revision     int64
	ReadRevision int64
}

type PageRequest struct {
	Limit    int
	Cursor   string
	Revision int64
}

type Page[T any] struct {
	Items      []Versioned[T]
	NextCursor string
	Revision   int64
}

type ProjectFilter struct {
	TenantID string
	Kind     ProjectKind
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
	record TenantRecord,
) (Versioned[TenantRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[TenantRecord]{}, err
	}
	if err := validateTenant(record); err != nil {
		return Versioned[TenantRecord]{}, err
	}
	value, err := recordcodec.Encode("tenant", record)
	if err != nil {
		return Versioned[TenantRecord]{}, err
	}
	coordinationValue, err := encodeInitialHierarchyCoordination(HierarchyDeletionTargetTenant, record.ID)
	if err != nil {
		return Versioned[TenantRecord]{}, err
	}
	primary := tenantKey(record.ID)
	slug := tenantSlugKey(record.Slug)
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
		return Versioned[TenantRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[TenantRecord]{}, repository.diagnoseCreate(ctx, primary, slug)
	}
	return Versioned[TenantRecord]{Record: record, Revision: result.Revision, ReadRevision: result.Revision}, nil
}

// CreateTenantIdempotent atomically claims the direct HTTP marker and creates
// the Tenant primary plus globally unique slug index.
func (repository *HierarchyRepository) CreateTenantIdempotent(
	ctx context.Context,
	record TenantRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateTenant(record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Tenant creation marker must be a completed direct mutation",
		)
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	value, err := encodeTenant(record)
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
		[]etcdstore.Condition{{Key: tenantKey(record.ID)}, {Key: tenantSlugKey(record.Slug)}, {Key: coordinationKey}},
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: tenantKey(record.ID), Value: value},
			{Type: etcdstore.MutationPut, Key: tenantSlugKey(record.Slug), Value: []byte(record.ID)},
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
	record ProjectRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateProject(record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if record.Kind != ProjectKindTenant {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "direct Project creation requires a tenant-owned Project",
		)
	}
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Project creation marker must be a completed direct mutation",
		)
	}
	if err := validateIdempotencyMarker(marker); err != nil {
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
	value, err := encodeProject(record)
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
			{Key: projectKey(record.ID)},
			{Key: projectSlugKey(record)},
			{Key: projectOwnerKey(record)},
			{Key: tenantKey(record.TenantID), ModRevision: owner.Revision},
			{Key: tombstoneKey},
			{Key: coordinationKey},
		},
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: projectKey(record.ID), Value: value},
			{Type: etcdstore.MutationPut, Key: projectSlugKey(record), Value: []byte(record.ID)},
			{Type: etcdstore.MutationPut, Key: projectOwnerKey(record), Value: []byte(record.ID)},
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

func classifyProjectCreateConflict(record ProjectRecord, ownerRevision int64) idempotencyPlanClassifier {
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
	current Versioned[TenantRecord],
	replacement TenantRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateTenant(current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateTenant(replacement); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if current.Record.ID != replacement.ID || current.Revision <= 0 || current.ReadRevision <= 0 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Tenant mutation revision is invalid",
		)
	}
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Tenant mutation marker must be a completed direct mutation",
		)
	}
	secondaryKeys := []string{
		tenantSlugKey(current.Record.Slug),
		deletionTombstoneKey("tenant", current.Record.ID),
	}
	renaming := current.Record.Slug != replacement.Slug
	if renaming {
		secondaryKeys = append(secondaryKeys, tenantSlugKey(replacement.Slug))
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
	value, err := encodeTenant(replacement)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	conditions := []etcdstore.Condition{
		{Key: tenantKey(current.Record.ID), ModRevision: current.Revision},
		{Key: tenantSlugKey(current.Record.Slug), ModRevision: secondary.Values[0].ModRevision},
		{Key: deletionTombstoneKey("tenant", current.Record.ID)},
	}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: tenantKey(current.Record.ID), Value: value}}
	if renaming {
		conditions = append(conditions, etcdstore.Condition{Key: tenantSlugKey(replacement.Slug)})
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: tenantSlugKey(current.Record.Slug)},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: tenantSlugKey(replacement.Slug), Value: []byte(current.Record.ID)},
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
	current Versioned[TenantRecord],
	replacement TenantRecord,
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
	record ProjectRecord,
) (Versioned[ProjectRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ProjectRecord]{}, err
	}
	if err := validateProject(record); err != nil {
		return Versioned[ProjectRecord]{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: projectKey(record.ID)},
		{Key: projectSlugKey(record)},
		{Key: projectOwnerKey(record)},
	}
	if record.Kind == ProjectKindTenant {
		owner, err := repository.GetTenant(ctx, record.TenantID)
		if err != nil {
			return Versioned[ProjectRecord]{}, err
		}
		conditions = append(conditions, etcdstore.Condition{Key: tenantKey(record.TenantID), ModRevision: owner.Revision})
	}
	value, err := recordcodec.Encode("project", record)
	if err != nil {
		return Versioned[ProjectRecord]{}, err
	}
	coordinationTarget := HierarchyDeletionTargetProject
	coordinationValue, err := encodeInitialHierarchyCoordination(coordinationTarget, record.ID)
	if err != nil {
		return Versioned[ProjectRecord]{}, err
	}
	coordinationKey := HierarchyCoordinationKey(string(coordinationTarget), record.ID)
	conditions = append(conditions, etcdstore.Condition{Key: coordinationKey})
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: projectKey(record.ID), Value: value},
		{Type: etcdstore.MutationPut, Key: projectSlugKey(record), Value: []byte(record.ID)},
		{Type: etcdstore.MutationPut, Key: projectOwnerKey(record), Value: []byte(record.ID)},
		{Type: etcdstore.MutationPut, Key: coordinationKey, Value: coordinationValue},
	})
	if err != nil {
		return Versioned[ProjectRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[ProjectRecord]{}, repository.diagnoseCreate(ctx, projectKey(record.ID), projectSlugKey(record))
	}
	return Versioned[ProjectRecord]{Record: record, Revision: result.Revision, ReadRevision: result.Revision}, nil
}

func (repository *HierarchyRepository) CreateEnvironment(
	ctx context.Context,
	record EnvironmentRecord,
) (Versioned[EnvironmentRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	if err := validateEnvironment(record); err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	owner, err := repository.GetProject(ctx, record.ProjectID)
	if err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	if owner.Record.Kind == ProjectKindBacking {
		return Versioned[EnvironmentRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backing project environments are created by the backing-service workflow",
		)
	}
	value, err := encodeEnvironment(record)
	if err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	epochValue, err := encodeEnvironmentMutationEpochRecord(EnvironmentMutationEpochRecord{
		EnvironmentID: record.ID,
	})
	if err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	coordinationValue, err := encodeInitialHierarchyCoordination(HierarchyDeletionTargetEnvironment, record.ID)
	if err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	scriptSetValue, err := encodeScriptSetGeneration(ScriptSetGenerationRecord{
		EnvironmentID: record.ID, GenerationID: record.ID,
	})
	if err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	defer clear(scriptSetValue)
	primary := environmentKey(record.ID)
	label := environmentNameKey(record.ProjectID, record.Name)
	ownerIndex := environmentOwnerKey(record.ProjectID, record.ID)
	epochKey := environmentMutationEpochKey(record.ID)
	coordinationKey := HierarchyCoordinationKey(string(HierarchyDeletionTargetEnvironment), record.ID)
	scriptSetKey := scriptSetActiveKey(record.ID)
	result, err := repository.store.Transact(ctx,
		[]etcdstore.Condition{
			{Key: primary},
			{Key: label},
			{Key: ownerIndex},
			{Key: projectKey(record.ProjectID), ModRevision: owner.Revision},
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
		return Versioned[EnvironmentRecord]{}, err
	}
	if !result.Succeeded {
		if len(result.FailureReads) != 7 {
			return Versioned[EnvironmentRecord]{}, errs.New(
				errs.KindInternal,
				"environment creation compare evidence is incomplete",
			)
		}
		if result.FailureReads[4] != nil {
			return Versioned[EnvironmentRecord]{}, errs.New(
				errs.KindInternal,
				"environment creation collided with mutation epoch state",
			)
		}
		if result.FailureReads[5] != nil {
			return Versioned[EnvironmentRecord]{}, errs.New(
				errs.KindInternal,
				"environment creation collided with hierarchy coordination state",
			)
		}
		return Versioned[EnvironmentRecord]{}, repository.diagnoseCreate(ctx, primary, label)
	}
	return Versioned[EnvironmentRecord]{Record: record, Revision: result.Revision, ReadRevision: result.Revision}, nil
}

func (repository *HierarchyRepository) GetTenant(
	ctx context.Context,
	id string,
) (Versioned[TenantRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[TenantRecord]{}, err
	}
	if err := validateID(ids.KindTenant, id); err != nil {
		return Versioned[TenantRecord]{}, err
	}
	return getRecord(
		ctx, repository.store, tenantKey(id), id, errs.KindTenantNotFound, decodeTenant,
		func(record TenantRecord) string { return record.ID },
	)
}

func (repository *HierarchyRepository) GetProject(
	ctx context.Context,
	id string,
) (Versioned[ProjectRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ProjectRecord]{}, err
	}
	if err := validateID(ids.KindProject, id); err != nil {
		return Versioned[ProjectRecord]{}, err
	}
	return getRecord(
		ctx, repository.store, projectKey(id), id, errs.KindProjectNotFound, decodeProject,
		func(record ProjectRecord) string { return record.ID },
	)
}

func (repository *HierarchyRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (Versioned[EnvironmentRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	if err := validateID(ids.KindEnvironment, id); err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	return getRecord(
		ctx, repository.store, environmentKey(id), id, errs.KindEnvironmentNotFound, decodeEnvironment,
		func(record EnvironmentRecord) string { return record.ID },
	)
}

func (repository *HierarchyRepository) ResolveTenant(
	ctx context.Context,
	slug string,
) (Versioned[TenantRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[TenantRecord]{}, err
	}
	if err := validateLabel("tenant slug", slug); err != nil {
		return Versioned[TenantRecord]{}, err
	}
	return resolveRecord(
		ctx,
		repository.store,
		tenantSlugKey(slug),
		tenantKey,
		ids.KindTenant,
		errs.KindTenantNotFound,
		decodeTenant,
		func(record TenantRecord) string { return record.ID },
		func(record TenantRecord) bool { return record.Slug == slug },
	)
}

func (repository *HierarchyRepository) ResolveTenantProject(
	ctx context.Context,
	tenantID string,
	slug string,
) (Versioned[ProjectRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ProjectRecord]{}, err
	}
	if err := validateID(ids.KindTenant, tenantID); err != nil {
		return Versioned[ProjectRecord]{}, err
	}
	if err := validateLabel("project slug", slug); err != nil {
		return Versioned[ProjectRecord]{}, err
	}
	return resolveRecord(
		ctx,
		repository.store,
		projectTenantSlugKey(tenantID, slug),
		projectKey,
		ids.KindProject,
		errs.KindProjectNotFound,
		decodeProject,
		func(record ProjectRecord) string { return record.ID },
		func(record ProjectRecord) bool {
			return record.Kind == ProjectKindTenant && record.TenantID == tenantID && record.Slug == slug
		},
	)
}

func (repository *HierarchyRepository) ResolveBackingProject(
	ctx context.Context,
	slug string,
) (Versioned[ProjectRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ProjectRecord]{}, err
	}
	if err := validateLabel("project slug", slug); err != nil {
		return Versioned[ProjectRecord]{}, err
	}
	return resolveRecord(
		ctx,
		repository.store,
		projectPlatformSlugKey(slug),
		projectKey,
		ids.KindProject,
		errs.KindProjectNotFound,
		decodeProject,
		func(record ProjectRecord) string { return record.ID },
		func(record ProjectRecord) bool {
			return record.Kind == ProjectKindBacking && record.TenantID == "" && record.Slug == slug
		},
	)
}

func (repository *HierarchyRepository) ResolveEnvironment(
	ctx context.Context,
	projectID string,
	name string,
) (Versioned[EnvironmentRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	if err := validateID(ids.KindProject, projectID); err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	if err := validateLabel("environment name", name); err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	return resolveRecord(
		ctx,
		repository.store,
		environmentNameKey(projectID, name),
		environmentKey,
		ids.KindEnvironment,
		errs.KindEnvironmentNotFound,
		decodeEnvironment,
		func(record EnvironmentRecord) string { return record.ID },
		func(record EnvironmentRecord) bool { return record.ProjectID == projectID && record.Name == name },
	)
}

func (repository *HierarchyRepository) RenameTenant(
	ctx context.Context,
	id string,
	expectedRevision int64,
	slug string,
) (Versioned[TenantRecord], error) {
	current, err := repository.GetTenant(ctx, id)
	if err != nil {
		return Versioned[TenantRecord]{}, err
	}
	if current.Revision != expectedRevision {
		return Versioned[TenantRecord]{}, stateConflict("tenant", id)
	}
	replacement := current.Record
	replacement.Slug = slug
	if err := validateTenant(replacement); err != nil {
		return Versioned[TenantRecord]{}, err
	}
	return renameRecord(
		ctx,
		repository.store,
		current,
		replacement,
		tenantKey(id),
		tenantSlugKey(current.Record.Slug),
		tenantSlugKey(slug),
		nil,
		deletionTombstoneKey("tenant", id),
		"tenant",
		id,
		errs.KindTenantNotFound,
		encodeTenant,
	)
}

// RenameTenantProject renames only ordinary tenant-owned Projects. Backing
// Project lifecycle is owned by its facade and is not exposed through this seam.
func (repository *HierarchyRepository) RenameTenantProject(
	ctx context.Context,
	id string,
	expectedRevision int64,
	slug string,
) (Versioned[ProjectRecord], error) {
	current, err := repository.GetProject(ctx, id)
	if err != nil {
		return Versioned[ProjectRecord]{}, err
	}
	if current.Revision != expectedRevision {
		return Versioned[ProjectRecord]{}, stateConflict("project", id)
	}
	if current.Record.Kind != ProjectKindTenant {
		return Versioned[ProjectRecord]{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	replacement := current.Record
	replacement.Slug = slug
	if err := validateProject(replacement); err != nil {
		return Versioned[ProjectRecord]{}, err
	}
	return renameRecord(
		ctx,
		repository.store,
		current,
		replacement,
		projectKey(id),
		projectSlugKey(current.Record),
		projectSlugKey(replacement),
		[]string{projectOwnerKey(current.Record)},
		deletionTombstoneKey("project", id),
		"project",
		id,
		errs.KindProjectNotFound,
		encodeProject,
	)
}

func (repository *HierarchyRepository) ListTenants(
	ctx context.Context,
	request PageRequest,
) (Page[TenantRecord], error) {
	return listPrimaryPage(
		ctx,
		repository.store,
		"tenants",
		"global",
		"-",
		tenantPrefix,
		ids.KindTenant,
		request,
		decodeTenant,
		func(record TenantRecord) string { return record.ID },
		func(TenantRecord) bool { return true },
	)
}

func (repository *HierarchyRepository) ListTenantProjects(
	ctx context.Context,
	tenantID string,
	request PageRequest,
) (Page[ProjectRecord], error) {
	if err := validateID(ids.KindTenant, tenantID); err != nil {
		return Page[ProjectRecord]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"projects",
		"tenant",
		tenantID,
		projectTenantOwnerPrefix(tenantID),
		projectKey,
		ids.KindProject,
		request,
		decodeProject,
		func(record ProjectRecord) string { return record.ID },
		func(record ProjectRecord) bool {
			return record.Kind == ProjectKindTenant && record.TenantID == tenantID
		},
	)
}

func (repository *HierarchyRepository) ListProjects(
	ctx context.Context,
	filter ProjectFilter,
	request PageRequest,
) (Page[ProjectRecord], error) {
	if filter.Kind != "" && filter.Kind != ProjectKindTenant && filter.Kind != ProjectKindBacking {
		return Page[ProjectRecord]{}, errs.New(errs.KindValidationFailed, "project kind must be tenant or backing")
	}
	if filter.TenantID != "" {
		if err := validateID(ids.KindTenant, filter.TenantID); err != nil {
			return Page[ProjectRecord]{}, err
		}
		if filter.Kind == ProjectKindBacking {
			return Page[ProjectRecord]{}, errs.New(
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
		projectPrefix,
		ids.KindProject,
		request,
		decodeProject,
		func(record ProjectRecord) string { return record.ID },
		func(record ProjectRecord) bool {
			return (filter.Kind == "" || record.Kind == filter.Kind) &&
				(filter.TenantID == "" || record.TenantID == filter.TenantID)
		},
	)
}

func (repository *HierarchyRepository) ListBackingProjects(
	ctx context.Context,
	request PageRequest,
) (Page[ProjectRecord], error) {
	return listIndexPage(
		ctx,
		repository.store,
		"projects",
		"platform",
		"-",
		projectPlatformOwnerPrefix,
		projectKey,
		ids.KindProject,
		request,
		decodeProject,
		func(record ProjectRecord) string { return record.ID },
		func(record ProjectRecord) bool {
			return record.Kind == ProjectKindBacking && record.TenantID == ""
		},
	)
}

func (repository *HierarchyRepository) ListEnvironments(
	ctx context.Context,
	projectID string,
	request PageRequest,
) (Page[EnvironmentRecord], error) {
	if err := validateID(ids.KindProject, projectID); err != nil {
		return Page[EnvironmentRecord]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"environments",
		"project",
		projectID,
		environmentOwnerPrefix(projectID),
		environmentKey,
		ids.KindEnvironment,
		request,
		decodeEnvironment,
		func(record EnvironmentRecord) string { return record.ID },
		func(record EnvironmentRecord) bool { return record.ProjectID == projectID },
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
