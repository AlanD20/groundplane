package etcd

import (
	"context"
	"time"

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

// TenantRecord is the versioned persistence DTO. It is intentionally not a
// core aggregate or a public API DTO.
type TenantRecord struct {
	ID   string `json:"id"`
	Slug string `json:"slug"`
	Name string `json:"name"`
}

// ProjectRecord keeps ownership in the flat primary. Backing projects have no
// tenant owner and use the platform owner index.
type ProjectRecord struct {
	ID       string      `json:"id"`
	TenantID string      `json:"tenant_id,omitempty"`
	Slug     string      `json:"slug"`
	Name     string      `json:"name"`
	Kind     ProjectKind `json:"kind"`
}

// EnvironmentRecord persists only the environment record itself. Desired
// resources are separate flat records and never embedded here.
type EnvironmentRecord struct {
	ID        string    `json:"id"`
	ProjectID string    `json:"project_id"`
	Slug      string    `json:"slug"`
	Name      string    `json:"name"`
	VolumeDir string    `json:"volume_dir"`
	CreatedAt time.Time `json:"created_at"`
}

type Versioned[T any] struct {
	Record       T
	Revision     int64
	ReadRevision int64
}

type PageRequest struct {
	Limit  int
	Cursor string
}

type Page[T any] struct {
	Items      []Versioned[T]
	NextCursor string
	Revision   int64
}

type hierarchyStore interface {
	Get(context.Context, string) (*GetResult, error)
	GetMany(context.Context, GetManyRequest) (*GetManyResult, error)
	Range(context.Context, RangeRequest) (*RangeResult, error)
	Transact(context.Context, []Condition, []Mutation) (TransactionResult, error)
}

// HierarchyRepository owns the durable key, envelope, CAS, index, and cursor
// mechanics for Tenant, Project, and Environment records.
type HierarchyRepository struct {
	store hierarchyStore
}

func NewHierarchyRepository(store Store) (*HierarchyRepository, error) {
	return newHierarchyRepository(store)
}

func newHierarchyRepository(store hierarchyStore) (*HierarchyRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "hierarchy store is required")
	}
	return &HierarchyRepository{store: store}, nil
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
	value, err := encodeEnvelope("tenant", record)
	if err != nil {
		return Versioned[TenantRecord]{}, err
	}
	primary := tenantKey(record.ID)
	slug := tenantSlugKey(record.Slug)
	result, err := repository.store.Transact(ctx,
		[]Condition{{Key: primary}, {Key: slug}},
		[]Mutation{
			{Type: MutationPut, Key: primary, Value: value},
			{Type: MutationPut, Key: slug, Value: []byte(record.ID)},
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
	conditions := []Condition{
		{Key: projectKey(record.ID)},
		{Key: projectSlugKey(record)},
		{Key: projectOwnerKey(record)},
	}
	if record.Kind == ProjectKindTenant {
		owner, err := repository.GetTenant(ctx, record.TenantID)
		if err != nil {
			return Versioned[ProjectRecord]{}, err
		}
		conditions = append(conditions, Condition{Key: tenantKey(record.TenantID), ModRevision: owner.Revision})
	}
	value, err := encodeEnvelope("project", record)
	if err != nil {
		return Versioned[ProjectRecord]{}, err
	}
	result, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationPut, Key: projectKey(record.ID), Value: value},
		{Type: MutationPut, Key: projectSlugKey(record), Value: []byte(record.ID)},
		{Type: MutationPut, Key: projectOwnerKey(record), Value: []byte(record.ID)},
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
	primary := environmentKey(record.ID)
	slug := environmentSlugKey(record.ProjectID, record.Slug)
	ownerIndex := environmentOwnerKey(record.ProjectID, record.ID)
	result, err := repository.store.Transact(ctx,
		[]Condition{
			{Key: primary},
			{Key: slug},
			{Key: ownerIndex},
			{Key: projectKey(record.ProjectID), ModRevision: owner.Revision},
		},
		[]Mutation{
			{Type: MutationPut, Key: primary, Value: value},
			{Type: MutationPut, Key: slug, Value: []byte(record.ID)},
			{Type: MutationPut, Key: ownerIndex, Value: []byte(record.ID)},
		},
	)
	if err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[EnvironmentRecord]{}, repository.diagnoseCreate(ctx, primary, slug)
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
	slug string,
) (Versioned[EnvironmentRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	if err := validateID(ids.KindProject, projectID); err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	if err := validateLabel("environment slug", slug); err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	return resolveRecord(
		ctx,
		repository.store,
		environmentSlugKey(projectID, slug),
		environmentKey,
		ids.KindEnvironment,
		errs.KindEnvironmentNotFound,
		decodeEnvironment,
		func(record EnvironmentRecord) string { return record.ID },
		func(record EnvironmentRecord) bool { return record.ProjectID == projectID && record.Slug == slug },
	)
}

func (repository *HierarchyRepository) RenameTenant(
	ctx context.Context,
	id string,
	expectedRevision int64,
	slug string,
	name string,
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
	replacement.Name = name
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
		"tenant",
		id,
		encodeTenant,
	)
}

func (repository *HierarchyRepository) RenameProject(
	ctx context.Context,
	id string,
	expectedRevision int64,
	slug string,
	name string,
) (Versioned[ProjectRecord], error) {
	current, err := repository.GetProject(ctx, id)
	if err != nil {
		return Versioned[ProjectRecord]{}, err
	}
	if current.Revision != expectedRevision {
		return Versioned[ProjectRecord]{}, stateConflict("project", id)
	}
	replacement := current.Record
	replacement.Slug = slug
	replacement.Name = name
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
		"project",
		id,
		encodeProject,
	)
}

func (repository *HierarchyRepository) RenameEnvironment(
	ctx context.Context,
	id string,
	expectedRevision int64,
	slug string,
	name string,
) (Versioned[EnvironmentRecord], error) {
	current, err := repository.GetEnvironment(ctx, id)
	if err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	if current.Revision != expectedRevision {
		return Versioned[EnvironmentRecord]{}, stateConflict("environment", id)
	}
	replacement := current.Record
	replacement.Slug = slug
	replacement.Name = name
	if err := validateEnvironment(replacement); err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	return renameRecord(
		ctx,
		repository.store,
		current,
		replacement,
		environmentKey(id),
		environmentSlugKey(current.Record.ProjectID, current.Record.Slug),
		environmentSlugKey(replacement.ProjectID, replacement.Slug),
		[]string{environmentOwnerKey(current.Record.ProjectID, id)},
		"environment",
		id,
		encodeEnvironment,
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
