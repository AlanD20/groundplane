package hierarchy

import (
	"context"

	"github.com/AlanD20/groundplane/internal/core"
	etcdinfra "github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EtcdRepository adapts the durable hierarchy repository to the Controller
// port without exposing persistence DTOs through the use-case interface.
type EtcdRepository struct {
	repository *etcdinfra.HierarchyRepository
}

func (repository *EtcdRepository) ready() bool {
	return repository != nil && repository.repository != nil
}

func NewEtcdRepository(repository *etcdinfra.HierarchyRepository) (*EtcdRepository, error) {
	if repository == nil {
		return nil, errs.New(errs.KindInternal, "etcd hierarchy repository is required")
	}
	return &EtcdRepository{repository: repository}, nil
}

func (repository *EtcdRepository) CreateTenant(
	ctx context.Context,
	record core.Tenant,
) (Versioned[core.Tenant], error) {
	stored, err := repository.repository.CreateTenant(ctx, tenantToEtcd(record))
	return tenantFromEtcd(stored), err
}

func (repository *EtcdRepository) GetTenant(
	ctx context.Context,
	id string,
) (Versioned[core.Tenant], error) {
	stored, err := repository.repository.GetTenant(ctx, id)
	return tenantFromEtcd(stored), err
}

func (repository *EtcdRepository) ResolveTenant(
	ctx context.Context,
	slug string,
) (Versioned[core.Tenant], error) {
	stored, err := repository.repository.ResolveTenant(ctx, slug)
	return tenantFromEtcd(stored), err
}

func (repository *EtcdRepository) ListTenants(
	ctx context.Context,
	request PageRequest,
) (Page[core.Tenant], error) {
	page, err := repository.repository.ListTenants(ctx, pageRequestToEtcd(request))
	return tenantPageFromEtcd(page), err
}

func (repository *EtcdRepository) RenameTenant(
	ctx context.Context,
	id string,
	expectedRevision int64,
	slug string,
) (Versioned[core.Tenant], error) {
	stored, err := repository.repository.RenameTenant(ctx, id, expectedRevision, slug)
	return tenantFromEtcd(stored), err
}

func (repository *EtcdRepository) CreateProject(
	ctx context.Context,
	record core.Project,
) (Versioned[core.Project], error) {
	if record.Kind != core.ProjectKindTenant {
		return Versioned[core.Project]{}, errs.New(
			errs.KindValidationFailed,
			"normal project kind must be tenant",
		)
	}
	stored, err := repository.repository.CreateProject(ctx, projectToEtcd(record))
	return projectFromEtcd(stored), err
}

func (repository *EtcdRepository) GetProject(
	ctx context.Context,
	id string,
) (Versioned[core.Project], error) {
	stored, err := repository.repository.GetProject(ctx, id)
	if err != nil {
		return Versioned[core.Project]{}, err
	}
	if stored.Record.Kind != etcdinfra.ProjectKindTenant {
		return Versioned[core.Project]{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	return projectFromEtcd(stored), nil
}

func (repository *EtcdRepository) ResolveTenantProject(
	ctx context.Context,
	tenantID string,
	slug string,
) (Versioned[core.Project], error) {
	stored, err := repository.repository.ResolveTenantProject(ctx, tenantID, slug)
	return projectFromEtcd(stored), err
}

func (repository *EtcdRepository) ListTenantProjects(
	ctx context.Context,
	tenantID string,
	request PageRequest,
) (Page[core.Project], error) {
	page, err := repository.repository.ListTenantProjects(ctx, tenantID, pageRequestToEtcd(request))
	return projectPageFromEtcd(page), err
}

func (repository *EtcdRepository) ListProjects(
	ctx context.Context,
	filter ProjectFilter,
	request PageRequest,
) (Page[core.Project], error) {
	page, err := repository.repository.ListProjects(ctx, etcdinfra.ProjectFilter{
		TenantID: filter.TenantID, Kind: etcdinfra.ProjectKind(filter.Kind),
	}, pageRequestToEtcd(request))
	return projectPageFromEtcd(page), err
}

func (repository *EtcdRepository) RenameProject(
	ctx context.Context,
	id string,
	expectedRevision int64,
	slug string,
) (Versioned[core.Project], error) {
	stored, err := repository.repository.RenameTenantProject(ctx, id, expectedRevision, slug)
	return projectFromEtcd(stored), err
}

func tenantToEtcd(record core.Tenant) etcdinfra.TenantRecord {
	return etcdinfra.TenantRecord{
		ID: record.ID, Slug: record.Slug, Name: record.Name, Description: record.Description,
	}
}

func tenantFromEtcd(stored etcdinfra.Versioned[etcdinfra.TenantRecord]) Versioned[core.Tenant] {
	return Versioned[core.Tenant]{
		Record: core.Tenant{
			ID: stored.Record.ID, Slug: stored.Record.Slug, Name: stored.Record.Name,
			Description: stored.Record.Description,
		},
		Revision: stored.Revision, ReadRevision: stored.ReadRevision,
	}
}

func projectToEtcd(record core.Project) etcdinfra.ProjectRecord {
	return etcdinfra.ProjectRecord{
		ID:          record.ID,
		TenantID:    record.TenantID,
		Slug:        record.Slug,
		Name:        record.Name,
		Description: record.Description,
		Kind:        etcdinfra.ProjectKind(record.Kind),
	}
}

func projectFromEtcd(stored etcdinfra.Versioned[etcdinfra.ProjectRecord]) Versioned[core.Project] {
	return Versioned[core.Project]{
		Record: core.Project{
			ID:          stored.Record.ID,
			TenantID:    stored.Record.TenantID,
			Slug:        stored.Record.Slug,
			Name:        stored.Record.Name,
			Description: stored.Record.Description,
			Kind:        core.ProjectKind(stored.Record.Kind),
		},
		Revision: stored.Revision, ReadRevision: stored.ReadRevision,
	}
}

func pageRequestToEtcd(request PageRequest) etcdinfra.PageRequest {
	return etcdinfra.PageRequest{Limit: request.Limit, Cursor: request.Cursor}
}

func tenantPageFromEtcd(page etcdinfra.Page[etcdinfra.TenantRecord]) Page[core.Tenant] {
	items := make([]Versioned[core.Tenant], len(page.Items))
	for index, item := range page.Items {
		items[index] = tenantFromEtcd(item)
	}
	return Page[core.Tenant]{Items: items, NextCursor: page.NextCursor, Revision: page.Revision}
}

func projectPageFromEtcd(page etcdinfra.Page[etcdinfra.ProjectRecord]) Page[core.Project] {
	items := make([]Versioned[core.Project], len(page.Items))
	for index, item := range page.Items {
		items[index] = projectFromEtcd(item)
	}
	return Page[core.Project]{Items: items, NextCursor: page.NextCursor, Revision: page.Revision}
}

var _ Repository = (*EtcdRepository)(nil)
