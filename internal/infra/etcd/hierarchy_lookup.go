package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	"github.com/AlanD20/groundplane/pkg/errs"
)

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
	return recordquery.Get(
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
	return recordquery.Get(
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
	return recordquery.Get(
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
	return recordquery.Resolve(
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
	return recordquery.Resolve(
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
	return recordquery.Resolve(
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
	return recordquery.Resolve(
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
