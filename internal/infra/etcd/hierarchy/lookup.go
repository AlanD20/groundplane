package hierarchy

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Reader) GetTenant(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[TenantRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[TenantRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindTenant, id); err != nil {
		return etcdstore.Versioned[TenantRecord]{}, err
	}
	return recordquery.Get(
		ctx, repository.store, TenantKey(id), id, errs.KindTenantNotFound, DecodeTenant,
		func(record TenantRecord) string { return record.ID },
	)
}

func (repository *Reader) GetProject(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[ProjectRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[ProjectRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindProject, id); err != nil {
		return etcdstore.Versioned[ProjectRecord]{}, err
	}
	return recordquery.Get(
		ctx, repository.store, ProjectKey(id), id, errs.KindProjectNotFound, DecodeProject,
		func(record ProjectRecord) string { return record.ID },
	)
}

func (repository *Reader) GetEnvironment(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[EnvironmentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[EnvironmentRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, id); err != nil {
		return etcdstore.Versioned[EnvironmentRecord]{}, err
	}
	return recordquery.Get(
		ctx, repository.store, EnvironmentKey(id), id, errs.KindEnvironmentNotFound, DecodeEnvironment,
		func(record EnvironmentRecord) string { return record.ID },
	)
}

func (repository *Reader) ResolveTenant(
	ctx context.Context,
	slug string,
) (etcdstore.Versioned[TenantRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[TenantRecord]{}, err
	}
	if err := recordcodec.ValidateLabel("tenant slug", slug); err != nil {
		return etcdstore.Versioned[TenantRecord]{}, err
	}
	return recordquery.Resolve(
		ctx,
		repository.store,
		TenantSlugKey(slug),
		TenantKey,
		ids.KindTenant,
		errs.KindTenantNotFound,
		DecodeTenant,
		func(record TenantRecord) string { return record.ID },
		func(record TenantRecord) bool { return record.Slug == slug },
	)
}

func (repository *Reader) ResolveTenantProject(
	ctx context.Context,
	tenantID string,
	slug string,
) (etcdstore.Versioned[ProjectRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[ProjectRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindTenant, tenantID); err != nil {
		return etcdstore.Versioned[ProjectRecord]{}, err
	}
	if err := recordcodec.ValidateLabel("project slug", slug); err != nil {
		return etcdstore.Versioned[ProjectRecord]{}, err
	}
	return recordquery.Resolve(
		ctx,
		repository.store,
		ProjectTenantSlugKey(tenantID, slug),
		ProjectKey,
		ids.KindProject,
		errs.KindProjectNotFound,
		DecodeProject,
		func(record ProjectRecord) string { return record.ID },
		func(record ProjectRecord) bool {
			return record.Kind == ProjectKindTenant && record.TenantID == tenantID && record.Slug == slug
		},
	)
}

func (repository *Reader) ResolveBackingProject(
	ctx context.Context,
	slug string,
) (etcdstore.Versioned[ProjectRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[ProjectRecord]{}, err
	}
	if err := recordcodec.ValidateLabel("project slug", slug); err != nil {
		return etcdstore.Versioned[ProjectRecord]{}, err
	}
	return recordquery.Resolve(
		ctx,
		repository.store,
		ProjectPlatformSlugKey(slug),
		ProjectKey,
		ids.KindProject,
		errs.KindProjectNotFound,
		DecodeProject,
		func(record ProjectRecord) string { return record.ID },
		func(record ProjectRecord) bool {
			return record.Kind == ProjectKindBacking && record.TenantID == "" && record.Slug == slug
		},
	)
}

func (repository *Reader) ResolveEnvironment(
	ctx context.Context,
	projectID string,
	name string,
) (etcdstore.Versioned[EnvironmentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[EnvironmentRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindProject, projectID); err != nil {
		return etcdstore.Versioned[EnvironmentRecord]{}, err
	}
	if err := recordcodec.ValidateLabel("environment name", name); err != nil {
		return etcdstore.Versioned[EnvironmentRecord]{}, err
	}
	return recordquery.Resolve(
		ctx,
		repository.store,
		EnvironmentNameKey(projectID, name),
		EnvironmentKey,
		ids.KindEnvironment,
		errs.KindEnvironmentNotFound,
		DecodeEnvironment,
		func(record EnvironmentRecord) string { return record.ID },
		func(record EnvironmentRecord) bool {
			return record.ProjectID == projectID && record.Name == name
		},
	)
}
