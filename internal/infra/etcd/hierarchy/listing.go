package hierarchy

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Reader) ListTenants(
	ctx context.Context,
	request etcdstore.PageRequest,
) (etcdstore.Page[TenantRecord], error) {
	return recordquery.ListPrimary(
		ctx,
		repository.store,
		"tenants",
		"global",
		"-",
		TenantPrefix,
		ids.KindTenant,
		request,
		DecodeTenant,
		func(record TenantRecord) string { return record.ID },
		func(TenantRecord) bool { return true },
	)
}

func (repository *Reader) ListTenantProjects(
	ctx context.Context,
	tenantID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[ProjectRecord], error) {
	if err := recordcodec.ValidateID(ids.KindTenant, tenantID); err != nil {
		return etcdstore.Page[ProjectRecord]{}, err
	}
	return recordquery.ListIndex(
		ctx,
		repository.store,
		"projects",
		"tenant",
		tenantID,
		ProjectTenantOwnerPrefix(tenantID),
		ProjectKey,
		ids.KindProject,
		request,
		DecodeProject,
		func(record ProjectRecord) string { return record.ID },
		func(record ProjectRecord) bool {
			return record.Kind == ProjectKindTenant && record.TenantID == tenantID
		},
	)
}

func (repository *Reader) ListProjects(
	ctx context.Context,
	filter ProjectFilter,
	request etcdstore.PageRequest,
) (etcdstore.Page[ProjectRecord], error) {
	if filter.Kind != "" && filter.Kind != ProjectKindTenant && filter.Kind != ProjectKindBacking {
		return etcdstore.Page[ProjectRecord]{}, errs.New(errs.KindValidationFailed, "project kind must be tenant or backing")
	}
	if filter.TenantID != "" {
		if err := recordcodec.ValidateID(ids.KindTenant, filter.TenantID); err != nil {
			return etcdstore.Page[ProjectRecord]{}, err
		}
		if filter.Kind == ProjectKindBacking {
			return etcdstore.Page[ProjectRecord]{}, errs.New(
				errs.KindValidationFailed,
				"backing projects cannot have a tenant filter",
			)
		}
	}
	ownerID := filter.TenantID
	if ownerID == "" {
		ownerID = "-"
	}
	return recordquery.ListFilteredPrimary(
		ctx,
		repository.store,
		"projects",
		"filter:"+string(filter.Kind),
		ownerID,
		ProjectPrefix,
		ids.KindProject,
		request,
		DecodeProject,
		func(record ProjectRecord) string { return record.ID },
		func(record ProjectRecord) bool {
			return (filter.Kind == "" || record.Kind == filter.Kind) &&
				(filter.TenantID == "" || record.TenantID == filter.TenantID)
		},
	)
}

func (repository *Reader) ListBackingProjects(
	ctx context.Context,
	request etcdstore.PageRequest,
) (etcdstore.Page[ProjectRecord], error) {
	return recordquery.ListIndex(
		ctx,
		repository.store,
		"projects",
		"platform",
		"-",
		ProjectPlatformOwnerPrefix,
		ProjectKey,
		ids.KindProject,
		request,
		DecodeProject,
		func(record ProjectRecord) string { return record.ID },
		func(record ProjectRecord) bool {
			return record.Kind == ProjectKindBacking && record.TenantID == ""
		},
	)
}

func (repository *Reader) ListEnvironments(
	ctx context.Context,
	projectID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[EnvironmentRecord], error) {
	if err := recordcodec.ValidateID(ids.KindProject, projectID); err != nil {
		return etcdstore.Page[EnvironmentRecord]{}, err
	}
	return recordquery.ListIndex(
		ctx,
		repository.store,
		"environments",
		"project",
		projectID,
		EnvironmentOwnerPrefix(projectID),
		EnvironmentKey,
		ids.KindEnvironment,
		request,
		DecodeEnvironment,
		func(record EnvironmentRecord) string { return record.ID },
		func(record EnvironmentRecord) bool { return record.ProjectID == projectID },
	)
}
