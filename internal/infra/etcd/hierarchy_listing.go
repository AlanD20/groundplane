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

func (repository *HierarchyRepository) ListTenants(
	ctx context.Context,
	request etcdstore.PageRequest,
) (etcdstore.Page[hierarchyrecord.TenantRecord], error) {
	return recordquery.ListPrimary(
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
	return recordquery.ListIndex(
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
	return recordquery.ListFilteredPrimary(
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
	return recordquery.ListIndex(
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
	return recordquery.ListIndex(
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
