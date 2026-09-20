package etcd

import recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

const (
	tenantPrefix                   = "/v1/records/tenants/"
	projectPrefix                  = "/v1/records/projects/"
	environmentPrefix              = "/v1/records/environments/"
	environmentMutationEpochPrefix = "/v1/runtime/environment-mutation-epochs/"
	environmentOperationLockPrefix = "/v1/runtime/environment-operation-locks/"
	projectPlatformOwnerPrefix     = "/v1/indexes/projects/by-owner/platform/-/"
)

func tenantKey(id string) string      { return tenantPrefix + id }
func projectKey(id string) string     { return projectPrefix + id }
func environmentKey(id string) string { return environmentPrefix + id }

func environmentMutationEpochKey(environmentID string) string {
	return environmentMutationEpochPrefix + environmentID
}

func environmentOperationLockKey(environmentID string) string {
	return environmentOperationLockPrefix + environmentID
}

func tenantSlugKey(slug string) string {
	return "/v1/indexes/tenants/by-slug/global/-/" + recordcodec.EncodeKeySegment(slug)
}

func projectTenantSlugKey(tenantID string, slug string) string {
	return "/v1/indexes/projects/by-slug/tenant/" + tenantID + "/" + recordcodec.EncodeKeySegment(slug)
}

func projectPlatformSlugKey(slug string) string {
	return "/v1/indexes/projects/by-slug/platform/-/" + recordcodec.EncodeKeySegment(slug)
}

func projectSlugKey(record ProjectRecord) string {
	if record.Kind == ProjectKindBacking {
		return projectPlatformSlugKey(record.Slug)
	}
	return projectTenantSlugKey(record.TenantID, record.Slug)
}

func environmentNameKey(projectID string, name string) string {
	return "/v1/indexes/environments/by-name/project/" + projectID + "/" + recordcodec.EncodeKeySegment(name)
}

func projectTenantOwnerPrefix(tenantID string) string {
	return "/v1/indexes/projects/by-owner/tenant/" + tenantID + "/"
}

func projectOwnerKey(record ProjectRecord) string {
	if record.Kind == ProjectKindBacking {
		return projectPlatformOwnerPrefix + record.ID
	}
	return projectTenantOwnerPrefix(record.TenantID) + record.ID
}

func environmentOwnerPrefix(projectID string) string {
	return "/v1/indexes/environments/by-owner/project/" + projectID + "/"
}

func environmentOwnerKey(projectID string, environmentID string) string {
	return environmentOwnerPrefix(projectID) + environmentID
}
