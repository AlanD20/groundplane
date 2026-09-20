package hierarchy

import recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

const (
	TenantPrefix                   = "/v1/records/tenants/"
	ProjectPrefix                  = "/v1/records/projects/"
	EnvironmentPrefix              = "/v1/records/environments/"
	EnvironmentMutationEpochPrefix = "/v1/runtime/environment-mutation-epochs/"
	EnvironmentOperationLockPrefix = "/v1/runtime/environment-operation-locks/"
	ProjectPlatformOwnerPrefix     = "/v1/indexes/projects/by-owner/platform/-/"
)

func TenantKey(id string) string      { return TenantPrefix + id }
func ProjectKey(id string) string     { return ProjectPrefix + id }
func EnvironmentKey(id string) string { return EnvironmentPrefix + id }

func EnvironmentMutationEpochKey(environmentID string) string {
	return EnvironmentMutationEpochPrefix + environmentID
}

func EnvironmentOperationLockKey(environmentID string) string {
	return EnvironmentOperationLockPrefix + environmentID
}

func TenantSlugKey(slug string) string {
	return "/v1/indexes/tenants/by-slug/global/-/" + recordcodec.EncodeKeySegment(slug)
}

func ProjectTenantSlugKey(tenantID string, slug string) string {
	return "/v1/indexes/projects/by-slug/tenant/" + tenantID + "/" + recordcodec.EncodeKeySegment(slug)
}

func ProjectPlatformSlugKey(slug string) string {
	return "/v1/indexes/projects/by-slug/platform/-/" + recordcodec.EncodeKeySegment(slug)
}

func ProjectSlugKey(record ProjectRecord) string {
	if record.Kind == ProjectKindBacking {
		return ProjectPlatformSlugKey(record.Slug)
	}
	return ProjectTenantSlugKey(record.TenantID, record.Slug)
}

func EnvironmentNameKey(projectID string, name string) string {
	return "/v1/indexes/environments/by-name/project/" + projectID + "/" + recordcodec.EncodeKeySegment(name)
}

func ProjectTenantOwnerPrefix(tenantID string) string {
	return "/v1/indexes/projects/by-owner/tenant/" + tenantID + "/"
}

func ProjectOwnerKey(record ProjectRecord) string {
	if record.Kind == ProjectKindBacking {
		return ProjectPlatformOwnerPrefix + record.ID
	}
	return ProjectTenantOwnerPrefix(record.TenantID) + record.ID
}

func EnvironmentOwnerPrefix(projectID string) string {
	return "/v1/indexes/environments/by-owner/project/" + projectID + "/"
}

func EnvironmentOwnerKey(projectID string, environmentID string) string {
	return EnvironmentOwnerPrefix(projectID) + environmentID
}
