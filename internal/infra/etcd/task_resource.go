package etcd

const (
	// TaskResourceKindParam closes the durable Controller Task dispatch catalog.
	TaskResourceKindParam             = "resource_kind"
	TaskResourceAgent                 = "agent"
	TaskResourceEntry                 = "entry"
	TaskResourceRoute                 = "route"
	TaskResourceScript                = "script"
	TaskResourceSecret                = "secret"
	TaskResourceConnector             = "connector"
	TaskResourceBackingZone           = "backing_zone"
	TaskResourceService               = "service"
	TaskResourceReleaseGroup          = "release_group"
	TaskComposeArtifactParam          = "compose_artifact_id"
	TaskEntryEnvironmentParam         = "entry_environment_id"
	TaskEntryTenantSlugParam          = "entry_tenant_slug"
	TaskEntryProjectSlugParam         = "entry_project_slug"
	TaskEntryEnvironmentNameParam     = "entry_environment_name"
	TaskEntryAuthorizedVolumeDirParam = "entry_authorized_volume_dir"
	TaskEntryCloudflareComponentParam = "entry_cloudflare_component_id"
	TaskEntryCloudflareRevisionParam  = "entry_cloudflare_component_revision"
	TaskRouteEnvironmentParam         = "route_environment_id"
	TaskServiceEnvironmentParam       = "service_environment_id"
	TaskZoneEnvironmentParam          = "zone_environment_id"
	TaskZoneImpactTokenParam          = "zone_impact_token"
)
