package etcd

const (
	// TaskResourceKindParam closes the durable Controller Task dispatch catalog.
	TaskResourceKindParam       = "resource_kind"
	TaskResourceAgent           = "agent"
	TaskResourceEntry           = "entry"
	TaskResourceRoute           = "route"
	TaskResourceScript          = "script"
	TaskResourceSecret          = "secret"
	TaskResourceConnector       = "connector"
	TaskResourceBackingZone     = "backing_zone"
	TaskResourceService         = "service"
	TaskResourceVolume          = "volume"
	TaskComposeArtifactParam    = "compose_artifact_id"
	TaskEntryEnvironmentParam   = "entry_environment_id"
	TaskRouteEnvironmentParam   = "route_environment_id"
	TaskServiceEnvironmentParam = "service_environment_id"
	TaskZoneEnvironmentParam    = "zone_environment_id"
	TaskZoneImpactTokenParam    = "zone_impact_token"
)
