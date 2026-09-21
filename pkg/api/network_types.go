package api

type Zone struct {
	ID            string        `json:"id"`
	EnvironmentID string        `json:"environment_id"`
	Name          string        `json:"name"`
	Subnet        string        `json:"subnet"`
	Internal      bool          `json:"internal"`
	OwnerKind     ZoneOwnerKind `json:"owner_kind" enum:"environment,backing_project"`
	OwnerID       string        `json:"owner_id"`
}

type ZoneCreate struct {
	EnvironmentID string `json:"environment_id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Name          string `json:"name"           pattern:"^[A-Za-z0-9._-]+$"`
	Subnet        string `json:"subnet"`
	Internal      bool   `json:"internal"`
}

type ZoneOwnerKind string

const (
	ZoneOwnerEnvironment    ZoneOwnerKind = "environment"
	ZoneOwnerBackingProject ZoneOwnerKind = "backing_project"
)

type ZoneRemovalImpactMode string

const (
	ZoneRemovalImpactOrdinary ZoneRemovalImpactMode = "ordinary"
	ZoneRemovalImpactCascade  ZoneRemovalImpactMode = "backing_cascade"
)

type ZoneRemovalImpactAttach struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	EnvironmentID string `json:"environment_id"`
	ServiceID     string `json:"service_id"`
	Database      string `json:"database,omitempty"`
	Status        string `json:"status"`
}

type ZoneRemovalImpactService struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	EnvironmentID string `json:"environment_id"`
}

type ZoneRemovalImpactDatabase struct {
	AttachID string `json:"attach_id"`
	Name     string `json:"name"`
}

type ZoneRemovalImpact struct {
	ZoneID      string                      `json:"zone_id"`
	ZoneName    string                      `json:"zone_name"`
	Mode        ZoneRemovalImpactMode       `json:"mode" enum:"ordinary,backing_cascade"`
	ImpactToken string                      `json:"impact_token"`
	Attaches    []ZoneRemovalImpactAttach   `json:"attaches"`
	Services    []ZoneRemovalImpactService  `json:"services"`
	Databases   []ZoneRemovalImpactDatabase `json:"databases"`
}

type Route struct {
	ID              string `json:"id"                pattern:"^rte_[0-9A-HJKMNP-TV-Z]{26}$"`
	EnvironmentID   string `json:"environment_id"    pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Host            string `json:"host,omitempty"    maxLength:"253"`
	Path            string `json:"path"              minLength:"1" maxLength:"2048"`
	Exposure        string `json:"exposure"          enum:"public,internal"`
	TargetServiceID string `json:"target_service_id" pattern:"^svc_[0-9A-HJKMNP-TV-Z]{26}$"`
	TargetPort      uint16 `json:"target_port"        minimum:"1"`
	Status          string `json:"status"             enum:"unserved,pending,served,degraded"`
}

type RouteTaskAccepted struct {
	Route  Route  `json:"route"`
	TaskID string `json:"task_id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type RouteCreate struct {
	EnvironmentID   string `json:"environment_id"    pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Host            string `json:"host,omitempty"    maxLength:"253"`
	Path            string `json:"path,omitempty"    maxLength:"2048"`
	Exposure        string `json:"exposure"          enum:"public,internal"`
	TargetServiceID string `json:"target_service_id" pattern:"^svc_[0-9A-HJKMNP-TV-Z]{26}$"`
	TargetPort      uint16 `json:"target_port"        minimum:"1"`
}

type RouteEdit struct {
	Exposure string `json:"exposure" enum:"public,internal"`
}

type RoutePage struct {
	Items      []Route `json:"items"`
	NextCursor string  `json:"next_cursor,omitempty"`
}
