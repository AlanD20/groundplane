// Package network defines persistence-neutral Zone and Route capability
// values shared by Controller use cases and transport adapters.
package network

// PageRequest carries an opaque fixed-revision cursor.
type PageRequest struct {
	Limit  int
	Cursor string
}

// Page is a stable page of capability records.
type Page[T any] struct {
	Items      []T
	NextCursor string
}

// MutationResponse is the exact protected response persisted for replay.
type MutationResponse struct {
	Status      int
	ContentKind string
	Body        []byte
}

// ZoneOwnerKind identifies the Controller-derived Zone owner.
type ZoneOwnerKind string

const (
	ZoneOwnerEnvironment    ZoneOwnerKind = "environment"
	ZoneOwnerBackingProject ZoneOwnerKind = "backing_project"
)

// Zone is the Network capability projection of one managed Docker network.
type Zone struct {
	ID            string
	EnvironmentID string
	Name          string
	Subnet        string
	Internal      bool
	OwnerKind     ZoneOwnerKind
	OwnerID       string
}

// CreateZoneRequest contains only operator decisions; ownership is derived.
type CreateZoneRequest struct {
	EnvironmentID string
	Name          string
	Subnet        string
	Internal      bool
}

// Route is one immutable match and target plus its mutable exposure policy.
type RouteExposure string

const (
	RouteExposurePublic   RouteExposure = "public"
	RouteExposureInternal RouteExposure = "internal"
)

// Route is one immutable match and target plus its mutable exposure policy.
type Route struct {
	ID              string
	EnvironmentID   string
	Host            string
	Path            string
	Exposure        RouteExposure
	TargetServiceID string
	TargetPort      uint16
}

// CreateRouteRequest contains the complete immutable Route match and target.
type CreateRouteRequest struct {
	EnvironmentID   string
	Host            string
	Path            string
	Exposure        RouteExposure
	TargetServiceID string
	TargetPort      uint16
}

// EditRouteRequest changes only the mutable exposure policy.
type EditRouteRequest struct {
	Exposure RouteExposure
}

// ZoneRemovalImpactMode distinguishes ordinary cleanup from the sole cascade.
type ZoneRemovalImpactMode string

const (
	ZoneRemovalImpactOrdinary ZoneRemovalImpactMode = "ordinary"
	ZoneRemovalImpactCascade  ZoneRemovalImpactMode = "backing_cascade"
)

type ZoneRemovalImpactAttach struct {
	ID            string
	Name          string
	EnvironmentID string
	ServiceID     string
	Database      string
	Status        string
}

type ZoneRemovalImpactService struct {
	ID            string
	Name          string
	EnvironmentID string
}

type ZoneRemovalImpactDatabase struct {
	AttachID string
	Name     string
}

// ZoneRemovalImpact is the fixed dependency set fenced by ImpactToken.
type ZoneRemovalImpact struct {
	ZoneID      string
	ZoneName    string
	Mode        ZoneRemovalImpactMode
	ImpactToken string
	Attaches    []ZoneRemovalImpactAttach
	Services    []ZoneRemovalImpactService
	Databases   []ZoneRemovalImpactDatabase
}
