package blueprints

import (
	"fmt"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"regexp"
	"time"
)

const (
	EnvironmentBlueprintMaxFiles      = 64
	EnvironmentBlueprintMaxFileBytes  = 256 * 1024
	environmentBlueprintMaxTotalBytes = 768 * 1024
	EnvironmentBlueprintMaxPathBytes  = 240

	EnvironmentDesiredRevisionParam = "desired_revision_id"
)

var environmentBlueprintInterpolationKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// EnvironmentBlueprintRevision is one immutable, verified desired-state
// input. RevisionID is the Task id that first reconciles it, so queued work can
// never be retargeted when a later apply advances the current pointer.
type EnvironmentBlueprintRevision struct {
	EnvironmentID  string
	RevisionID     string
	RootPath       string
	ComposeSources []string
	Interpolation  map[string]string
	Files          []EnvironmentBlueprintFile
	CreatedAt      time.Time
}
type EnvironmentBlueprintFile struct {
	Path    string
	Content []byte
}
type EnvironmentBlueprintHead struct {
	EnvironmentID string
	RevisionID    string
}

// EnvironmentBlueprintZoneChange is one immutable existing Zone fence or one
// new Zone and subnet reservation committed with the Blueprint head.
type EnvironmentBlueprintZoneChange struct {
	Current *etcdstore.Versioned[zonerecord.Record]
	Record  zonerecord.Record
}

// EnvironmentBlueprintServiceChange is one desired-only Service replacement
// committed with the Blueprint head. Current is nil only when the Blueprint
// first introduces the stable Service id.
type EnvironmentBlueprintServiceChange struct {
	Current *etcdstore.Versioned[servicerecord.ServiceRecord]
	Record  servicerecord.ServiceRecord
}

// EnvironmentBlueprintRouteChange is one exposure-only Route replacement or
// one new stable Route committed with its target Service desired state.
type EnvironmentBlueprintRouteChange struct {
	Current *etcdstore.Versioned[routerecord.Record]
	Record  routerecord.Record
}
type environmentBlueprintManifest struct {
	EnvironmentID  string                             `json:"environment_id"`
	RevisionID     string                             `json:"revision_id"`
	RootPath       string                             `json:"root"`
	ComposeSources []string                           `json:"compose_sources"`
	Interpolation  map[string]string                  `json:"interpolation"`
	Files          []environmentBlueprintManifestFile `json:"files"`
	CreatedAt      string                             `json:"created_at"`
}
type environmentBlueprintManifestFile struct {
	Path   string `json:"path"`
	Size   int    `json:"size"`
	SHA256 string `json:"sha256"`
}

func EnvironmentBlueprintHeadKey(environmentID string) string {
	return "/v1/records/environment-blueprints/" + environmentID + "/current"
}
func EnvironmentBlueprintRevisionsPrefix(environmentID string) string {
	return "/v1/records/environment-blueprints/" + environmentID + "/revisions/"
}
func environmentBlueprintRevisionPrefix(environmentID string, revisionID string) string {
	return EnvironmentBlueprintRevisionsPrefix(environmentID) + revisionID + "/"
}
func environmentBlueprintManifestKey(environmentID string, revisionID string) string {
	return environmentBlueprintRevisionPrefix(environmentID, revisionID) + "manifest"
}
func environmentBlueprintFileKey(environmentID string, revisionID string, index int) string {
	return environmentBlueprintRevisionPrefix(
		environmentID,
		revisionID,
	) + fmt.Sprintf(
		"files/%06d",
		index+1,
	)
}
