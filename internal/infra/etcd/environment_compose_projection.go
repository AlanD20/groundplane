package etcd

import (
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
)

type EnvironmentVolumeIdentity struct {
	ID   string `json:"id"`
	Slug string `json:"slug"`
	Key  string `json:"key"`
}

type EnvironmentServiceVolumeMount struct {
	ServiceID string `json:"service_id"`
	VolumeID  string `json:"volume_id"`
	Target    string `json:"target"`
	ReadOnly  bool   `json:"read_only"`
}
type EnvironmentZoneProjection struct {
	EnvironmentID string    `json:"environment_id"`
	Desired       core.Zone `json:"desired"`
}
type EnvironmentServiceProjection struct {
	EnvironmentID    string       `json:"environment_id"`
	BackingNetworkID string       `json:"backing_network_id,omitempty"`
	Desired          core.Service `json:"desired"`
}
type EnvironmentRouteProjection struct {
	EnvironmentID     string     `json:"environment_id"`
	Desired           core.Route `json:"desired"`
	DesiredGeneration uint64     `json:"desired_generation"`
}

// ManagedComponentRuntimeSource pins the one Compose service that may still
// exist for an Environment Component after the source revision was attempted.
// A projection contains at most one source per fixed Component singleton.
type ManagedComponentRuntimeSource struct {
	ComponentKind  core.ComponentKind `json:"component_kind"`
	ComponentID    string             `json:"component_id"`
	ServiceID      string             `json:"service_id"`
	ComposeName    string             `json:"compose_service_name"`
	RevisionID     string             `json:"source_revision_id"`
	ArtifactID     string             `json:"source_artifact_id"`
	ArtifactSHA256 string             `json:"source_artifact_sha256"`
}

// EnvironmentComposeProjection is the sorted durable input for one Environment render.
type EnvironmentComposeProjection struct {
	EnvironmentID                  string                               `json:"environment_id"`
	RevisionID                     string                               `json:"blueprint_revision_id"`
	RenderGeneration               uint64                               `json:"render_generation"`
	ComposeArtifact                []byte                               `json:"compose_artifact"`
	NormalizedCompose              []byte                               `json:"normalized_compose"`
	RuntimeFiles                   []core.BlueprintFile                 `json:"runtime_files,omitempty"`
	ServiceExtensions              map[string]core.ServiceExtensionSpec `json:"service_extensions,omitempty"`
	DesiredZones                   []EnvironmentZoneProjection          `json:"desired_zones,omitempty"`
	DesiredServices                []EnvironmentServiceProjection       `json:"desired_services,omitempty"`
	DesiredRoutes                  []EnvironmentRouteProjection         `json:"desired_routes,omitempty"`
	Volumes                        []EnvironmentVolumeIdentity          `json:"volumes,omitempty"`
	VolumeMounts                   []EnvironmentServiceVolumeMount      `json:"volume_mounts,omitempty"`
	Components                     []componentrecord.Record             `json:"components,omitempty"`
	ManagedComponentRuntimeSources []ManagedComponentRuntimeSource      `json:"managed_component_runtime_sources,omitempty"`
	Entries                        []entryrecord.Record                 `json:"entries,omitempty"`
	Backup                         *EnvironmentBlueprintBackupPolicy    `json:"backup,omitempty"`
	core.ServiceDependencyPlans
	core.BlueprintRequirements
}

const environmentComposeProjectionPrefix = "/v1/records/environment-compose-projections/"

func environmentComposeProjectionKey(environmentID string) string {
	return environmentComposeProjectionPrefix + environmentID
}

func EnvironmentComposeProjectionStorageKey(environmentID string) string {
	return environmentComposeProjectionKey(environmentID)
}
