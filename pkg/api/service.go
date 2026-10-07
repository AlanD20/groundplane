package api

import "time"

// ServiceRuntimeIntent is the Controller-owned operational state projected on
// Service responses. It is separate from Blueprint desired-state input.
type ServiceRuntimeIntent string

const (
	ServiceRuntimeIntentRunning ServiceRuntimeIntent = "running"
	ServiceRuntimeIntentStopped ServiceRuntimeIntent = "stopped"
	ServiceRuntimeIntentAbsent  ServiceRuntimeIntent = "absent"
)

type ServiceHealthcheck struct {
	HTTP        string `json:"http,omitempty"`
	TCP         string `json:"tcp,omitempty"`
	Pgrep       string `json:"pgrep,omitempty"`
	Interval    string `json:"interval,omitempty"`
	Timeout     string `json:"timeout,omitempty"`
	StartPeriod string `json:"start_period,omitempty"`
	Retries     int    `json:"retries,omitempty"`
}

type ServiceResources struct {
	Mem  string  `json:"mem,omitempty"`
	CPUs float64 `json:"cpus,omitempty"`
}

type ServiceMount struct {
	Volume string `json:"volume,omitempty"`
	File   string `json:"file,omitempty"`
	Mount  string `json:"mount"`
	RO     bool   `json:"ro,omitempty"`
}

type ServiceVolumeMount struct {
	Volume string `json:"volume" doc:"Stable Volume id in this Environment"`
	Mount  string `json:"mount" doc:"Absolute container path"`
	RO     bool   `json:"ro,omitempty"`
}

type ServiceDependency struct {
	Condition string   `json:"condition" enum:"service_started,service_healthy,service_completed_successfully"`
	Phases    []string `json:"phases,omitempty" enum:"start,deploy,rollback,always"`
}

type ServiceLogging struct {
	MaxSize string `json:"max_size,omitempty" doc:"Docker log rotation size such as 10m; empty disables the size override"`
	MaxFile int    `json:"max_file,omitempty" minimum:"0" doc:"Rotated log files to retain; zero disables the count override"`
}

type Service struct {
	ID                         string                       `json:"id"`
	EnvironmentID              string                       `json:"environment_id"`
	Name                       string                       `json:"name"`
	Image                      string                       `json:"image"`
	RuntimeIntent              ServiceRuntimeIntent         `json:"runtime_intent"`
	Observation                *ServiceObservation          `json:"observation,omitempty"`
	Zones                      []string                     `json:"zones,omitempty"`
	Strategy                   string                       `json:"strategy,omitempty"`   // declared default: "blue-green" | "recreate"
	OnFailure                  OnFailure                    `json:"on_failure,omitempty"` // declared default: "switch_back" | "leave_active"
	Healthcheck                *ServiceHealthcheck          `json:"healthcheck,omitempty"`
	Resources                  ServiceResources             `json:"resources,omitempty"`
	Command                    []string                     `json:"command,omitempty"`
	Entrypoint                 []string                     `json:"entrypoint,omitempty"`
	WorkingDir                 string                       `json:"working_dir,omitempty"`
	User                       string                       `json:"user,omitempty"`
	Mounts                     []ServiceMount               `json:"mounts,omitempty"`
	Aliases                    map[string][]string          `json:"aliases,omitempty"`
	DependsOn                  map[string]ServiceDependency `json:"depends_on,omitempty"`
	Expose                     []string                     `json:"expose,omitempty"`
	Restart                    string                       `json:"restart,omitempty"`
	Logging                    ServiceLogging               `json:"logging,omitempty"`
	Replicas                   int                          `json:"replicas,omitempty"`
	Adapter                    string                       `json:"adapter,omitempty"`
	FactsPrefix                string                       `json:"facts_prefix,omitempty"`
	Label                      string                       `json:"label,omitempty"`
	BackingNetworkID           string                       `json:"backing_network_id,omitempty"`
	ServingReleaseID           string                       `json:"serving_release_id,omitempty"`
	CurrentSuccessfulReleaseID string                       `json:"current_successful_release_id,omitempty"`
	Hooks                      *BackingHookConfiguration    `json:"hooks,omitempty"`
}

// ServiceDetail adds the lossless normalized native Compose source used by
// operator detail surfaces. Direct mutation responses and collection pages
// intentionally retain the smaller Service projection.
type ServiceDetail struct {
	Service
	NativeCompose string               `json:"native_compose,omitempty"`
	ReleaseLedger Page[ReleaseSummary] `json:"release_ledger"`
}

type ServiceCreate struct {
	EnvironmentID string             `json:"environment_id"`
	Name          string             `json:"name"`
	Image         string             `json:"image"`
	Zones         []string           `json:"zones"`
	Strategy      string             `json:"strategy"`
	OnFailure     OnFailure          `json:"on_failure"`
	Healthcheck   ServiceHealthcheck `json:"healthcheck"`
	Resources     ServiceResources   `json:"resources"`
	Expose        []string           `json:"expose"`
	Restart       string             `json:"restart"`
	Replicas      int                `json:"replicas"`
}

type ServiceEdit struct {
	VolumeMounts *[]ServiceVolumeMount         `json:"volume_mounts,omitempty" doc:"Replacement managed Volume mounts; omission preserves, an empty list removes all. File mounts are preserved. Applies on next deploy."`
	Command      *[]string                     `json:"command,omitempty" doc:"Replacement argument vector; omission preserves, an empty list removes the Compose override and restores the image command."`
	Entrypoint   *[]string                     `json:"entrypoint,omitempty" doc:"Replacement entrypoint vector; omission preserves, an empty list removes the Compose override and restores the image entrypoint."`
	WorkingDir   *string                       `json:"working_dir,omitempty" doc:"Replacement container working directory; omission preserves, an empty string restores the image default."`
	User         *string                       `json:"user,omitempty" doc:"Replacement container user; omission preserves, an empty string restores the image default."`
	Aliases      *map[string][]string          `json:"aliases,omitempty" doc:"Replacement network aliases keyed by joined Zone name; omission preserves, an empty object removes all aliases."`
	DependsOn    *map[string]ServiceDependency `json:"depends_on,omitempty" doc:"Replacement Service dependencies keyed by Service name; omission preserves, an empty object removes all dependencies."`
	Logging      *ServiceLogging               `json:"logging,omitempty" doc:"Replacement supported log rotation options; omission preserves, an empty object removes both overrides."`
	Image        string                        `json:"image"`
	Zones        []string                      `json:"zones"`
	Strategy     string                        `json:"strategy"`
	OnFailure    OnFailure                     `json:"on_failure"`
	Healthcheck  ServiceHealthcheck            `json:"healthcheck"`
	Resources    ServiceResources              `json:"resources"`
	Expose       []string                      `json:"expose"`
	Restart      string                        `json:"restart"`
	Replicas     int                           `json:"replicas"`
	Hooks        *BackingHookConfiguration     `json:"hooks,omitempty"`
}

type ServiceObservationState string

const (
	ServiceObservationUnavailable ServiceObservationState = "unavailable"
	ServiceObservationAbsent      ServiceObservationState = "absent"
	ServiceObservationFailed      ServiceObservationState = "failed"
	ServiceObservationStopped     ServiceObservationState = "stopped"
	ServiceObservationStarting    ServiceObservationState = "starting"
	ServiceObservationHealthy     ServiceObservationState = "healthy"
	ServiceObservationRunning     ServiceObservationState = "running"
	ServiceObservationDegraded    ServiceObservationState = "degraded"
)

// ServiceObservation contains either unavailable alone or a complete serving
// workload snapshot. Runtime intent and desired replica count remain separate.
type ServiceObservation struct {
	State            ServiceObservationState       `json:"state" enum:"unavailable,absent,failed,stopped,starting,healthy,running,degraded"`
	ObservedAt       *time.Time                    `json:"observed_at,omitempty"`
	ExpiresAt        *time.Time                    `json:"expires_at,omitempty"`
	ServingReleaseID *string                       `json:"serving_release_id,omitempty"`
	ExpectedReplicas *uint32                       `json:"expected_replicas,omitempty" minimum:"1"`
	Replicas         *ServiceReplicaCounts         `json:"replicas,omitempty"`
	Containers       []ServiceContainerObservation `json:"containers,omitempty"`
}

type ServiceContainerObservation struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Image   string `json:"image"`
	State   string `json:"state" enum:"created,running,paused,restarting,removing,exited,dead"`
	Health  string `json:"health" enum:"none,healthy,starting,unhealthy"`
	Replica uint32 `json:"replica" minimum:"1"`
}

// ServiceReplicaCounts partitions only the selected serving workload.
type ServiceReplicaCounts struct {
	Running      uint32 `json:"running"`
	Healthy      uint32 `json:"healthy"`
	Starting     uint32 `json:"starting"`
	Unhealthy    uint32 `json:"unhealthy"`
	Transitional uint32 `json:"transitional"`
	Stopped      uint32 `json:"stopped"`
	Failed       uint32 `json:"failed"`
}
