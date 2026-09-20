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

type ServiceDependency struct {
	Condition string   `json:"condition"`
	Phases    []string `json:"phases,omitempty"`
}

type ServiceLogging struct {
	MaxSize string `json:"max_size,omitempty"`
	MaxFile int    `json:"max_file,omitempty"`
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
	Image       string                    `json:"image"`
	Zones       []string                  `json:"zones"`
	Strategy    string                    `json:"strategy"`
	OnFailure   OnFailure                 `json:"on_failure"`
	Healthcheck ServiceHealthcheck        `json:"healthcheck"`
	Resources   ServiceResources          `json:"resources"`
	Expose      []string                  `json:"expose"`
	Restart     string                    `json:"restart"`
	Replicas    int                       `json:"replicas"`
	Hooks       *BackingHookConfiguration `json:"hooks,omitempty"`
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
	State            ServiceObservationState `json:"state" enum:"unavailable,absent,failed,stopped,starting,healthy,running,degraded"`
	ObservedAt       *time.Time              `json:"observed_at,omitempty"`
	ExpiresAt        *time.Time              `json:"expires_at,omitempty"`
	ServingReleaseID *string                 `json:"serving_release_id,omitempty"`
	ExpectedReplicas *uint32                 `json:"expected_replicas,omitempty" minimum:"1"`
	Replicas         *ServiceReplicaCounts   `json:"replicas,omitempty"`
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
