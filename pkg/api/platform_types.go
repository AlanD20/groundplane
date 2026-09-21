package api

import (
	"time"
)

type ComponentEnableRequest struct {
	Config *ComponentConfigMutationInput `json:"config,omitempty"`
}

// HealthState is the closed health vocabulary rendered by the Console.
type HealthState string

const (
	HealthHealthy  HealthState = "healthy"
	HealthDegraded HealthState = "degraded"
	HealthFailed   HealthState = "failed"
	HealthStopped  HealthState = "stopped"
	HealthPending  HealthState = "pending"
)

type CoreOverview struct {
	Components []CoreComponent `json:"components"`
	Agents     []AgentSummary  `json:"agents"`
}

type CoreComponent struct {
	Kind    string `json:"kind"` // coredns | agent | controller
	Version string `json:"version"`
	Healthy bool   `json:"healthy"`
}

type AgentSummary struct {
	ID     string            `json:"id"`
	Online bool              `json:"online"`
	Labels map[string]string `json:"labels,omitempty"`
}

type AgentConfig struct {
	PullIntervalSeconds int               `json:"pull_interval_seconds"`
	MaxConcurrentTasks  int               `json:"max_concurrent_tasks"`
	Labels              map[string]string `json:"labels"`
}

type ControllerConfigDocument struct {
	Path            string `json:"path"`
	Content         string `json:"content"`
	Revision        string `json:"revision"         pattern:"^sha256:[0-9a-f]{64}$"`
	RestartRequired bool   `json:"restart_required"`
}

type ControllerConfigReplacement struct {
	Content          string `json:"content"           maxLength:"1048576"`
	ExpectedRevision string `json:"expected_revision" pattern:"^sha256:[0-9a-f]{64}$"`
}

type LogEvent struct {
	Sequence      uint64    `json:"sequence"`
	ServiceID     string    `json:"service_id"`
	ServiceName   string    `json:"service_name"`
	ContainerID   string    `json:"container_id"`
	ContainerName string    `json:"container_name"`
	ReleaseID     string    `json:"release_id"`
	Slot          string    `json:"slot"`
	Stream        string    `json:"stream"`
	Timestamp     time.Time `json:"timestamp"`
	Line          string    `json:"line"`
	Truncated     bool      `json:"truncated"`
}

type LogSSEEvent struct {
	Event string   `json:"event"`
	Data  LogEvent `json:"data"`
}
