package api

import "time"

type AgentStatus string

// AgentUpdate selects an independently distributed, immutable Agent image.
type AgentUpdate struct {
	Image string `json:"image" minLength:"1" maxLength:"1024"`
}

const (
	AgentPending  AgentStatus = "pending"
	AgentHealthy  AgentStatus = "healthy"
	AgentDegraded AgentStatus = "degraded"
	AgentStopped  AgentStatus = "stopped"
)

// Agent is the stable public projection of the Controller-owned local Agent.
type Agent struct {
	ID               string            `json:"id"`
	EnrollmentTaskID string            `json:"enrollment_task_id"`
	Host             string            `json:"host"`
	Status           AgentStatus       `json:"status"`
	Version          *string           `json:"version"`
	Labels           map[string]string `json:"labels"`
	ReadyAt          *time.Time        `json:"ready_at"`
	LastReportAt     *time.Time        `json:"last_report_at"`
	InFlight         int               `json:"in_flight"`
}
