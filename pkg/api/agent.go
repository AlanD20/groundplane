package api

import "time"

type AgentStatus string

const (
	AgentPending  AgentStatus = "pending"
	AgentHealthy  AgentStatus = "healthy"
	AgentDegraded AgentStatus = "degraded"
	AgentStopped  AgentStatus = "stopped"
)

// Agent is the stable public projection of the Controller-owned local Agent.
type Agent struct {
	ID           string            `json:"id"`
	Host         string            `json:"host"`
	Status       AgentStatus       `json:"status"`
	Version      *string           `json:"version"`
	Labels       map[string]string `json:"labels"`
	LastReportAt *time.Time        `json:"last_report_at"`
	InFlight     int               `json:"in_flight"`
}
