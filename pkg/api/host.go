package api

import (
	"github.com/AlanD20/groundplane/internal/common/storagemaintenance"
	"github.com/AlanD20/groundplane/internal/common/version"
	"time"
)

type HostCPU struct {
	Model string `json:"model"`
	Cores int    `json:"cores"`
	Load  int    `json:"load"`
}

type HostResource struct {
	Total   string `json:"total"`
	Used    string `json:"used"`
	UsedPct int    `json:"used_pct"`
}

type HostEtcd struct {
	Maintenance *storagemaintenance.Status `json:"maintenance,omitempty"`
	Software    *version.ContainerIdentity `json:"software,omitempty"`
	Version     *string                    `json:"version,omitempty"`
	Node        string                     `json:"node"`
	Status      HealthState                `json:"status"`
	DBSize      string                     `json:"db_size"`
}

type HostController struct {
	Service string                   `json:"service"`
	Status  HealthState              `json:"status"`
	Version string                   `json:"version"`
	Source  *version.BuildProvenance `json:"source,omitempty"`
	Update  ControllerUpdateState    `json:"update"`
}

type HostAgent struct {
	Status                       HealthState `json:"status"`
	PullInterval                 string      `json:"pull_interval"`
	MaxConcurrentTasks           int         `json:"max_concurrent_tasks"`
	MaxParallelOperationsPerTask int         `json:"max_parallel_operations_per_task"`
	Labels                       []string    `json:"labels"`
}

type Host struct {
	Hostname   string         `json:"hostname"`
	Arch       string         `json:"arch"`
	OS         string         `json:"os"`
	Uptime     string         `json:"uptime"`
	CPU        HostCPU        `json:"cpu"`
	Memory     HostResource   `json:"memory"`
	Disk       HostResource   `json:"disk"`
	Swap       HostResource   `json:"swap"`
	Docker     string         `json:"docker"`
	Etcd       HostEtcd       `json:"etcd"`
	Controller HostController `json:"controller"`
	Agent      HostAgent      `json:"agent"`
}

// HostTerminalOpen is the initial PTY geometry. The executable, user,
// environment, and working directory are fixed by the Controller.
type HostTerminalOpen struct {
	Columns int `json:"columns" minimum:"20" maximum:"500"`
	Rows    int `json:"rows" minimum:"5" maximum:"300"`
}

type ContainerTerminalOpen struct {
	ContainerID      string   `json:"container_id" minLength:"64" maxLength:"64" pattern:"^[0-9a-f]{64}$"`
	User             string   `json:"user" default:"root" minLength:"1" maxLength:"128"`
	Command          []string `json:"command" minItems:"1" maxItems:"64"`
	WorkingDirectory string   `json:"working_directory,omitempty" maxLength:"4096"`
	Columns          int      `json:"columns" minimum:"20" maximum:"500"`
	Rows             int      `json:"rows" minimum:"5" maximum:"300"`
}

// TerminalSession is the content-free projection of one transient shell.
// Ownership is carried only by an HttpOnly browser-session cookie.
type TerminalSession struct {
	ID                     string    `json:"id"`
	Host                   string    `json:"host"`
	User                   string    `json:"user"`
	Kind                   string    `json:"kind" enum:"host,container"`
	ServiceID              string    `json:"service_id,omitempty"`
	ContainerID            string    `json:"container_id,omitempty"`
	ContainerName          string    `json:"container_name,omitempty"`
	State                  string    `json:"state" enum:"active,terminated"`
	Reason                 string    `json:"reason,omitempty"`
	ExitCode               *int      `json:"exit_code,omitempty"`
	InputInactivitySeconds int       `json:"input_inactivity_seconds"`
	ExpiresAt              time.Time `json:"expires_at"`
	StreamPath             string    `json:"stream_path"`
	Resumed                bool      `json:"resumed"`
}
