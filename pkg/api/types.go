// Package api holds the public REST/JSON DTOs and the resource map's
// request/response shapes — the human surface Console, CLI, and scripts
// all speak. This document is generated CODE-FIRST from the Controller's
// typed handlers (Huma or oapi-codegen) at build time and served at
// /openapi.json; the types here are the source those handlers are built
// from. See architecture.md, "API contracts (locked)", and api-cli.md
// section 4 for the exact endpoint shapes these back.
//
// These DTOs are deliberately independent of internal/core's domain
// model, not aliases of it (standards.md's import matrix: pkg/api never
// imports internal/core). The API contract and the internal domain
// model are allowed to diverge in shape; internal/controller's handlers
// translate between them.
package api

// --- Core entity DTOs (mirror internal/core's fields at the API
// surface; see mvp.md's "Desired state" and blueprint.md's extension
// grammar for the full internal shape) ---

type Tenant struct {
	ID   string `json:"id"`
	Slug string `json:"slug"`
	Name string `json:"name"`
}

type Project struct {
	ID       string `json:"id"`
	TenantID string `json:"tenant_id,omitempty"`
	Slug     string `json:"slug"`
	Name     string `json:"name"`
	Kind     string `json:"kind"` // "tenant" | "backing"
}

type Environment struct {
	ID        string `json:"id"`
	ProjectID string `json:"project_id"`
	Slug      string `json:"slug"`
	Name      string `json:"name"`
	VolumeDir string `json:"volume_dir"`
}

type Zone struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Subnet   string `json:"subnet,omitempty"`
	Internal bool   `json:"internal,omitempty"`
	OwnedBy  string `json:"owned_by,omitempty"`
}

type Service struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Image       string   `json:"image"`
	Zones       []string `json:"zones,omitempty"`
	Strategy    string   `json:"strategy,omitempty"`   // declared default: "blue-green" | "recreate"
	OnFailure   string   `json:"on_failure,omitempty"` // declared default: "switch_back" | "leave_active"
	Replicas    int      `json:"replicas,omitempty"`
	Adapter     string   `json:"adapter,omitempty"`
	FactsPrefix string   `json:"facts_prefix,omitempty"`
}

// EntrySource is the discriminated union api-cli.md section 4 shows:
// literal, secret_ref, and fact are mutually exclusive.
type EntrySource struct {
	Kind      string   `json:"kind"` // "literal" | "secret_ref" | "fact"
	Literal   string   `json:"literal,omitempty"`
	SecretRef string   `json:"secret_ref,omitempty"`
	Fact      *FactRef `json:"fact,omitempty"`
}

type FactRef struct {
	AttachID string `json:"attach_id"`
	Fact     string `json:"fact"` // e.g. "pg16_URL"
}

type Entry struct {
	ID       string      `json:"id"`
	Type     string      `json:"type"`           // "env" | "file"
	Key      string      `json:"key,omitempty"`  // Type=env
	Path     string      `json:"path,omitempty"` // Type=file
	Source   EntrySource `json:"source"`
	Exposure []string    `json:"exposure"` // service names, or ["all"]
	Secret   bool        `json:"secret"`
}

// AttachStatus values mirror internal/core's lifecycle exactly:
// pending -> provisioning -> ready, with terminal failed and
// detaching -> detached paths.
type Attach struct {
	ID                   string   `json:"id"`
	Name                 string   `json:"name"`
	ServiceID            string   `json:"service_id"`
	BackingServiceID     string   `json:"backing_service_id"`
	BackingEnvironmentID string   `json:"backing_environment_id,omitempty"`
	Grants               []string `json:"grants,omitempty"` // other attach ids
	Status               string   `json:"status"`
}

type Route struct {
	ID          string `json:"id"`
	Host        string `json:"host,omitempty"`
	Path        string `json:"path,omitempty"`
	ServiceName string `json:"service"`
	Exposure    string `json:"exposure"` // "public" | "internal"
}

type Volume struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Script struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	ServiceName string `json:"service"`
	Body        string `json:"script"`
	When        string `json:"when"`
}

// Component is the generic environment-component record. Caddy and Cloudflare
// Tunnel are component KINDS ("caddy", "cloudflare-tunnel"),
// not bespoke resources — see blueprint.md, "x-gp-components".
type Component struct {
	ID                string         `json:"id"`
	EnvironmentID     string         `json:"environment_id"`
	Kind              string         `json:"kind"`
	Enabled           bool           `json:"enabled"`
	Config            map[string]any `json:"config,omitempty"`
	GeneratedServices []string       `json:"generated_services,omitempty"`
	Healthy           bool           `json:"healthy"`
}

// Router is the READ-ONLY projection grouping ingress components — GET
// only, never PUT (api-cli.md: "router | GET /environments/{id}/router
// read-only projection grouping ingress components").
type Router struct {
	Caddy  *ComponentProjection `json:"caddy,omitempty"`
	Tunnel *ComponentProjection `json:"tunnel,omitempty"`
}

type ComponentProjection struct {
	ComponentID string `json:"component_id"`
	Enabled     bool   `json:"enabled"`
	PinnedIPv4  string `json:"pinned_ipv4,omitempty"`
}

type BackupPolicy struct {
	Enabled      bool           `json:"enabled"`
	Frequency    string         `json:"frequency,omitempty"`
	Keep         int            `json:"keep,omitempty"`
	Encryption   string         `json:"encryption,omitempty"`
	ConnectorID  string         `json:"connector_id,omitempty"`
	Sources      []BackupSource `json:"sources,omitempty"`
	AgeRecipient string         `json:"age_recipient,omitempty"`
	KeyEra       int            `json:"key_era,omitempty"`
}

type BackupSource struct {
	ID   string `json:"id"`
	Kind string `json:"kind"` // "attach" | "volume" | "config"
	Ref  string `json:"ref,omitempty"`
}

type RecoveryPoint struct {
	ID         string `json:"id"`
	SourceID   string `json:"source_id"`
	SourceKind string `json:"source_kind"`
	CreatedAt  string `json:"created_at"` // RFC3339
	Locator    string `json:"locator"`
	Size       int64  `json:"size,omitempty"`
	Encrypted  bool   `json:"encrypted"`
	KeyEra     int    `json:"key_era,omitempty"`
	Status     string `json:"status"` // "verified" | "failed"
}

type Runner struct {
	ID        string   `json:"id"`
	TenantID  string   `json:"tenant_id"`
	ProjectID string   `json:"project_id,omitempty"` // set => repo-scoped; unset => org-scoped
	Labels    []string `json:"labels,omitempty"`
	Online    bool     `json:"online"`
}

type Secret struct {
	ID        string `json:"id"`
	ProjectID string `json:"project_id"`
	Kind      string `json:"kind"` // "env_var" | "file"
	Ref       string `json:"ref"`
}

// Connector is owned by exactly one environment.
type Connector struct {
	ID            string            `json:"id"`
	EnvironmentID string            `json:"environment_id"`
	Kind          string            `json:"kind"`
	Credentials   map[string]string `json:"credentials,omitempty"`
}

// ReleaseRecord is one per-service deploy/rollback ledger entry — see
// blueprint.md, "x-gp-release" for the exact field set this mirrors.
type ReleaseRecord struct {
	DeploymentID string  `json:"deployment_id"`
	ServiceID    string  `json:"service_id"`
	Image        string  `json:"image"`
	Tag          string  `json:"tag"`
	Digest       string  `json:"digest,omitempty"`
	Strategy     string  `json:"strategy"`
	Slot         string  `json:"slot,omitempty"`
	OnFailure    string  `json:"on_failure"`
	TaskID       string  `json:"task_id"`
	Status       string  `json:"status"` // pending|running|candidate_healthy|switching|active|failed|superseded|aborted|timed_out
	StartedAt    string  `json:"started_at"`
	CompletedAt  *string `json:"completed_at,omitempty"`
}

// ReleaseGroup coordinates multiple services under one task lock and
// one failure policy — never inferred, always explicit. See
// blueprint.md, "x-gp-release-group".
type ReleaseGroup struct {
	ID       string   `json:"id"`
	Name     string   `json:"name"`
	Services []string `json:"services"`
	Order    []string `json:"order,omitempty"`
	Tag      string   `json:"tag,omitempty"`
}

// --- Ops action request bodies (every action returns a TaskAccepted) ---

type DeployRequest struct {
	Tag       string `json:"tag,omitempty"`        // omitted => the service's current tag
	Strategy  string `json:"strategy,omitempty"`   // omitted => the service's declared default
	OnFailure string `json:"on_failure,omitempty"` // omitted => "switch_back"
}

type RollbackRequest struct {
	Tag string `json:"tag,omitempty"` // omitted => the pre-selected previous tag
}

type ReleaseGroupDeployRequest struct {
	Tag string `json:"tag"`
}

type AttachRequest struct {
	ServiceID        string   `json:"service_id"`
	BackingServiceID string   `json:"backing_service_id"`
	Name             string   `json:"name,omitempty"` // omitted => Controller-suggested, made unique within the environment
	Grants           []string `json:"grants,omitempty"`
}

type BackupRunRequest struct {
	SourceIDs []string `json:"source_ids,omitempty"` // empty = every enabled source
}

type RestoreRequest struct {
	SourceID        string `json:"source_id"`
	RecoveryPointID string `json:"recovery_point_id,omitempty"` // empty = latest
	AgeIdentity     string `json:"age_identity,omitempty"`
}

type ScriptRunRequest struct {
	Parameters map[string]string `json:"parameters,omitempty"`
}

type ComponentEnableRequest struct {
	Config map[string]any `json:"config,omitempty"`
}

type RunnerCreateRequest struct {
	TenantID          string `json:"tenant_id,omitempty"`
	ProjectID         string `json:"project_id,omitempty"`
	RegistrationToken string `json:"registration_token"` // discarded by the Controller after registration
}

// --- Task / activity ---

// TaskStatus is the durable, persisted state — "acked" is a streamed
// event, not a terminal status. See api-cli.md, "Task states".
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskRunning   TaskStatus = "running"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
	TaskAborted   TaskStatus = "aborted"
	TaskTimedOut  TaskStatus = "timed_out"
)

type Task struct {
	ID          string     `json:"id"`
	OperationID string     `json:"operation_id"`
	RetryOf     string     `json:"retry_of,omitempty"`
	PlanHash    string     `json:"plan_hash,omitempty"`
	Type        string     `json:"type"`
	Target      string     `json:"target"`
	Status      TaskStatus `json:"status"`
	Steps       []TaskStep `json:"steps,omitempty"`
}

type TaskStep struct {
	Name   string     `json:"name"`
	Status TaskStatus `json:"status"`
}

// TaskAccepted is the 202 body every action endpoint returns — the
// field is `task_id`, snake_case, matching every other JSON field name
// in this API (api-cli.md: "Snake_case everywhere").
type TaskAccepted struct {
	TaskID string `json:"task_id"`
}

// --- Pagination ---

type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

// --- Host / core / agents ---

type Host struct {
	Etcd struct {
		Endpoints []string `json:"endpoints"`
		Healthy   bool     `json:"healthy"`
	} `json:"etcd"`
}

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
	Labels              map[string]string `json:"labels,omitempty"`
}
