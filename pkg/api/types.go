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
	ID          string `json:"id"`
	Slug        string `json:"slug"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type TenantCreate struct {
	Slug        string  `json:"slug"`
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
}

type TenantEdit struct {
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
}

type TenantRename struct {
	Slug string `json:"slug"`
}

type TenantPage struct {
	Items      []Tenant `json:"items"`
	NextCursor string   `json:"next_cursor,omitempty"`
}

type Project struct {
	ID          string `json:"id"`
	TenantID    string `json:"tenant_id,omitempty"`
	Slug        string `json:"slug"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Kind        string `json:"kind"` // "tenant" | "backing"
}

type ProjectCreate struct {
	TenantID    string  `json:"tenant_id"`
	Slug        string  `json:"slug"`
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
}

type ProjectEdit struct {
	Name *string `json:"name,omitempty"`
}

type ProjectRename struct {
	Slug string `json:"slug"`
}

type ProjectPage struct {
	Items      []Project `json:"items"`
	NextCursor string    `json:"next_cursor,omitempty"`
}

// BackingService is the public facade over one backing Project, its sole
// main Environment, and its sole adapter-backed Service. ProjectID is the
// facade's stable public identity; the other ids address shared operations.
type BackingService struct {
	ProjectID     string `json:"project_id"`
	EnvironmentID string `json:"environment_id"`
	ServiceID     string `json:"service_id"`
}

type Environment struct {
	ID                string                       `json:"id"`
	ProjectID         string                       `json:"project_id"`
	Name              string                       `json:"name"`
	VolumeDir         string                       `json:"volume_dir"`
	ProvisioningState EnvironmentProvisioningState `json:"provisioning_state"`
	CreateTaskID      *string                      `json:"create_task_id"`
}

type EnvironmentCreate struct {
	ProjectID string `json:"project_id"`
	Name      string `json:"name"`
}

type EnvironmentRename struct {
	Name string `json:"name"`
}

type EnvironmentPage struct {
	Items      []Environment `json:"items"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

type EnvironmentProvisioningState string

const (
	EnvironmentProvisioning EnvironmentProvisioningState = "provisioning"
	EnvironmentReady        EnvironmentProvisioningState = "ready"
	EnvironmentFailed       EnvironmentProvisioningState = "failed"
)

type Zone struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Subnet   string `json:"subnet,omitempty"`
	Internal bool   `json:"internal,omitempty"`
	OwnedBy  string `json:"owned_by,omitempty"`
}

// OnFailure is the shared service and release-group failure policy.
type OnFailure string

const (
	OnFailureSwitchBack  OnFailure = "switch_back"
	OnFailureLeaveActive OnFailure = "leave_active"
)

// ServiceRuntimeIntent is the Controller-owned operational state projected on
// Service responses. It is separate from Blueprint desired-state input.
type ServiceRuntimeIntent string

const (
	ServiceRuntimeIntentRunning ServiceRuntimeIntent = "running"
	ServiceRuntimeIntentStopped ServiceRuntimeIntent = "stopped"
	ServiceRuntimeIntentAbsent  ServiceRuntimeIntent = "absent"
)

type Service struct {
	ID               string               `json:"id"`
	Name             string               `json:"name"`
	Image            string               `json:"image"`
	RuntimeIntent    ServiceRuntimeIntent `json:"runtime_intent"`
	Zones            []string             `json:"zones,omitempty"`
	Strategy         string               `json:"strategy,omitempty"`   // declared default: "blue-green" | "recreate"
	OnFailure        OnFailure            `json:"on_failure,omitempty"` // declared default: "switch_back" | "leave_active"
	Replicas         int                  `json:"replicas,omitempty"`
	Adapter          string               `json:"adapter,omitempty"`
	FactsPrefix      string               `json:"facts_prefix,omitempty"`
	BackingNetworkID string               `json:"backing_network_id,omitempty"`
}

// EntrySource is the discriminated union api-cli.md section 4 shows:
// literal, secret_ref, and fact are mutually exclusive.
type EntrySource struct {
	Kind          string `json:"kind"` // "literal" | "secret_ref" | "fact"
	Literal       string `json:"literal,omitempty"`
	SecretRef     string `json:"secret_ref,omitempty"`
	AttachID      string `json:"attach_id,omitempty"`
	GrantAttachID string `json:"grant_attach_id,omitempty"`
	Fact          string `json:"fact,omitempty"` // e.g. "pg16_URL"
}

type Entry struct {
	ID       string      `json:"id"`
	Type     string      `json:"type"`           // "env" | "file"
	Key      string      `json:"key,omitempty"`  // Type=env
	Path     string      `json:"path,omitempty"` // Type=file
	UID      *uint32     `json:"uid,omitempty"`  // Type=file; required even when zero
	GID      *uint32     `json:"gid,omitempty"`  // Type=file; required even when zero
	Source   EntrySource `json:"source"`
	Exposure []string    `json:"exposure"` // service names, or ["all"]
	Secret   bool        `json:"secret"`
}

type EntryValue struct {
	Value string `json:"value"`
}

// AttachStatus values mirror internal/core's lifecycle exactly:
// pending -> provisioning -> ready, with terminal failed and
// detaching -> detached paths.
type Attach struct {
	ID                   string   `json:"id"`
	Name                 string   `json:"name"`
	ServiceIDs           []string `json:"service_ids"`
	BackingServiceID     string   `json:"backing_service_id"`
	BackingEnvironmentID string   `json:"backing_environment_id,omitempty"`
	BackingNetworkID     string   `json:"backing_network_id"`
	GrantAttachIDs       []string `json:"grant_attach_ids,omitempty"`
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

type BackupSourceKind string

const (
	BackupSourceAttach BackupSourceKind = "attach"
	BackupSourceVolume BackupSourceKind = "volume"
	BackupSourceConfig BackupSourceKind = "config"
)

type BackupSource struct {
	ID   string           `json:"id"`
	Kind BackupSourceKind `json:"kind"`
	Ref  string           `json:"ref,omitempty"`
}

type RecoveryPointStatus string

const RecoveryPointVerified RecoveryPointStatus = "verified"

type RecoveryPoint struct {
	ID         string              `json:"id"`
	SourceID   string              `json:"source_id"`
	SourceKind BackupSourceKind    `json:"source_kind"`
	CreatedAt  string              `json:"created_at"` // RFC3339
	Locator    string              `json:"locator"`
	Size       int64               `json:"size,omitempty"`
	Encrypted  bool                `json:"encrypted"`
	KeyEra     int                 `json:"key_era,omitempty"`
	Status     RecoveryPointStatus `json:"status"` // verified points only; failures remain task state
}

type Runner struct {
	ID        string   `json:"id"`
	TenantID  string   `json:"tenant_id"`
	ProjectID string   `json:"project_id,omitempty"` // set => repo-scoped; unset => org-scoped
	Labels    []string `json:"labels,omitempty"`
	Online    bool     `json:"online"`
}

// MaximumSecretValueBytes is the decoded UTF-8 input ceiling for one reusable
// Secret. The durable ciphertext ceiling remains 256 KiB; this leaves a full
// KiB for the locked single-recipient age envelope.
const MaximumSecretValueBytes = 255 << 10

type Secret struct {
	ID        string `json:"id"`
	Scope     string `json:"scope"` // "project" | "platform"
	ProjectID string `json:"project_id,omitempty"`
	Key       string `json:"key"`
	Kind      string `json:"kind"` // "env_var" | "file"
	Ref       string `json:"ref"`
	UpdatedAt string `json:"updated_at"`
}

// SecretCreateRequest keeps the value write-only. ProjectID and Platform are
// mutually exclusive; Path is required only for file Secrets.
type SecretCreateRequest struct {
	ProjectID string `json:"project_id,omitempty"`
	Platform  bool   `json:"platform,omitempty"`
	Key       string `json:"key"`
	Kind      string `json:"kind"`
	Path      string `json:"path,omitempty"`
	Value     string `json:"value"`
}

type SecretValue struct {
	Value string `json:"value"`
}

type ConnectorCredentialKind string

const (
	ConnectorCredentialSecretRef ConnectorCredentialKind = "secret_ref"
	ConnectorCredentialDirect    ConnectorCredentialKind = "direct"
)

// ConnectorCredential is the redacted response projection. Direct values
// are represented only by their source kind and are never returned.
type ConnectorCredential struct {
	Kind      ConnectorCredentialKind `json:"kind"`
	SecretRef string                  `json:"secret_ref,omitempty"`
}

// ConnectorCredentialInput is accepted only on connector creation. Exactly
// one of SecretRef or Value is supplied and direct Value is write-only.
type ConnectorCredentialInput struct {
	SecretRef string `json:"secret_ref,omitempty"`
	Value     string `json:"value,omitempty"`
}

// Connector is owned by exactly one environment and never falls back to a
// project or platform connector.
type Connector struct {
	ID            string                         `json:"id"`
	EnvironmentID string                         `json:"environment_id"`
	Name          string                         `json:"name"`
	Kind          string                         `json:"kind"`
	Endpoint      string                         `json:"endpoint,omitempty"`
	Bucket        string                         `json:"bucket,omitempty"`
	Prefix        string                         `json:"prefix,omitempty"`
	Region        string                         `json:"region,omitempty"`
	Credentials   map[string]ConnectorCredential `json:"credentials,omitempty"`
}

type ConnectorCreateRequest struct {
	Name        string                              `json:"name"`
	Kind        string                              `json:"kind"`
	Endpoint    string                              `json:"endpoint,omitempty"`
	Bucket      string                              `json:"bucket,omitempty"`
	Prefix      string                              `json:"prefix,omitempty"`
	Region      string                              `json:"region,omitempty"`
	Credentials map[string]ConnectorCredentialInput `json:"credentials,omitempty"`
}

// ReleaseRecord is one per-service deploy/rollback ledger entry — see
// blueprint.md, "x-gp-release" for the exact field set this mirrors.
type ReleaseRecord struct {
	DeploymentID string    `json:"deployment_id"`
	ServiceID    string    `json:"service_id"`
	Image        string    `json:"image"`
	Tag          string    `json:"tag"`
	Digest       string    `json:"digest,omitempty"`
	Strategy     string    `json:"strategy"`
	Slot         string    `json:"slot,omitempty"`
	OnFailure    OnFailure `json:"on_failure"`
	TaskID       string    `json:"task_id"`
	Status       string    `json:"status"` // pending|running|candidate_healthy|switching|active|failed|superseded|aborted|timed_out
	StartedAt    string    `json:"started_at"`
	CompletedAt  *string   `json:"completed_at,omitempty"`
}

// ReleaseGroup coordinates multiple services under one task lock and
// one failure policy — never inferred, always explicit. See
// blueprint.md, "x-gp-release-group".
type ReleaseGroup struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Services  []string  `json:"services"`
	Order     []string  `json:"order,omitempty"`
	Tag       string    `json:"tag,omitempty"`
	OnFailure OnFailure `json:"on_failure"`
}

// --- Ops action request bodies (every action returns a TaskAccepted) ---

type DeployRequest struct {
	Tag       string    `json:"tag,omitempty"`        // omitted => the service's current tag
	Strategy  string    `json:"strategy,omitempty"`   // omitted => the service's declared default
	OnFailure OnFailure `json:"on_failure,omitempty"` // omitted => "switch_back"
}

type RollbackRequest struct {
	Tag string `json:"tag,omitempty"` // omitted => the pre-selected previous tag
}

type ReleaseGroupDeployRequest struct {
	Tag string `json:"tag"`
}

type AttachRequest struct {
	ServiceIDs       []string `json:"service_ids"`
	BackingServiceID string   `json:"backing_service_id"`
	Name             string   `json:"name,omitempty"` // omitted => Controller-suggested, made unique within the environment
	GrantAttachIDs   []string `json:"grant_attach_ids,omitempty"`
}

type AttachRenameRequest struct {
	Name string `json:"name"`
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

// HealthState is the closed health vocabulary rendered by the Console.
type HealthState string

const (
	HealthHealthy  HealthState = "healthy"
	HealthDegraded HealthState = "degraded"
	HealthFailed   HealthState = "failed"
	HealthStopped  HealthState = "stopped"
	HealthPending  HealthState = "pending"
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
	Node   string      `json:"node"`
	Status HealthState `json:"status"`
	DBSize string      `json:"db_size"`
}

type HostController struct {
	Service string      `json:"service"`
	Status  HealthState `json:"status"`
	Version string      `json:"version"`
}

type HostAgent struct {
	Status        HealthState `json:"status"`
	PullInterval  string      `json:"pull_interval"`
	MaxConcurrent int         `json:"max_concurrent"`
	Labels        []string    `json:"labels"`
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
