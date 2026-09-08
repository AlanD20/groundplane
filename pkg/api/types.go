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

import "time"

// --- Core entity DTOs (mirror internal/core's fields at the API
// surface; see mvp.md's "Desired state" and blueprint.md's extension
// grammar for the full internal shape) ---

type Tenant struct {
	ID             string  `json:"id"`
	Slug           string  `json:"slug"`
	Name           string  `json:"name"`
	Description    string  `json:"description"`
	DeletionTaskID *string `json:"deletion_task_id"`
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
	ID             string  `json:"id"`
	TenantID       string  `json:"tenant_id,omitempty"`
	Slug           string  `json:"slug"`
	Name           string  `json:"name"`
	Description    string  `json:"description"`
	Kind           string  `json:"kind"` // "tenant" | "backing"
	DeletionTaskID *string `json:"deletion_task_id"`
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
	Authentication   string `json:"authentication,omitempty"`
	ProjectID        string `json:"project_id"`
	EnvironmentID    string `json:"environment_id"`
	ServiceID        string `json:"service_id"`
	BackingNetworkID string `json:"backing_network_id"`
}

type Environment struct {
	ID                string                       `json:"id"`
	ProjectID         string                       `json:"project_id"`
	Name              string                       `json:"name"`
	NetworkPool       string                       `json:"network_pool"`
	VolumeDir         string                       `json:"volume_dir"`
	ProvisioningState EnvironmentProvisioningState `json:"provisioning_state"`
	CreateTaskID      *string                      `json:"create_task_id"`
	DeletionTaskID    *string                      `json:"deletion_task_id"`
	NetworkCapacity   EnvironmentNetworkCapacity   `json:"network_capacity"`
}
type EnvironmentCreate struct {
	ProjectID   string `json:"project_id"`
	Name        string `json:"name"`
	NetworkPool string `json:"network_pool"`
}
type EnvironmentEdit struct {
	NetworkPool string `json:"network_pool"`
}

const EnvironmentBlueprintInitialRevision = "0"

type EnvironmentBlueprintDocument struct {
	EnvironmentID string `json:"environment_id"`
	Revision      string `json:"revision"`
	Document      string `json:"document"`
}

type BlueprintChangeAction string

const (
	BlueprintChangeCreate BlueprintChangeAction = "create"
	BlueprintChangeUpdate BlueprintChangeAction = "update"
	BlueprintChangeRetain BlueprintChangeAction = "retain"
)

type EnvironmentBlueprintChange struct {
	Resource string                `json:"resource"`
	Key      string                `json:"key"`
	Action   BlueprintChangeAction `json:"action" enum:"create,update,retain"`
}

type EnvironmentBlueprintValidation struct {
	Revision string                       `json:"revision"`
	Changes  []EnvironmentBlueprintChange `json:"changes"`
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
	ID            string        `json:"id"`
	EnvironmentID string        `json:"environment_id"`
	Name          string        `json:"name"`
	Subnet        string        `json:"subnet"`
	Internal      bool          `json:"internal"`
	OwnerKind     ZoneOwnerKind `json:"owner_kind" enum:"environment,backing_project"`
	OwnerID       string        `json:"owner_id"`
}

type ZoneCreate struct {
	EnvironmentID string `json:"environment_id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Name          string `json:"name"           pattern:"^[A-Za-z0-9._-]+$"`
	Subnet        string `json:"subnet"`
	Internal      bool   `json:"internal"`
}

type ZoneOwnerKind string

const (
	ZoneOwnerEnvironment    ZoneOwnerKind = "environment"
	ZoneOwnerBackingProject ZoneOwnerKind = "backing_project"
)

type ZoneRemovalImpactMode string

const (
	ZoneRemovalImpactOrdinary ZoneRemovalImpactMode = "ordinary"
	ZoneRemovalImpactCascade  ZoneRemovalImpactMode = "backing_cascade"
)

type ZoneRemovalImpactAttach struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	EnvironmentID string `json:"environment_id"`
	ServiceID     string `json:"service_id"`
	Database      string `json:"database,omitempty"`
	Status        string `json:"status"`
}

type ZoneRemovalImpactService struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	EnvironmentID string `json:"environment_id"`
}

type ZoneRemovalImpactDatabase struct {
	AttachID string `json:"attach_id"`
	Name     string `json:"name"`
}

type ZoneRemovalImpact struct {
	ZoneID      string                      `json:"zone_id"`
	ZoneName    string                      `json:"zone_name"`
	Mode        ZoneRemovalImpactMode       `json:"mode" enum:"ordinary,backing_cascade"`
	ImpactToken string                      `json:"impact_token"`
	Attaches    []ZoneRemovalImpactAttach   `json:"attaches"`
	Services    []ZoneRemovalImpactService  `json:"services"`
	Databases   []ZoneRemovalImpactDatabase `json:"databases"`
}

type Route struct {
	ID              string `json:"id"                pattern:"^rte_[0-9A-HJKMNP-TV-Z]{26}$"`
	EnvironmentID   string `json:"environment_id"    pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Host            string `json:"host,omitempty"    maxLength:"253"`
	Path            string `json:"path"              minLength:"1" maxLength:"2048"`
	Exposure        string `json:"exposure"          enum:"public,internal"`
	TargetServiceID string `json:"target_service_id" pattern:"^svc_[0-9A-HJKMNP-TV-Z]{26}$"`
	TargetPort      uint16 `json:"target_port"        minimum:"1"`
	Status          string `json:"status"             enum:"unserved,pending,served,degraded"`
}

type RouteTaskAccepted struct {
	Route  Route  `json:"route"`
	TaskID string `json:"task_id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type RouteCreate struct {
	EnvironmentID   string `json:"environment_id"    pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Host            string `json:"host,omitempty"    maxLength:"253"`
	Path            string `json:"path,omitempty"    maxLength:"2048"`
	Exposure        string `json:"exposure"          enum:"public,internal"`
	TargetServiceID string `json:"target_service_id" pattern:"^svc_[0-9A-HJKMNP-TV-Z]{26}$"`
	TargetPort      uint16 `json:"target_port"        minimum:"1"`
}

type RouteEdit struct {
	Exposure string `json:"exposure" enum:"public,internal"`
}

type RoutePage struct {
	Items      []Route `json:"items"`
	NextCursor string  `json:"next_cursor,omitempty"`
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
	Image       string             `json:"image"`
	Zones       []string           `json:"zones"`
	Strategy    string             `json:"strategy"`
	OnFailure   OnFailure          `json:"on_failure"`
	Healthcheck ServiceHealthcheck `json:"healthcheck"`
	Resources   ServiceResources   `json:"resources"`
	Expose      []string           `json:"expose"`
	Restart     string             `json:"restart"`
	Replicas    int                `json:"replicas"`
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
	UID      *int64      `json:"uid,omitempty"`  // Type=file; required even when zero
	GID      *int64      `json:"gid,omitempty"`  // Type=file; required even when zero
	Source   EntrySource `json:"source"`
	Exposure []string    `json:"exposure"` // service names, or ["all"]
	Secret   bool        `json:"secret"`
}

const MaximumEntryValueBytes = 256 << 10

type EntryCreateRequest struct {
	EnvironmentID string      `json:"environment_id"`
	Type          string      `json:"type"`
	Key           string      `json:"key,omitempty"`
	Path          string      `json:"path,omitempty"`
	UID           *int64      `json:"uid,omitempty"`
	GID           *int64      `json:"gid,omitempty"`
	Source        EntrySource `json:"source"`
	Exposure      []string    `json:"exposure"`
	Secret        bool        `json:"secret"`
}

type EntryValue struct {
	Value string `json:"value"`
}

type AttachFact struct {
	Key    string `json:"key"`
	Secret bool   `json:"secret"`
}

type AttachFactSet struct {
	GrantAttachID string       `json:"grant_attach_id,omitempty"`
	Facts         []AttachFact `json:"facts"`
}

type AttachFactValue struct {
	Value string `json:"value"`
}

type AttachCredentialMode string

const (
	AttachCredentialNew      AttachCredentialMode = "new"
	AttachCredentialExisting AttachCredentialMode = "existing"
)

// AttachCredential is a closed credential-source choice. AttachID is present
// only for existing credentials and always names the direct credential owner.
type AttachCredential struct {
	Mode     AttachCredentialMode `json:"mode" enum:"new,existing"`
	AttachID string               `json:"attach_id,omitempty" pattern:"^att_[0-9A-HJKMNP-TV-Z]{26}$"`
}

// AttachStatus values mirror internal/core's lifecycle exactly:
// pending -> provisioning -> ready, with terminal failed and
// detaching -> detached paths.
type Attach struct {
	ID                   string           `json:"id"`
	Name                 string           `json:"name"`
	ServiceID            string           `json:"service_id"`
	Credential           AttachCredential `json:"credential"`
	BackingProjectID     string           `json:"backing_project_id"`
	BackingServiceID     string           `json:"backing_service_id"`
	BackingEnvironmentID string           `json:"backing_environment_id,omitempty"`
	BackingNetworkID     string           `json:"backing_network_id"`
	GrantAttachIDs       []string         `json:"grant_attach_ids,omitempty"`
	FactSets             []AttachFactSet  `json:"fact_sets"`
	Status               string           `json:"status"`
}

type Script struct {
	ID                string `json:"id"`
	EnvironmentID     string `json:"environment_id"`
	Slug              string `json:"slug"`
	ServiceID         string `json:"service_id"`
	ServiceName       string `json:"service"`
	Body              string `json:"script"`
	When              string `json:"when"`
	Origin            string `json:"origin" enum:"api,blueprint"`
	ReconciliationKey string `json:"reconciliation_key,omitempty"`
	ActiveGeneration  uint64 `json:"active_generation"`
}

// ScriptCreate is the complete operator-authored durable Script input.
type ScriptCreate struct {
	EnvironmentID string `json:"environment_id"`
	Slug          string `json:"slug"`
	ServiceID     string `json:"service_id"`
	Body          string `json:"script"`
	When          string `json:"when"`
}

// ScriptEdit contains the mutable Script desired-state fields.
type ScriptEdit struct {
	Slug *string `json:"slug,omitempty"`
	Body *string `json:"script,omitempty"`
	When *string `json:"when,omitempty"`
}

// Component is the generic environment-component record. Caddy and Cloudflare
// Tunnel are component KINDS ("caddy", "cloudflare-tunnel"),
// not bespoke resources — see blueprint.md, "x-gp-components".
type Component struct {
	ID                string           `json:"id"`
	Owner             string           `json:"owner" enum:"environment,platform"`
	OwnerID           string           `json:"owner_id,omitempty"`
	EnvironmentID     string           `json:"environment_id"`
	Kind              string           `json:"kind"`
	Enabled           bool             `json:"enabled"`
	Config            *ComponentConfig `json:"config"`
	GeneratedServices []string         `json:"generated_services,omitempty"`
	PinnedIPv4        string           `json:"pinned_ipv4,omitempty"`
	Healthy           bool             `json:"healthy"`
	Status            string           `json:"status" enum:"disabled,pending,healthy,degraded,unknown"`
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

// ComponentConfig is the complete desired configuration singleton for one
// Component. Runtime and secret material are deliberately absent.
type ComponentConfig struct {
	Caddy            *CaddyComponentConfig            `json:"-"`
	CloudflareTunnel *CloudflareTunnelComponentConfig `json:"-"`
	CoreDNS          *CoreDNSComponentConfig          `json:"-"`
}

// ComponentConfigResponse is the generator-safe response envelope for the
// config singleton. Its nested config is null while disabled or unconfigured.
type ComponentConfigResponse struct {
	Config       *ComponentConfig    `json:"config"`
	ManagedFiles []ManagedConfigFile `json:"managed_files"`
}

// ManagedConfigFile is a side-effect-free Controller projection of one
// registered Component's authored template and current rendered content.
type ManagedConfigFile struct {
	Path     string `json:"path"`
	Template string `json:"template"`
	Rendered string `json:"rendered"`
}

type CaddyComponentConfig struct {
	ZoneIDs           []string `json:"zone_ids"`
	CaddyfileTemplate string   `json:"caddyfile_template,omitempty"`
}

type CloudflareTunnelComponentConfig struct {
	ZoneIDs  []string `json:"zone_ids"`
	SecretID string   `json:"secret_id"`
}

type CoreDNSComponentConfig struct {
	CorefileTemplate  string                  `json:"corefile_template"`
	UpstreamAuto      bool                    `json:"upstream_auto"`
	UpstreamResolvers []string                `json:"upstream_resolvers"`
	Forwarders        []ComponentDNSForwarder `json:"forwarders"`
	TailnetDelegation bool                    `json:"tailnet_delegation"`
}

type ComponentDNSForwarder struct {
	Domain    string   `json:"domain"`
	Resolvers []string `json:"resolvers"`
}

type ComponentConfigMutationRequest struct {
	Config ComponentConfigMutationInput `json:"config"`
}

type ComponentConfigMutationInput struct {
	Caddy            *CaddyComponentConfigMutationInput            `json:"-"`
	CloudflareTunnel *CloudflareTunnelComponentConfigMutationInput `json:"-"`
	CoreDNS          *CoreDNSComponentConfigMutationInput          `json:"-"`
}

type CaddyComponentConfigMutationInput struct {
	ZoneIDs           []string `json:"zone_ids"`
	CaddyfileTemplate string   `json:"caddyfile_template,omitempty"`
}

type CloudflareTunnelComponentConfigMutationInput struct {
	ZoneIDs    []string                        `json:"zone_ids"`
	Credential CloudflareTunnelCredentialInput `json:"credential"`
}

type CoreDNSComponentConfigMutationInput struct {
	CorefileTemplate  *string                  `json:"corefile_template"`
	UpstreamAuto      *bool                    `json:"upstream_auto"`
	UpstreamResolvers *[]string                `json:"upstream_resolvers"`
	Forwarders        *[]ComponentDNSForwarder `json:"forwarders"`
	TailnetDelegation *bool                    `json:"tailnet_delegation"`
}

type CloudflareTunnelCredentialInput struct {
	Mode       string `json:"mode"`
	SecretID   string `json:"secret_id,omitempty"`
	SecretName string `json:"secret_name,omitempty"`
	Token      string `json:"token,omitempty"`
}

type ComponentConfigMutationResult struct {
	Resource        ComponentConfig `json:"resource"`
	ReconcileTaskID *string         `json:"reconcile_task_id"`
}

// MaximumBackupPolicySources is the public replacement bound. It mirrors the
// persistence transaction budget and is intentionally available to clients.
const (
	MaximumBackupPolicySources       = 12
	MaximumBackupPolicyKeep    int64 = 9_007_199_254_740_991
)

// ValidBackupPolicyKeep reports whether keep is within the public integer
// range that every supported JSON consumer can represent exactly.
func ValidBackupPolicyKeep(keep int64) bool {
	return keep >= 1 && keep <= MaximumBackupPolicyKeep
}

// BackupPolicyReplacementRequest is one complete desired policy document.
type BackupPolicyReplacementRequest struct {
	Enabled     bool                `json:"enabled"`
	Frequency   string              `json:"frequency,omitempty" pattern:"^(\\*-\\*-\\* (?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]|(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) \\*-\\*-\\* (?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9])$"`
	Keep        int64               `json:"keep,omitempty" minimum:"1" maximum:"9007199254740991"`
	Encryption  BackupEncryption    `json:"encryption,omitempty" enum:"age,none"`
	ConnectorID string              `json:"connector_id,omitempty" pattern:"^con_[0-9A-HJKMNP-TV-Z]{26}$"`
	Sources     []BackupSourceInput `json:"sources" maxItems:"12" nullable:"false"`
}

type BackupEncryption string

const (
	BackupEncryptionAge  BackupEncryption = "age"
	BackupEncryptionNone BackupEncryption = "none"
)

// BackupPolicy is the effective Environment policy projection.
type BackupPolicy struct {
	Enabled      bool             `json:"enabled"`
	Frequency    string           `json:"frequency,omitempty" pattern:"^(\\*-\\*-\\* (?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]|(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) \\*-\\*-\\* (?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9])$"`
	Keep         int64            `json:"keep,omitempty" minimum:"1" maximum:"9007199254740991"`
	Encryption   BackupEncryption `json:"encryption,omitempty" enum:"age,none"`
	ConnectorID  string           `json:"connector_id,omitempty"`
	Sources      []BackupSource   `json:"sources" nullable:"false"`
	AgeRecipient string           `json:"age_recipient,omitempty"`
	KeyEra       int              `json:"key_era,omitempty"`
	KeyCreatedAt string           `json:"key_created_at,omitempty" format:"date-time"`
	KeyRotatedAt string           `json:"key_rotated_at,omitempty" format:"date-time"`
	NextRunAt    *string          `json:"next_run_at" format:"date-time"`
}

// BackupPolicyMutationResult carries the typed public value and the exact
// protected JSON representation committed for semantic replay.
type BackupPolicyMutationResult struct {
	Policy         BackupPolicy
	Representation []byte
}

type BackupSourceKind string

const (
	BackupSourceAttach BackupSourceKind = "attach"
	BackupSourceVolume BackupSourceKind = "volume"
	BackupSourceConfig BackupSourceKind = "config"
)

type BackupSourceInput struct {
	Kind     BackupSourceKind `json:"kind" enum:"attach,volume,config"`
	TargetID string           `json:"target_id"`
}

type BackupSource struct {
	ID       string           `json:"id"`
	Kind     BackupSourceKind `json:"kind" enum:"attach,volume,config"`
	TargetID string           `json:"target_id"`
}

type RecoveryPointStatus string

const RecoveryPointVerified RecoveryPointStatus = "verified"

type RecoveryPoint struct {
	ID         string              `json:"id"`
	SourceID   string              `json:"source_id"`
	SourceKind BackupSourceKind    `json:"source_kind" enum:"attach,volume,config"`
	TargetID   string              `json:"target_id"`
	CreatedAt  string              `json:"created_at" format:"date-time"`
	SizeBytes  int64               `json:"size_bytes"`
	Encrypted  bool                `json:"encrypted"`
	KeyEra     int                 `json:"key_era,omitempty"`
	Status     RecoveryPointStatus `json:"status" enum:"verified"`
}

type RecoveryPointPage struct {
	Items      []RecoveryPoint `json:"items"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

type Runner struct {
	ID           string          `json:"id"`
	Slug         string          `json:"slug"`
	TenantID     string          `json:"tenant_id"`
	ProjectID    string          `json:"project_id,omitempty"`
	GitHubURL    string          `json:"github_url"`
	Name         string          `json:"name"`
	Labels       []string        `json:"labels,omitempty"`
	Lifecycle    RunnerLifecycle `json:"lifecycle" enum:"provisioning,ready,failed,deleting"`
	CreateTaskID string          `json:"create_task_id"`
	RemoveTaskID *string         `json:"remove_task_id"`
	Online       bool            `json:"online"`
	ObservedAt   *string         `json:"observed_at"`
	CreatedAt    string          `json:"created_at"`
}

type RunnerLifecycle string

const (
	RunnerLifecycleProvisioning RunnerLifecycle = "provisioning"
	RunnerLifecycleReady        RunnerLifecycle = "ready"
	RunnerLifecycleFailed       RunnerLifecycle = "failed"
	RunnerLifecycleDeleting     RunnerLifecycle = "deleting"
)

type RunnerPage struct {
	Items      []Runner `json:"items"`
	NextCursor string   `json:"next_cursor,omitempty"`
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
	Endpoint      string                         `json:"endpoint"`
	Bucket        string                         `json:"bucket"`
	Prefix        string                         `json:"prefix,omitempty"`
	Region        string                         `json:"region"`
	PathStyle     bool                           `json:"path_style"`
	Credentials   map[string]ConnectorCredential `json:"credentials"`
}

type ConnectorCreateRequest struct {
	Name        string                              `json:"name"`
	Kind        string                              `json:"kind"`
	Endpoint    string                              `json:"endpoint"`
	Bucket      string                              `json:"bucket"`
	Prefix      string                              `json:"prefix,omitempty"`
	Region      string                              `json:"region"`
	PathStyle   *bool                               `json:"path_style" nullable:"false"`
	Credentials map[string]ConnectorCredentialInput `json:"credentials"`
}

type ReleaseState string

const (
	ReleasePending          ReleaseState = "pending"
	ReleaseRunning          ReleaseState = "running"
	ReleaseCandidateHealthy ReleaseState = "candidate_healthy"
	ReleaseSwitching        ReleaseState = "switching"
	ReleaseServing          ReleaseState = "serving"
	ReleasePostHooks        ReleaseState = "post_hooks"
	ReleaseCompensating     ReleaseState = "compensating"
	ReleaseCompleted        ReleaseState = "completed"
	ReleaseFailed           ReleaseState = "failed"
	ReleaseTimedOut         ReleaseState = "timed_out"
	ReleaseAborted          ReleaseState = "aborted"
	ReleaseRecoveryRequired ReleaseState = "recovery_required"
	ReleaseRecovering       ReleaseState = "recovering"
)

type ReleaseSummary struct {
	ID                 string       `json:"id"`
	EnvironmentID      string       `json:"environment_id"`
	ServiceID          string       `json:"service_id"`
	OperationID        string       `json:"operation_id"`
	OperationKind      string       `json:"operation_kind"`
	GroupOperationID   string       `json:"group_operation_id,omitempty"`
	GroupMemberOrdinal uint32       `json:"group_member_ordinal,omitempty"`
	Image              string       `json:"image"`
	Tag                string       `json:"tag"`
	Digest             string       `json:"digest,omitempty"`
	Strategy           string       `json:"strategy"`
	Slot               string       `json:"slot,omitempty"`
	OnFailure          OnFailure    `json:"on_failure"`
	State              ReleaseState `json:"state"`
	CreatedAt          string       `json:"created_at"`
	CompletedAt        *string      `json:"completed_at,omitempty"`
	Serving            bool         `json:"serving"`
	CurrentSuccessful  bool         `json:"current_successful"`
}

type ReleaseAttempt struct {
	ID        string `json:"id"`
	TaskID    string `json:"task_id"`
	RetryOf   string `json:"retry_of,omitempty"`
	StartedAt string `json:"started_at"`
}

type ReleaseRollbackMaterial struct {
	Status    string  `json:"status"`
	Revision  uint64  `json:"revision"`
	ExpiredAt *string `json:"expired_at,omitempty"`
}

type ReleaseDetail struct {
	ReleaseSummary
	RollbackSourceReleaseID  string                   `json:"rollback_source_release_id,omitempty"`
	PriorServingReleaseID    string                   `json:"prior_serving_release_id,omitempty"`
	PriorSuccessfulReleaseID string                   `json:"prior_successful_release_id,omitempty"`
	RenderInputDigest        string                   `json:"render_input_digest"`
	OriginatingTaskID        string                   `json:"originating_task_id"`
	Attempts                 []ReleaseAttempt         `json:"attempts"`
	RollbackMaterial         *ReleaseRollbackMaterial `json:"rollback_material,omitempty"`
}

type ReleaseTaskAccepted struct {
	TaskID      string `json:"task_id"`
	OperationID string `json:"operation_id"`
	ReleaseID   string `json:"release_id"`
}

// ReleaseGroup coordinates multiple services under one task lock and
// one failure policy — never inferred, always explicit. See
// blueprint.md, "x-gp-release-group".
type ReleaseGroup struct {
	ID            string    `json:"id"`
	EnvironmentID string    `json:"environment_id"`
	Name          string    `json:"name"`
	ServiceIDs    []string  `json:"service_ids"`
	Order         []string  `json:"order"`
	Tag           string    `json:"tag,omitempty"`
	OnFailure     OnFailure `json:"on_failure"`
}

type ReleaseGroupAddRequest struct {
	EnvironmentID string    `json:"environment_id"`
	Name          string    `json:"name"`
	ServiceIDs    []string  `json:"service_ids"`
	Order         []string  `json:"order,omitempty"`
	Tag           string    `json:"tag,omitempty"`
	OnFailure     OnFailure `json:"on_failure,omitempty"`
}

type ReleaseGroupEditRequest struct {
	Name       *string                `json:"name,omitempty"`
	ServiceIDs *[]string              `json:"service_ids,omitempty"`
	Order      *[]string              `json:"order,omitempty"`
	Tag        OptionalNullableString `json:"tag,omitempty"`
	OnFailure  *OnFailure             `json:"on_failure,omitempty"`
}

type ReleaseGroupMutationAccepted struct {
	TaskID         string `json:"task_id"`
	ReleaseGroupID string `json:"release_group_id"`
}

type ReleaseGroupMemberRelease struct {
	ServiceID string `json:"service_id"`
	ReleaseID string `json:"release_id"`
}

type ReleaseGroupTaskAccepted struct {
	TaskID                  string                      `json:"task_id"`
	ReleaseGroupOperationID string                      `json:"release_group_operation_id"`
	Releases                []ReleaseGroupMemberRelease `json:"releases"`
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
	Tag string `json:"tag,omitempty"`
}

type AttachRequest struct {
	ServiceID        string           `json:"service_id"`
	BackingServiceID string           `json:"backing_service_id"`
	Name             string           `json:"name,omitempty"` // omitted => Controller-suggested, made unique within the environment
	Credential       AttachCredential `json:"credential"`
	GrantAttachIDs   []string         `json:"grant_attach_ids,omitempty"`
}

type AttachRenameRequest struct {
	Name string `json:"name"`
}

type RestoreRequest struct {
	SourceID        string `json:"source_id"`
	RecoveryPointID string `json:"recovery_point_id,omitempty"` // empty = latest
	AgeIdentity     string `json:"age_identity,omitempty"`
}

type ComponentEnableRequest struct {
	Config *ComponentConfigMutationInput `json:"config,omitempty"`
}

type RunnerCreateRequest struct {
	Slug              string   `json:"slug"`
	TenantID          string   `json:"tenant_id,omitempty"`
	ProjectID         string   `json:"project_id,omitempty"`
	GitHubURL         string   `json:"github_url"`
	Labels            []string `json:"labels,omitempty"`
	RegistrationToken string   `json:"registration_token"` // discarded by the Controller after registration
}

type RunnerRetryRequest struct {
	RegistrationToken string `json:"registration_token"` // discarded by the Controller after registration
}

type RunnerEditRequest struct {
	Slug string `json:"slug"`
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

type TaskWorkspaceType string

const (
	TaskWorkspacePlatform TaskWorkspaceType = "platform"
	TaskWorkspaceTenant   TaskWorkspaceType = "tenant"
)

type TaskActor string

const (
	TaskActorOperator TaskActor = "operator"
	TaskActorSystem   TaskActor = "system"
)

type Task struct {
	ID            string            `json:"id"`
	OperationID   string            `json:"operation_id"`
	RetryOf       string            `json:"retry_of,omitempty"`
	PlanHash      string            `json:"plan_hash,omitempty"`
	Type          string            `json:"type"                     enum:"deploy,rollback,backup,backup_prune,restore,attach,detach,run,script,provision,create,update,remove,start,stop,destroy,rotate"`
	Target        string            `json:"target"`
	Status        TaskStatus        `json:"status"`
	WorkspaceType TaskWorkspaceType `json:"workspace_type"           enum:"platform,tenant"`
	TenantID      string            `json:"tenant_id,omitempty"`
	ProjectID     string            `json:"project_id,omitempty"`
	EnvironmentID string            `json:"environment_id,omitempty"`
	Actor         TaskActor         `json:"actor"                    enum:"operator,system"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	StartedAt     *time.Time        `json:"started_at"`
	FinishedAt    *time.Time        `json:"finished_at"`
	Steps         []TaskStep        `json:"steps,omitempty"`
}

type TaskStepKind string

const (
	TaskStepOperation TaskStepKind = "operation"
	TaskStepScript    TaskStepKind = "script"
)

type TaskStep struct {
	Name       string       `json:"name"`
	Status     TaskStatus   `json:"status"`
	Kind       TaskStepKind `json:"kind" enum:"operation,script"`
	ScriptID   string       `json:"script_id,omitempty"`
	ScriptSlug string       `json:"script_slug,omitempty"`
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
	Revision   int64  `json:"revision,omitempty"`
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
