// Package core is the domain model + the Blueprint desired-state schema.
// Pure: no infra imports (no etcd, docker, systemd, age), so it tests in
// isolation. This is the type-level expression of mvp.md's "Model" and
// blueprint.md's "typed Controller desired state" layer — the Controller
// parses an authored Blueprint (Compose body + x-gp-* extensions, see
// envelope.go) INTO these types; they are not the authored YAML shape
// itself. Keep this file, envelope.go, and blueprint.md in sync; this
// file's comments cite the doc section that motivates each field.
package core

import "time"

// ProjectKind distinguishes a tenant project (an application) from a
// backing project (shared infra: datastore/cache/queue). Both follow the
// SAME hierarchy — project -> environment(s) -> service(s) — so the
// Controller runs identical logic for either; only adapter fields differ
// on a backing project's single service.
type ProjectKind string

const (
	ProjectKindTenant  ProjectKind = "tenant"
	ProjectKindBacking ProjectKind = "backing"
)

// Tenant is a strict isolation boundary (mvp.md, "Model").
type Tenant struct {
	ID          string `yaml:"id"          json:"id"`   // tnt_<ulid>
	Slug        string `yaml:"slug"        json:"slug"` // globally unique, renamable
	Name        string `yaml:"name"        json:"name"`
	Description string `yaml:"description" json:"description"` // optional operator-authored summary
}

// Project is either a tenant application or a backing service.
type Project struct {
	ID          string      `yaml:"id"                  json:"id"`                  // prj_<ulid>
	TenantID    string      `yaml:"tenant_id,omitempty" json:"tenant_id,omitempty"` // empty for backing
	Slug        string      `yaml:"slug"                json:"slug"`                // unique within tenant
	Name        string      `yaml:"name"                json:"name"`
	Description string      `yaml:"description"         json:"description"` // optional create-time summary
	Kind        ProjectKind `yaml:"kind"                json:"kind"`
}

// Environment is one deployable instance of a Project. Its id is static
// and generated at creation; the label (the envelope's `metadata.
// environment`, or the Blueprint document's path segment) is ONLY a
// display/placement convenience — every environment-scoped reference
// keys off ID, never the label, so renaming never orphans data or moves
// the Compose project identity. See blueprint.md, "Identity and rename
// rules".
type Environment struct {
	ID          string `yaml:"id"           json:"id"` // env_<ulid>, generated once, never regenerated
	ProjectID   string `yaml:"project_id"   json:"project_id"`
	Name        string `yaml:"name"         json:"name"` // sole human/URL label; unique within project, freely renamable
	NetworkPool string `yaml:"network_pool" json:"network_pool"`

	VolumeDir string `yaml:"volume_dir" json:"volume_dir"` // ADR 0025: generated from configured root and stable owner ids

	Zones      map[string]Zone    `yaml:"zones,omitempty"      json:"zones,omitempty"` // Groundplane's "zone" IS a Compose network — see blueprint.md
	Services   map[string]Service `yaml:"services,omitempty"   json:"services,omitempty"`
	Attaches   []Attach           `yaml:"attaches,omitempty"   json:"attaches,omitempty"`
	Routes     []Route            `yaml:"routes,omitempty"     json:"routes,omitempty"`
	Components []Component        `yaml:"components,omitempty" json:"components,omitempty"`
	Volumes    map[string]Volume  `yaml:"volumes,omitempty"    json:"volumes,omitempty"`
	Entries    []EnvEntry         `yaml:"entries,omitempty"    json:"entries,omitempty"`
	Scripts    map[string]Script  `yaml:"scripts,omitempty"    json:"scripts,omitempty"`
	// Backup is present only for tenant environments. Backing environments
	// never own a consumer backup policy or age key.
	Backup *BackupPolicy `yaml:"backup,omitempty" json:"backup,omitempty"`

	CreatedAt time.Time `yaml:"created_at" json:"created_at"`
}

// Router is a READ-ONLY projection assembled from the environment's
// ingress-kind Components (kind "caddy", "cloudflare-tunnel",
// …) — it is never authored or PUT directly. See api-cli.md: "router |
// GET /environments/{id}/router read-only projection grouping ingress
// components", and blueprint.md: "The Router page is a UI grouping for
// ingress components; it is not a separate runtime model."
type Router struct {
	Caddy  *ComponentProjection `yaml:"caddy,omitempty"  json:"caddy,omitempty"`
	Tunnel *ComponentProjection `yaml:"tunnel,omitempty" json:"tunnel,omitempty"`
}

// ComponentProjection is Router's per-component summary view — the full Component
// record (config, generated services, health) lives in Component itself;
// this is just what the read-only projection surfaces.
type ComponentProjection struct {
	ComponentID string `yaml:"component_id"          json:"component_id"`
	Enabled     bool   `yaml:"enabled"               json:"enabled"`
	PinnedIPv4  string `yaml:"pinned_ipv4,omitempty" json:"pinned_ipv4,omitempty"` // Caddy only; durable while enabled, released on disable
}

// Zone is a named internal docker network grouping Services — Groundplane's
// domain term for a Compose `network`; there is no second zone topology
// grammar (blueprint.md). A Service on two zones is a bridge.
type Zone struct {
	ID     string `yaml:"id"               json:"id"` // net_<ulid> — see blueprint.md's x-gp-network example
	Name   string `yaml:"name"             json:"name"`
	Subnet string `yaml:"subnet,omitempty" json:"subnet,omitempty"`
	// Internal marks the zone as having no route to the host's default
	// gateway (e.g. prod_identity_private_net's "no host egress").
	Internal bool `yaml:"internal,omitempty" json:"internal,omitempty"`
	// OwnerKind and OwnerID are Controller-derived stable ownership. A
	// tenant Environment owns its ordinary Zones; a backing Project owns
	// its adapter-created Zone. Consumers only join it as external.
	OwnerKind ZoneOwnerKind `yaml:"owner_kind" json:"owner_kind"`
	OwnerID   string        `yaml:"owner_id"   json:"owner_id"`
}

type ZoneOwnerKind string

const (
	ZoneOwnerEnvironment    ZoneOwnerKind = "environment"
	ZoneOwnerBackingProject ZoneOwnerKind = "backing_project"
)

// Healthcheck matches exactly one of the three declared kinds. The
// renderer compiles this to native Compose `healthcheck.test` (see
// blueprint.md's Compose base document example: `test: [CMD, curl, -f,
// http://localhost/up]`).
type Healthcheck struct {
	HTTP  string `yaml:"http,omitempty"  json:"http,omitempty"`  // path, e.g. "/up"
	TCP   string `yaml:"tcp,omitempty"   json:"tcp,omitempty"`   // host:port
	Pgrep string `yaml:"pgrep,omitempty" json:"pgrep,omitempty"` // process name/cmd

	Interval    string `yaml:"interval,omitempty"     json:"interval,omitempty"`
	Timeout     string `yaml:"timeout,omitempty"      json:"timeout,omitempty"`
	StartPeriod string `yaml:"start_period,omitempty" json:"start_period,omitempty"`
	Retries     int    `yaml:"retries,omitempty"      json:"retries,omitempty"`
}

type Resources struct {
	Mem  string  `yaml:"mem,omitempty"  json:"mem,omitempty"` // e.g. "512m" — compiles to Compose deploy.resources.limits
	CPUs float64 `yaml:"cpus,omitempty" json:"cpus,omitempty"`
}

// Mount is EITHER a volume mount or a read-only file-secret mount —
// exactly one of Volume or File is set. Compiles to native Compose
// `volumes` or the `configs`/`secrets` grant pair (blueprint.md,
// "Groundplane entries compile to native Compose resources by type").
type Mount struct {
	Volume string `yaml:"volume,omitempty" json:"volume,omitempty"`
	File   string `yaml:"file,omitempty"   json:"file,omitempty"` // references an EnvEntry with Kind=EntryKindFile
	Mount  string `yaml:"mount"            json:"mount"`          // container path
	RO     bool   `yaml:"ro,omitempty"     json:"ro,omitempty"`
}

// Strategy is a deploy strategy. "rolling" is declared-deferred (rejected
// with errs.CodeStrategyNotImplemented). See mvp.md, "Release strategy
// rules (locked in)".
type Strategy string

const (
	StrategyBlueGreen Strategy = "blue-green"
	StrategyRecreate  Strategy = "recreate"
	StrategyRolling   Strategy = "rolling" // declared, not implemented
)

// OnFailure decides what a failed post-switch deploy step does to
// traffic: switch the alias/router target back to the previous healthy
// slot (the default), or leave the new (unhealthy) slot active for an
// explicit operator rollback. See blueprint.md, "x-gp-release" and "The
// blue-green deploy procedure".
type OnFailure string

const (
	OnFailureSwitchBack  OnFailure = "switch_back"
	OnFailureLeaveActive OnFailure = "leave_active"
)

// WithDefault resolves an omitted failure policy to the contract default.
func (o OnFailure) WithDefault() OnFailure {
	if o == "" {
		return OnFailureSwitchBack
	}
	return o
}

// Service is one container or shared runtime. Name is the stable,
// human-referenced identifier (the Compose service key, DNS name); ID
// disambiguates provisioned values on backing services (see Attach) and
// never appears in names or env var keys. See mvp.md, "Service", and
// blueprint.md's "x-gp-release" / "x-gp-resource".
type Service struct {
	ID   string `yaml:"id"   json:"id"`   // svc_<ulid>
	Name string `yaml:"name" json:"name"` // unique within the environment; the Compose service key for `recreate` strategy, or the logical-service prefix for blue-green slots (`<name>__blue`, `<name>__green`)

	Image     string    `yaml:"image"                json:"image"`
	Zones     []string  `yaml:"zones,omitempty"      json:"zones,omitempty"`
	Strategy  Strategy  `yaml:"strategy,omitempty"   json:"strategy,omitempty"`   // declared default; overridden per-deploy
	OnFailure OnFailure `yaml:"on_failure,omitempty" json:"on_failure,omitempty"` // declared default (switch_back); overridden per-deploy

	Healthcheck Healthcheck `yaml:"healthcheck,omitempty" json:"healthcheck,omitempty"`
	Resources   Resources   `yaml:"resources,omitempty"   json:"resources,omitempty"`
	Command     []string    `yaml:"command,omitempty"     json:"command,omitempty"`

	Mounts      []Mount    `yaml:"mounts,omitempty"      json:"mounts,omitempty"`
	Environment []EnvEntry `yaml:"environment,omitempty" json:"environment,omitempty"` // service-scoped entries, wins on key conflict with the environment's all-services file

	Aliases   map[string][]string          `yaml:"aliases,omitempty"    json:"aliases,omitempty"` // per-zone; blue-green also gets a per-slot alias, see blueprint.md
	DependsOn map[string]ServiceDependency `yaml:"depends_on,omitempty" json:"depends_on,omitempty"`

	Expose  []string `yaml:"expose,omitempty"  json:"expose,omitempty"` // internal-only, no host publishing (ports is rejected for tenant services — see blueprint.md's MVP Compose surface table)
	Restart string   `yaml:"restart,omitempty" json:"restart,omitempty"`

	Logging struct {
		MaxSize string `yaml:"max_size,omitempty" json:"max_size,omitempty"`
		MaxFile int    `yaml:"max_file,omitempty" json:"max_file,omitempty"`
	} `yaml:"logging,omitempty" json:"logging,omitempty"`

	Replicas int `yaml:"replicas,omitempty" json:"replicas,omitempty"` // native Compose deploy.replicas

	// For a backing project's single service only — see x-gp-adapter:
	Adapter     string `yaml:"adapter,omitempty"      json:"adapter,omitempty"`      // e.g. "postgres:16" — looks up internal/adapters
	FactsPrefix string `yaml:"facts_prefix,omitempty" json:"facts_prefix,omitempty"` // optional override; adapter supplies a default
	Label       string `yaml:"label,omitempty"        json:"label,omitempty"`        // display only
}

// ServiceDependency is x-gp-depends_on's per-dependency shape:
// Compose health/start condition plus which lifecycle phases it applies
// to. See blueprint.md, "x-gp-depends_on".
type ServiceDependency struct {
	Condition string   `yaml:"condition"        json:"condition"`        // Compose condition, e.g. "service_completed_successfully", "service_healthy"
	Phases    []string `yaml:"phases,omitempty" json:"phases,omitempty"` // "start" | "deploy" | "rollback" | "always"; empty = normal Compose startup only
}

// EntryKind and EntrySourceKind implement the unified environment-entry
// model — one CLI noun (`entry`), one API shape, covering variables and
// files, plain and secret, including fact-derived values. Replaces the
// old separate EnvEntry/FileEntry split. See blueprint.md, "x-gp-entry",
// and api-cli.md section 4's discriminated entry-request example.
type EntryKind string

const (
	EntryKindEnv  EntryKind = "env"
	EntryKindFile EntryKind = "file"
)

// EntrySourceKind is exactly one of the three, mutually exclusive
// (api-cli.md: "Literal, secret_ref, and fact sources are mutually
// exclusive"):
type EntrySourceKind string

const (
	SourceLiteral   EntrySourceKind = "literal"
	SourceSecretRef EntrySourceKind = "secret_ref"
	SourceFact      EntrySourceKind = "fact"
)

// EntrySource is a tagged union over SourceKind — exactly one of
// Literal/SecretRef/Fact is populated, matching SourceKind.
type EntrySource struct {
	Kind      EntrySourceKind `yaml:"kind"                 json:"kind"`
	Literal   string          `yaml:"literal,omitempty"    json:"literal,omitempty"`
	SecretRef string          `yaml:"secret_ref,omitempty" json:"secret_ref,omitempty"` // secret-store id/ref
	Fact      *FactRef        `yaml:"fact,omitempty"       json:"fact,omitempty"`
}

// FactRef is a LIVE reference: the Controller resolves {attach, key}
// again on every render/materialization, so an adapter repair or
// rotation updates every declared destination without rewriting the
// Blueprint. See blueprint.md, "x-gp-entry": "A fact source is live".
type FactRef struct {
	Attach string `yaml:"attach"          json:"attach"`          // the attach name (or id)
	Grant  string `yaml:"grant,omitempty" json:"grant,omitempty"` // optional granted attach name (or id)
	Key    string `yaml:"key"             json:"key"`             // e.g. "pg16_URL"
}

// EnvEntry is one destination — reusing a fact under a different env
// key, file path, or exposure scope uses multiple explicit entries;
// facts are never auto-injected because an attach exists. See
// blueprint.md, "x-gp-entry": "One x-gp-entry is one destination."
type EnvEntry struct {
	ID     string      `yaml:"id"             json:"id"` // ev_<ulid>
	Kind   EntryKind   `yaml:"kind"           json:"kind"`
	Key    string      `yaml:"key,omitempty"  json:"key,omitempty"`  // Kind=env: the env var name
	Path   string      `yaml:"path,omitempty" json:"path,omitempty"` // Kind=file: relative to the environment's volume folder — never escapes it
	UID    *uint32     `yaml:"uid,omitempty"  json:"uid,omitempty"`  // Kind=file: explicit numeric workload owner
	GID    *uint32     `yaml:"gid,omitempty"  json:"gid,omitempty"`  // Kind=file: explicit numeric workload group
	Source EntrySource `yaml:"source"         json:"source"`
	// Exposure is a list of service names, OR the single literal
	// sentinel "all" meaning every service in the environment
	// (blueprint.md: "exposure: [all] means the entry is written to the
	// canonical secrets/.env.<environment-id> file"). Multiple explicit
	// service names create per-service generated files.
	Exposure []string `yaml:"exposure" json:"exposure"`
	Secret   bool     `yaml:"secret"   json:"secret"` // true -> Source must not be SourceLiteral with a plaintext value visible in desired state
}

// ExposesAll reports whether this entry's Exposure is the "all services"
// sentinel, per blueprint.md's `exposure: [all]` convention.
func (e EnvEntry) ExposesAll() bool {
	return len(e.Exposure) == 1 && e.Exposure[0] == "all"
}

// AttachStatus is the attach lifecycle. See blueprint.md, "x-gp-attachments":
// "pending -> provisioning -> ready, with terminal failed and
// detaching -> detached paths."
type AttachStatus string

const (
	AttachPending      AttachStatus = "pending"
	AttachProvisioning AttachStatus = "provisioning"
	AttachReady        AttachStatus = "ready"
	AttachFailed       AttachStatus = "failed"
	AttachDetaching    AttachStatus = "detaching"
	AttachDetached     AttachStatus = "detached"
)

// Attach is how a Service consumes a backing project. Never
// deduplicated: a service may attach the same backing project many
// times, each attach provisioning its own database + role. Name is a
// MUTABLE LABEL (auto-suggested from tenant-project-environment-service
// if omitted, unique within the environment) — renaming it is a
// separate explicit operation and never recalculated by unrelated
// renames. See mvp.md, "Attach", and blueprint.md, "x-gp-attachments".
type Attach struct {
	ID                   string       `yaml:"id"                               json:"id"` // att_<ulid> — its random tail's first 6 chars disambiguate provisioned names
	Name                 string       `yaml:"name"                             json:"name"`
	BackingProjectID     string       `yaml:"backing_project_id"               json:"backing_project_id"`
	BackingEnvironmentID string       `yaml:"backing_environment_id,omitempty" json:"backing_environment_id,omitempty"` // always "main" for a backing project, but resolved to an id like any environment
	BackingServiceID     string       `yaml:"backing_service_id,omitempty"     json:"backing_service_id,omitempty"`
	Services             []string     `yaml:"services"                         json:"services"` // attaching service name(s) sharing this attach
	Grants               []Grant      `yaml:"grants,omitempty"                 json:"grants,omitempty"`
	Status               AttachStatus `yaml:"status"                           json:"status"`
	CreatedAt            time.Time    `yaml:"created_at"                       json:"created_at"`
}

// Grant lets an attach's role also access another attach's database.
// References the OTHER attach by id/name — never a mutable database
// string (blueprint.md: "Grants reference attach ids/names, never
// mutable database strings").
type Grant struct {
	AttachID string `yaml:"attach_id" json:"attach_id"`
}

// ComponentKind namespaces component kinds by category: "ingress.*" (Caddy),
// "edge.*" (Cloudflare Tunnel), and future kinds (e.g. "ingress.traefik")
// register the same way. See blueprint.md, "x-gp-components": "Adding
// Traefik later is an component package and registration, not a new router
// architecture."
type ComponentKind string

const (
	ComponentKindIngressCaddy   ComponentKind = "caddy"
	ComponentKindEdgeCloudflare ComponentKind = "cloudflare-tunnel"
	ComponentKindCoreDNS        ComponentKind = "coredns"
	ComponentKindController     ComponentKind = "controller"
	ComponentKindAgent          ComponentKind = "agent"
)

// ComponentOwner discriminates the two valid component ownership scopes.
type ComponentOwner string

const (
	ComponentOwnerEnvironment ComponentOwner = "environment"
	ComponentOwnerPlatform    ComponentOwner = "platform"
)

// Component is the generic component record shared by environment-owned and
// platform-owned kinds. OwnerID is the environment id for environment owners
// and empty for the singleton platform owner.
type Component struct {
	ID                string         `yaml:"id"                           json:"id"` // cmp_<ulid>
	Owner             ComponentOwner `yaml:"owner"                        json:"owner"`
	OwnerID           string         `yaml:"owner_id,omitempty"           json:"owner_id,omitempty"`
	Kind              ComponentKind  `yaml:"kind"                         json:"kind"`
	Enabled           bool           `yaml:"enabled"                      json:"enabled"`
	Config            map[string]any `yaml:"config,omitempty"             json:"config,omitempty"`             // typed per kind at the registry level; kept generic here (see internal/adapters-style component registry, TODO)
	GeneratedServices []string       `yaml:"generated_services,omitempty" json:"generated_services,omitempty"` // stable Service ids allocated for this component
	PinnedIPv4        string         `yaml:"pinned_ipv4,omitempty"        json:"pinned_ipv4,omitempty"`        // Caddy only; derived durable state, never authored
	Healthy           bool           `yaml:"healthy"                      json:"healthy"`
}

// Route is a domain or path routed to a Service. Public routes require
// an enabled ingress component to be served.
type Route struct {
	ID              string `yaml:"id"                        json:"id"` // rte_<ulid>
	Host            string `yaml:"host,omitempty"            json:"host,omitempty"`
	Path            string `yaml:"path"                       json:"path"`
	TargetServiceID string `yaml:"target_service_id"          json:"target_service_id"`
	TargetPort      uint16 `yaml:"target_port"                json:"target_port"`
	Exposure        string `yaml:"exposure"                   json:"exposure"` // "public" | "internal"
}

// Volume is persistent storage owned by an Environment, named and
// referenced by service mounts; the bind mount is
// <volume_dir>/<name> and cannot traverse outside it.
type Volume struct {
	ID   string `yaml:"id"   json:"id"` // vol_<ulid>
	Name string `yaml:"name" json:"name"`
}

// Script is a first-class per-environment concept, executed against a
// service as a task; When doubles as a deploy/rollback hook. See mvp.md,
// "Scripts".
type ScriptHook string

const (
	ScriptManual       ScriptHook = "manual"
	ScriptPreDeploy    ScriptHook = "pre-deploy"
	ScriptPostDeploy   ScriptHook = "post-deploy"
	ScriptPreRollback  ScriptHook = "pre-rollback"
	ScriptPostRollback ScriptHook = "post-rollback"
	ScriptOnFailure    ScriptHook = "on-failure"
)

type Script struct {
	ID          string     `yaml:"id"      json:"id"` // scr_<ulid>
	Name        string     `yaml:"name"    json:"name"`
	ServiceName string     `yaml:"service" json:"service"`
	Body        string     `yaml:"script"  json:"script"` // one line or many
	When        ScriptHook `yaml:"when"    json:"when"`
}

// BackupSource selects what a backup run backs up. The canonical source
// kinds are attach, volume, and config; an attach resolves its adapter from
// the referenced backing service. See mvp.md, "Backup", and blueprint.md,
// "x-gp-backup".
type BackupSourceKind string

const (
	BackupSourceAttach BackupSourceKind = "attach"
	BackupSourceVolume BackupSourceKind = "volume"
	BackupSourceConfig BackupSourceKind = "config" // this environment's env entries only — never backing environments
)

type BackupSource struct {
	ID   string           `yaml:"id"            json:"id"` // spt_<ulid>
	Kind BackupSourceKind `yaml:"kind"          json:"kind"`
	Ref  string           `yaml:"ref,omitempty" json:"ref,omitempty"` // attach id/name or volume name; unused for config
}

// BackupPolicy is per-environment, toggleable off, with fully selectable
// sources under one schedule/retention/encryption. Removing a source
// from the active policy stops new points but existing recovery points
// remain addressable until explicit deletion or retention pruning (see
// blueprint.md, "x-gp-backup").
type BackupPolicy struct {
	Enabled     bool           `yaml:"enabled"                json:"enabled"`
	Frequency   string         `yaml:"frequency,omitempty"    json:"frequency,omitempty"` // systemd calendar expr
	Keep        int            `yaml:"keep,omitempty"         json:"keep,omitempty"`
	Encryption  string         `yaml:"encryption,omitempty"   json:"encryption,omitempty"` // "age" | "none"
	ConnectorID string         `yaml:"connector_id,omitempty" json:"connector_id,omitempty"`
	Sources     []BackupSource `yaml:"sources,omitempty"      json:"sources,omitempty"`

	// AgeRecipient is the public half of the CURRENT per-environment
	// keypair, generated LAZILY on first backup enable. Safe in desired
	// state. KeyEra increments on each rotation; a recovery point
	// records the era it was encrypted under (see RecoveryPoint), so
	// restore knows which identity it needs.
	AgeRecipient string `yaml:"age_recipient,omitempty" json:"age_recipient,omitempty"`
	KeyEra       int    `yaml:"key_era,omitempty"       json:"key_era,omitempty"`
}

// RecoveryPointStatus tracks the commit sequence: a recovery point is
// visible only after dump/archive, encryption, upload, AND remote
// verification all succeed. See blueprint.md, "x-gp-backup".
type RecoveryPointStatus string

const (
	RecoveryPointVerified RecoveryPointStatus = "verified"
)

// RecoveryPoint is one backup run's immutable output for one source.
type RecoveryPoint struct {
	ID         string              `yaml:"id"                json:"id"` // rp_<ulid>
	SourceID   string              `yaml:"source_id"         json:"source_id"`
	SourceKind BackupSourceKind    `yaml:"source_kind"       json:"source_kind"`
	CreatedAt  time.Time           `yaml:"created_at"        json:"created_at"`
	Locator    string              `yaml:"locator"           json:"locator"` // e.g. r2://<connector>/<environment-id>/<source-id>/<point-id>
	Size       int64               `yaml:"size,omitempty"    json:"size,omitempty"`
	Encrypted  bool                `yaml:"encrypted"         json:"encrypted"`
	KeyEra     int                 `yaml:"key_era,omitempty" json:"key_era,omitempty"` // which age keypair generation encrypted this point
	Status     RecoveryPointStatus `yaml:"status"            json:"status"`
}

// Runner is a GitHub Actions self-hosted runner registration, scoped to
// a tenant (org) OR a project (repo) — exactly one of TenantID/ProjectID
// is the owning scope; ProjectID set implies repo-scoped.
type Runner struct {
	ID        string   `yaml:"id"                   json:"id"` // run_<ulid>
	TenantID  string   `yaml:"tenant_id"            json:"tenant_id"`
	ProjectID string   `yaml:"project_id,omitempty" json:"project_id,omitempty"` // set for repo-scoped
	Labels    []string `yaml:"labels,omitempty"     json:"labels,omitempty"`
	Online    bool     `yaml:"online"               json:"online"`
}

// Secret is project-scoped (with platform fallback) — see mvp.md,
// "Secrets are project-scoped resources with platform fallback."
// Connectors, by contrast, are environment-scoped only — see Connector
// below and blueprint.md: "There is no project or platform connector and
// no fallback."
type SecretKind string

const (
	SecretKindEnvVar SecretKind = "env_var"
	SecretKindFile   SecretKind = "file"
)

type SecretScope string

const (
	SecretScopeProject  SecretScope = "project"
	SecretScopePlatform SecretScope = "platform"
)

type Secret struct {
	ID        string      `yaml:"id"                   json:"id"` // sec_<ulid>
	Scope     SecretScope `yaml:"scope"                json:"scope"`
	ProjectID string      `yaml:"project_id,omitempty" json:"project_id,omitempty"`
	Key       string      `yaml:"key"                  json:"key"`
	Kind      SecretKind  `yaml:"kind"                 json:"kind"`
	Ref       string      `yaml:"ref"                  json:"ref"` // generated env-file name or volume-relative file path
	UpdatedAt time.Time   `yaml:"updated_at"           json:"updated_at"`
}

// Connector is a backup destination + credentials owned by exactly one
// environment. There are no project or platform connectors.
type Connector struct {
	ID            string            `yaml:"id"                    json:"id"` // con_<ulid>
	EnvironmentID string            `yaml:"environment_id"        json:"environment_id"`
	Kind          string            `yaml:"kind"                  json:"kind"` // "s3-compatible", later s3/minio/b2
	PathStyle     bool              `yaml:"path_style"            json:"path_style"`
	Credentials   map[string]string `yaml:"credentials,omitempty" json:"credentials,omitempty"` // secret-store ref or opaque encrypted-value record reference
}

// DeployStatus is the release ledger's task-driven state machine. See
// blueprint.md, "x-gp-release":
//
//	pending -> running -> candidate_healthy -> switching -> active
//	                          |                    |
//	                          v                    v
//	                       failed       failed -> switch_back or leave_active
//
// superseded/aborted/timed_out are also terminal outcomes. The
// Controller writes `active` only after the Agent acknowledges the
// health gate and slot switch; a failed or timed-out task never becomes
// the current release.
type DeployStatus string

const (
	DeployPending          DeployStatus = "pending"
	DeployRunning          DeployStatus = "running"
	DeployCandidateHealthy DeployStatus = "candidate_healthy"
	DeploySwitching        DeployStatus = "switching"
	DeployActive           DeployStatus = "active"
	DeployFailed           DeployStatus = "failed"
	DeploySuperseded       DeployStatus = "superseded"
	DeployAborted          DeployStatus = "aborted"
	DeployTimedOut         DeployStatus = "timed_out"
)

// ReleaseRecord is one per-service deploy/rollback record — the release
// ledger is per logical service, never one environment-wide field.
// Rollback's target is the most recent record whose Tag differs from
// the current one. See blueprint.md, "x-gp-release" for the full field
// set (this mirrors it exactly).
type ReleaseRecord struct {
	DeploymentID string       `yaml:"deployment_id"          json:"deployment_id"` // dep_<ulid>
	ServiceID    string       `yaml:"service_id"             json:"service_id"`
	Image        string       `yaml:"image"                  json:"image"`
	Tag          string       `yaml:"tag"                    json:"tag"`
	Digest       string       `yaml:"digest,omitempty"       json:"digest,omitempty"`
	Strategy     Strategy     `yaml:"strategy"               json:"strategy"`
	Slot         string       `yaml:"slot,omitempty"         json:"slot,omitempty"` // "blue" | "green"; empty for recreate
	OnFailure    OnFailure    `yaml:"on_failure"             json:"on_failure"`
	TaskID       string       `yaml:"task_id"                json:"task_id"`
	Status       DeployStatus `yaml:"status"                 json:"status"`
	StartedAt    time.Time    `yaml:"started_at"             json:"started_at"`
	CompletedAt  *time.Time   `yaml:"completed_at,omitempty" json:"completed_at,omitempty"`
}

// ReleaseGroup makes multi-service coordination EXPLICIT — never
// inferred from shared image names or service names. One task lock and
// one group-level failure policy; each member still keeps its own
// release records and observed state. See blueprint.md,
// "x-gp-release-groups".
type ReleaseGroup struct {
	ID        string    `yaml:"id"                   json:"id"` // rg_<ulid>
	Name      string    `yaml:"name"                 json:"name"`
	Services  []string  `yaml:"services"             json:"services"`
	Order     []string  `yaml:"order,omitempty"      json:"order,omitempty"` // deploy order within the group; defaults to Services order
	Tag       string    `yaml:"tag,omitempty"        json:"tag,omitempty"`
	OnFailure OnFailure `yaml:"on_failure,omitempty" json:"on_failure,omitempty"`
}
