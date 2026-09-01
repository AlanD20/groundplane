import type { EnvironmentNetworkCapacity } from './environment-types'

// Groundplane desired-state model (prototype). Mirrors the Blueprint contract.
export type HealthState = 'healthy' | 'degraded' | 'failed' | 'stopped' | 'pending' | 'unknown'
export type ServiceStrategy = 'blue-green' | 'recreate' | 'rolling'
export type ServiceRuntimeIntent = 'running' | 'stopped' | 'absent'

export type Slot = 'blue' | 'green'

export type Healthcheck = {
  kind: 'http' | 'tcp' | 'pgrep'
  target: string // path for http, host:port for tcp, cmd for pgrep
  interval: string
  timeout: string
  startPeriod: string
  retries: number
}

export type Mount =
  | { type: 'volume'; volume: string; mount: string }
  | { type: 'file'; file: string; mount: string; ro: boolean }

// An environment-level env var entry: a stable id, a key (the name the
// container reads — a mutable label) and a value. Secret entries live in the
// secret store instead and reference their env entry.
export type EnvVar = {
  id: string
  key: string
  value: string
}

// A service is one container. Tenant services are the app's own containers;
// a BACKING service (a shared datastore like postgres/valkey) is the same
// Service shape plus adapter knowledge: `adapter` selects the provisioning
// adapter from the registry, `serviceName` is the unique DNS-resolvable name
// consumers connect to, `prefix` overrides the adapter's fact prefix.
export type Service = {
  id: string
  name: string
  image: string
  role: string // human label of what it does
  zones: string[]
  strategy: ServiceStrategy
  onFailure?: 'switch_back' | 'leave_active'
  healthcheck: Healthcheck | null
  resources: { mem: string; cpus: string }
  command?: string
  mounts: Mount[]
  envFiles: string[]
  environment: EnvVar[] // service-scoped env vars (exposure: this service)
  aliases: string[]
  dependsOn: string[]
  expose: string[] // e.g. "cms:3000"
  restart: 'unless-stopped' | 'always' | 'no'
  replicas: number
  runtimeIntent: ServiceRuntimeIntent
  status: HealthState
	 nativeCompose?: string
	 releaseLedger?: DeployRecord[]
  activeSlot?: Slot // for blue-green services
  group?: 'app' | 'workers' | 'identity' | 'web' | 'edge'
  // backing services only:
  adapter?: string // adapter registry key, e.g. "postgres:16"
  serviceName?: string // unique DNS name consumers connect to, e.g. "postgres"
  prefix?: string // fact prefix override (defaults to the adapter's)
}

export type Zone = {
  id: string
  environmentId: string
  name: string
  subnet: string
  internal: boolean
  ownerKind: 'environment' | 'backing_project'
  ownerId: string
}

export type Attach = {
  id: string
  name: string // the attach's spec key — hyphenated, operator-chosen, unique per environment; nothing else derives from it
  backingProjectId: string
  backingServiceId: string
  backingEnvironmentId?: string
  backingNetworkId: string
  serviceId: string
  credential: { mode: 'new' | 'existing'; attachId?: string }
  grantAttachIds: string[]
  factSets: { grantAttachId?: string; facts: { key: string; secret: boolean }[] }[]
  projectId: string // references a BACKING project
  database: string // Controller-resolved non-secret fact; "—" when the adapter has no database
  role: string // Controller-resolved non-secret fact
  service: string // the one service in this environment that gains access
  grants?: string[] // other attaches' databases this role may also access
  status: HealthState
}

export type Route = {
  id: string
  environmentId: string
  host: string
  path: string
  exposure: 'public' | 'internal'
  targetServiceId: string
  targetPort: number
  status: 'unserved' | 'pending' | 'served' | 'degraded'
}

type EnvironmentComponentBase = {
  id: string
  owner: 'environment'
  ownerRef: string
  enabled: boolean
  status: HealthState
  dependencies: string[]
  generatedServices: string[]
}

export type CaddyComponent = EnvironmentComponentBase & {
  kind: 'caddy'
  config: { zone_id: string; caddyfile_template?: string } | null
  state: { pinnedIPv4?: string }
}

export type CloudflareTunnelComponent = EnvironmentComponentBase & {
  kind: 'cloudflare-tunnel'
  config: { secret_id: string } | null
  state: Record<string, never>
}

export type EnvironmentComponent = CaddyComponent | CloudflareTunnelComponent

export type SecretKind = 'env' | 'file'
export type ReusableSecretBase = {
  id: string
  key: string
  kind: SecretKind
  ref: string
  updatedAt: string
}

export type ProjectReusableSecret = ReusableSecretBase & {
  scope: 'project'
  projectId: string
}

export type PlatformReusableSecret = ReusableSecretBase & {
  scope: 'platform'
  projectId?: never
}

export type ReusableSecret = ProjectReusableSecret | PlatformReusableSecret

export type ScriptHook =
  | 'manual'
  | 'pre-deploy'
  | 'post-deploy'
  | 'pre-rollback'
  | 'post-rollback'
  | 'on-failure'

export type Script = {
  id: string
  environmentId: string
  slug: string
  serviceId: string
  service: string
  when: ScriptHook
  body: string
  origin: 'api' | 'blueprint'
  reconciliationKey?: string
  activeGeneration: number
}

// A backup source is an ATTACH's database, a VOLUME, or the environment's
// CONFIG (env entries: vars, files, secrets — values included, age-encrypted).
// One source per attach, never per service, so a shared attach (api + worker +
// scheduler) is backed up once, not three times. Config backs up THIS
// environment's entries only — never backing environments, never
// platform state.
export type BackupSource = {
  id: string
  kind: 'attach' | 'volume' | 'config'
  ref?: string // attach id or volume name; omitted for config
  name: string
  target: string
}

export type BackupPolicy = {
  enabled: boolean
  frequency: string // Controller-evaluated bounded UTC frequency
  keep: number
  encryption: 'age' | 'none'
  ageRecipientRef?: string
  connector?: string
  sources: BackupSource[]
  nextRun: string
  lastRun: string
  lastStatus: HealthState
}

export type BackupPolicySourceRecord = {
  id: string
  kind: BackupSource['kind']
  targetId: string
}

export type BackupPolicySourceInput = Pick<BackupPolicySourceRecord, 'kind' | 'targetId'>

// Authoritative Controller projection for the one Backup Policy singleton
// owned by a tenant Environment. An unconfigured policy is represented by an
// effective disabled document with optional configuration omitted and sources empty.
export type BackupPolicyDocument = {
  enabled: boolean
  nextRunAt: string | null
  frequency?: string
  keep?: number
  encryption?: 'age' | 'none'
  connectorId?: string
  sources: BackupPolicySourceRecord[]
  ageRecipient?: string
  keyEra?: number
  keyCreatedAt?: string
  keyRotatedAt?: string
}

export type BackupPolicyReplacement = Omit<BackupPolicyDocument, 'sources' | 'ageRecipient' | 'keyEra' | 'keyCreatedAt' | 'keyRotatedAt' | 'nextRunAt'> & {
  sources: BackupPolicySourceInput[]
}

export type BackupPolicyState = {
  policy: BackupPolicyDocument
  attaches: {
    id: string
    name: string
    backingProjectId: string
    backingServiceId: string
  }[]
  volumes: { id: string; slug: string; key: string }[]
  loaded: boolean
  loading: boolean
  saving: boolean
  loadError: string | null
  saveError: string | null
  recoveryPoints: RecoveryPointState
}

export type RecoveryPointState = {
  items: RecoveryPoint[]
  nextCursor: string | null
  loaded: boolean
  loading: boolean
  loadingMore: boolean
  loadError: string | null
  failedCursor: string | null
}

type RecoveryPointBase = {
  id: string
  sourceId: string
  sourceKind: BackupSource['kind']
  targetId: string
  createdAt: string
  sizeBytes: number
  status: 'verified'
}

export type RecoveryPoint = RecoveryPointBase & (
  | { encrypted: true; keyEra: number }
  | { encrypted: false; keyEra?: never }
)

export type DeployRecord = {
  id: string // stable id — deploy history and rollback reference deployments by id
  service: string // which service this deployment released
  tag: string // immutable image tag, e.g. "sha-9f3c1ad"
  digest: string
  strategy: ServiceStrategy // the strategy chosen for THIS deployment
  when: string
  status: 'active' | 'superseded'
}

export type ReleaseGroupOnFailure = 'switch_back' | 'leave_active'

export type ReleaseGroup = {
  id: string
  name: string
  services: string[]
  order: string[]
  tag?: string
  onFailure: ReleaseGroupOnFailure
}

// A plain (non-secret) FILE entry of an environment: materialized by the
// Agent into the environment's volume folder at <volume_dir>/<path>, exposed
// to a specific service (exposedTo) or all services (undefined).
export type EnvFile = {
  id: string
  name: string
  path: string // relative to the environment's volume folder
  content: string
  exposedTo?: string
}

// The environment's age keypair for backup encryption: generated LAZILY the
// first time backups are enabled (never at creation — a staging/dev env that
// never backs up gets no key). Backups are encrypted with the PUBLIC
// recipient (safe in desired state). The PRIVATE identity is Controller-only,
// never part of this public Environment shape or an ordinary Secret/EnvFile,
// and is returned only by explicit export.
export type EnvAgeKey = {
  recipient: string // age1… — the encryption recipient, referenced by the backup policy
  generatedAt: string
  lastRotatedAt?: string
  // key era: 1 = the original keypair, incremented on every rotation.
  // Recovery points record the era they were encrypted under — restoring a
  // point from a rotated-away era requires the previously exported identity.
  keyEra: number
}

export type BlueprintFileAudit = {
  path: string
  part: string
  size: number
  sha256: string
}

export type BlueprintApplyAudit = {
  generation: number
  rootPath: string
  composeSources: string[]
  interpolationKeys: string[]
  files: BlueprintFileAudit[]
  appliedAt: string
}

export type EnvironmentEntrySource =
  | { kind: 'literal'; literal?: string }
  | { kind: 'secret_ref'; secretRef: string }
  | { kind: 'fact'; attachId: string; grantAttachId?: string; fact: string }

export type EnvironmentEntry = {
  id: string
  type: 'env' | 'file'
  key?: string
  path?: string
  uid?: number
  gid?: number
  source: EnvironmentEntrySource
  exposure: string[]
  secret: boolean
}

export type Environment = {
  id: string
  projectId: string
  name: string // staging | production
  networkPool: string
  networkCapacity: EnvironmentNetworkCapacity
  status: HealthState
  provisioningState: 'provisioning' | 'ready' | 'failed'
  createTaskId: string | null; deletionTaskId: string | null
  release: string // tag of the most recent deployment
  previousRelease?: string
  deploys: DeployRecord[]
  releaseGroups: ReleaseGroup[]
  zones: Zone[]
  services: Service[]
  attaches: Attach[]
  routes: Route[]
  components: EnvironmentComponent[]
  volumes: Volume[]
  volumeDir: string
  entries: EnvironmentEntry[]
  envVars: EnvVar[] // all-services env vars (exposure: all services)
  files: EnvFile[]
  age?: EnvAgeKey // lazy: generated when backups are first enabled
  scripts: Script[]
  backup?: BackupPolicy // tenant environments only; backing environments never own a policy
  retention: { inactiveSlotDays: number; keepImages: number }
  lastDeployAt: string
  lastAppliedBlueprint?: BlueprintApplyAudit
}

export type ProvisionOp = {
  op: string
  detail: string
}

export type Adapter = {
  key: string // "postgres:16" / "valkey:9" / "manual"
  label: string // "PostgreSQL" / "Valkey" / "Manual"
  prefix: string
  urlScheme: string // "pgsql" / "redis"
  requires: { database: boolean; role: boolean }
  envVars: string[]
  provision: ProvisionOp[]
  // manual = network-only: attaching joins the service's network and that's
  // all — no auto-provisioning, no facts, no credentials, no backups. The
  // operator runs and manages the service themselves; Groundplane only wires
  // connectivity.
  manual?: boolean
}

// A consumer is the triple (environment, service, attach): the same
// environment appears once per attaching service, and potentially twice for
// the same service (two attaches of the same shared project).
export type ConsumerLink = {
  tenant: string
  project: string
  environment: string
  service: string
  attachId: string
  database: string
  role: string
  connectionFactKey?: string
}

// A project is an application (tenant kind) or a backing service (backing
// kind — a datastore/cache the apps attach to). BOTH kinds follow the same
// hierarchy: project → environment(s) → services. A backing project holds
// exactly one environment ("main") with one adapter-backed service; the
// controller logic (envs, services, attaches, backups) is shared.
export type Project = {
  id: string
  slug: string
  name: string
  kind: 'tenant' | 'backing'
  tenantId: string | null
  description: string
  deletionTaskId: string | null
  environments?: Environment[]
  status?: HealthState
  consumers?: ConsumerLink[]
  createdAt?: string
}

export type Runner = {
  id: string
  slug: string
  tenantId: string
  projectId: string | null
  githubUrl: string
  name: string
  labels: string[]
  lifecycle: 'provisioning' | 'ready' | 'failed' | 'deleting'
  createTaskId: string
  removeTaskId: string | null
  online: boolean
  observedAt: string | null
  createdAt: string
}

// A connector credential is either a REFERENCE to a secret-store env var
// (e.g. R2_ACCESS_KEY_ID — never stored in desired state) or a DIRECT value
// baked into the connector (stored encrypted).
export type ConnectorCredentialInput = { kind: 'ref'; name: string } | { kind: 'value'; value: string }
export type ConnectorCredential = { kind: 'ref'; name: string } | { kind: 'direct' }

export type Connector = {
  id: string
  name: string
  kind: 's3-compatible'
  scope: 'environment'
  scopeRef: string
  endpoint: string
  bucket: string
  prefix: string
  region: string
  pathStyle: boolean
  credentials: { accessKey: ConnectorCredential; secretKey: ConnectorCredential }
}

export type ConnectorCreateInput = Omit<Connector, 'id' | 'credentials'> & {
  credentials: { accessKey: ConnectorCredentialInput; secretKey: ConnectorCredentialInput }
}

export type Tenant = {
  id: string
  slug: string
  name: string
  description: string
  deletionTaskId: string | null
}

export type TaskType =
  | 'deploy'
  | 'rollback'
  | 'backup'
  | 'backup_prune'
  | 'restore'
  | 'attach'
  | 'detach'
  | 'run'
  | 'script'
  | 'provision'
  | 'create'
  | 'start'
  | 'stop'
  | 'destroy'
  | 'remove'
  | 'update'
  | 'rotate'

export type TaskStepState = 'pending' | 'running' | 'done' | 'failed'

export type TaskStep = {
  label: string
  state: TaskStepState
  detail?: string
}

export type TaskStatus = 'pending' | 'running' | 'completed' | 'failed' | 'timed_out' | 'aborted'

export type TaskWorkspaceType = 'platform' | 'tenant'
export type TaskActor = 'operator' | 'system'

export type TaskJournalScope =
  | { kind: 'all' }
  | { kind: 'workspace'; workspace: 'platform' | string }
  | { kind: 'project'; projectId: string }
  | { kind: 'environment'; environmentId: string }

export type TaskJournalSurface = 'tasks' | 'activity'

export type TaskJournalState = {
  entries: ActivityEntry[]
  nextCursor: string | null
  loaded: boolean
  loading: boolean
  loadingMore: boolean
  loadError: string | null
  failedCursor: string | null
}

export type ActivityEntry = {
  id: string
  operationId?: string
  retryOf?: string
  idempotencyKey?: string
  planHash?: string
  params?: Record<string, string>
  timeout?: string
  type: TaskType
  title: string
  target: string
  workspace: string // "platform" or tenant slug
  status: TaskStatus
  actor: string
  ts: string
  workspaceType?: TaskWorkspaceType
  tenantId?: string
  projectId?: string
  environmentId?: string
  createdAt?: string
  updatedAt?: string
  startedAt?: string | null
  finishedAt?: string | null
  note?: string
  taskState?: 'queued' | 'pulled' | 'running' | 'acked'
  steps?: TaskStep[]
}

export type HostInfo = {
  hostname: string
  arch: string
  os: string
  uptime: string
  cpu: { model: string; cores: number; load: number }
  memory: { total: string; used: string; usedPct: number }
  disk: { total: string; used: string; usedPct: number }
  swap: { total: string; used: string; usedPct: number }
  docker: string
  etcd: { node: string; status: HealthState; dbSize: string }
  controller: { service: string; status: HealthState; version: string }
  agent: { status: HealthState; pullInterval: string; maxConcurrent: number; labels: string[] }
}

// Platform runtime state rendered by the Controller. The Controller owns the
// local Agent container lifecycle; the Agent only executes assigned work.
export type PlatformComponent = {
  id: string
  name: string
  kind: 'agent' | 'coredns' | 'controller'
  status: HealthState
  image: string
  version: string
  runtime: string
  hostNetwork?: boolean
  mounts: string[]
  notes: string[]
}

export type PlatformAgent = {
  id: string
  enrollmentTaskId: string
  host: string
  status: HealthState
  version: string | null
  labels: Record<string, string>
  readyAt: string | null
  lastReportAt: string | null
  inFlight: number
}

export type PlatformInfra = {
  project: string
  components: PlatformComponent[]
  agents: PlatformAgent[]
  controllerHistory: { version: string; when: string; status: 'completed' | 'failed' }[]
  dns: {
    enabled: boolean
    listen?: string
    // resolver addresses only (e.g. "1.1.1.1 8.8.8.8"); when upstreamAuto
    // is true they are a fallback read from /etc/resolv.conf at render time
    upstream?: string
    upstreamAuto?: boolean
    tailnetDelegation?: boolean
    // per-zone forwarders: domain routed to specific resolvers
    // (rendered as `forward <domain> <resolvers>` lines in the Corefile)
    forwarders?: { id?: string; domain: string; upstream: string }[]
    staticEntries?: number
    corefileRev?: number
    reloaded?: string
  }
}
import type { Volume } from '@/features/volume/types'

export type { Volume, VolumeDeletionImpactItem, VolumeDeletionImpactPage } from '@/features/volume/types'
