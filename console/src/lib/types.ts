import type { EnvironmentNetworkCapacity } from "@/features/environment/types";
import type { EnvironmentEntry } from "./entry-types";
import type { ServiceObservation } from "@/features/service/service-observation";
import type { BackingHooks } from "@/features/backing-service/api";
import type { Script } from "@/features/script/types";
import type { Volume } from "@/features/volume/types";
import type { BackupPolicy } from "@/features/backup/types";
import type { TaskResponse } from "@/features/task/api";
export type { EnvironmentEntry, EnvironmentEntrySource } from "./entry-types";

// Groundplane desired-state model (prototype). Mirrors the Blueprint contract.
export type HealthState =
  "healthy" | "degraded" | "failed" | "stopped" | "pending" | "unknown";

export type ServiceStrategy = "blue-green" | "recreate" | "rolling";

export type ServiceRuntimeIntent = "running" | "stopped" | "absent";

export type ValkeyAuthentication = "username_password" | "password" | "none";

export type Slot = "blue" | "green";

export type Healthcheck = {
  test: string[];
  disable: boolean;
  interval: string;
  timeout: string;
  startPeriod: string;
  startInterval: string;
  retries: number;
};

export type Mount =
  | { type: "volume"; volume: string; mount: string; ro?: boolean }
  | { type: "file"; file: string; mount: string; ro: boolean };

export type ServiceDependencyCondition =
  "service_started" | "service_healthy" | "service_completed_successfully";

export type ServiceDependencyPhase = "start" | "deploy" | "rollback" | "always";

export type ServiceDependency = {
  condition: ServiceDependencyCondition;
  phases: ServiceDependencyPhase[];
};

export type ServiceLogging = {
  maxSize: string;
  maxFile: number;
};

// An environment-level env var entry: a stable id, a key (the name the
// container reads — a mutable label) and a value. Secret entries live in the
// secret store instead and reference their env entry.
export type EnvVar = {
  id: string;
  key: string;
  value: string;
};

// A service is one container. Tenant services are the app's own containers;
// a BACKING service (a shared datastore like postgres/valkey) is the same
// Service shape plus adapter knowledge: `adapter` selects the provisioning
// adapter from the registry, `serviceName` is the unique DNS-resolvable name
// consumers connect to, `prefix` overrides the adapter's fact prefix.
export type Service = {
  id: string;
  name: string;
  image: string;
  role: string; // human label of what it does
  zones: string[];
  strategy: ServiceStrategy;
  onFailure?: "switch_back" | "leave_active";
  rollingReplicaTimeoutSeconds?: number;
  healthcheck: Healthcheck | null;
  resources: { mem: string; cpus: string };
  command: string[];
  entrypoint: string[];
  workingDir: string;
  user: string;
  mounts: Mount[];
  envFiles: string[];
  environment: EnvVar[]; // service-scoped env vars (exposure: this service)
  aliases: string[];
  aliasesByZone: Record<string, string[]>;
  dependsOn: string[];
  dependencies: Record<string, ServiceDependency>;
  logging: ServiceLogging;
  expose: string[]; // e.g. "cms:3000"
  restart: "unless-stopped" | "always" | "no";
  replicas: number;
  runtimeIntent: ServiceRuntimeIntent;
  observation: ServiceObservation;
  nativeCompose?: string;
  releaseLedger?: DeployRecord[];
  activeSlot?: Slot; // for blue-green services
  group?: "app" | "workers" | "identity" | "web" | "edge";
  // backing services only:
  adapter?: string; // adapter registry key, e.g. "postgres"
  adapterVersion?: string;
  hooks?: BackingHooks;
  serviceName?: string; // unique DNS name consumers connect to, e.g. "postgres"
  prefix?: string; // fact prefix override (defaults to the adapter's)
  authentication?: string; // raw immutable Valkey instance policy, projected from the backing facade and validated before use
};

export type DNSRecord =
  | { hostname: string; address: string; service_id?: never; zone_id?: never }
  | { hostname: string; service_id: string; zone_id: string; address?: never };

export type Zone = {
  id: string;
  environmentId: string;
  name: string;
  subnet: string;
  internal: boolean;
  allowGPDNS: boolean;
  ownerKind: "environment" | "backing_project";
  ownerId: string;
};

export type Attach = {
  id: string;
  name: string; // the attach's spec key — hyphenated, operator-chosen, unique per environment; nothing else derives from it
  backingProjectId: string;
  backingServiceId: string;
  backingEnvironmentId?: string;
  backingNetworkId: string;
  serviceId: string;
  credential: { mode: "new" | "existing"; attachId?: string };
  grantAttachIds: string[];
  factSets: {
    grantAttachId?: string;
    facts: { key: string; secret: boolean }[];
  }[];
  projectId: string; // references a BACKING project
  database: string; // Controller-resolved non-secret fact; "—" when the adapter has no database
  role: string; // Controller-resolved non-secret fact
  factLoadError?: string; // Optional value reads must not hide the connection or its Environment.
  service: string; // the one service in this environment that gains access
  grants?: string[]; // other attaches' databases this role may also access
  status: HealthState;
};

export type Route = {
  id: string;
  environmentId: string;
  host: string;
  path: string;
  exposure: "public" | "internal";
  targetServiceId: string;
  targetPort: number;
  status: "unserved" | "pending" | "served" | "degraded";
};

type EnvironmentComponentBase = {
  software?: import("./api.generated").components["schemas"]["ContainerIdentity"];
  id: string;
  owner: "environment";
  ownerRef: string;
  enabled: boolean;
  status: HealthState;
  dependencies: string[];
  generatedServices: string[];
};

export type CaddyComponent = EnvironmentComponentBase & {
  kind: "caddy";
  config: {
    zone_ids: string[];
    caddyfile_template?: string;
    alias?: string;
  } | null;
  state: { pinnedIPv4?: string };
};

export type CloudflareTunnelComponent = EnvironmentComponentBase & {
  kind: "cloudflare-tunnel";
  config: { zone_ids: string[]; secret_id: string } | null;
  state: Record<string, never>;
};

export type EnvoyComponent = EnvironmentComponentBase & {
  kind: "envoy";
  config: { zone_ids: string[]; alias?: string } | null;
  state: { pinnedIPv4?: string };
};

export type HTTPRouterComponent = CaddyComponent | EnvoyComponent;
export type EnvironmentComponent =
  HTTPRouterComponent | CloudflareTunnelComponent;

export type SecretKind = "env" | "file";

export type ReusableSecretBase = {
  id: string;
  key: string;
  kind: SecretKind;
  ref: string;
  updatedAt: string;
};

export type ProjectReusableSecret = ReusableSecretBase & {
  scope: "project";
  projectId: string;
};

export type PlatformReusableSecret = ReusableSecretBase & {
  scope: "platform";
  projectId?: never;
};

export type ReusableSecret = ProjectReusableSecret | PlatformReusableSecret;

export type { Script, ScriptHook } from "@/features/script/types";

export type DeployRecord = {
  id: string; // stable id — deploy history and rollback reference deployments by id
  service: string; // which service this deployment released
  tag: string; // immutable image tag, e.g. "sha-9f3c1ad"
  digest: string;
  strategy: ServiceStrategy; // the strategy chosen for THIS deployment
  when: string;
  status: "active" | "superseded";
};

export type ReleaseGroupOnFailure = "switch_back" | "leave_active";

export type ReleaseGroup = {
  id: string;
  name: string;
  services: string[];
  order: string[];
  tag?: string;
  onFailure: ReleaseGroupOnFailure;
};

// A plain (non-secret) FILE entry of an environment: materialized by the
// Agent into the environment's volume folder at <volume_dir>/<path>, exposed
// to a specific service (exposedTo) or all services (undefined).
export type EnvFile = {
  id: string;
  name: string;
  path: string; // relative to the environment's volume folder
  content: string;
  exposedTo?: string;
};

export type BlueprintFileAudit = {
  path: string;
  part: string;
  size: number;
  sha256: string;
};

export type BlueprintApplyAudit = {
  generation: number;
  rootPath: string;
  composeSources: string[];
  interpolationKeys: string[];
  files: BlueprintFileAudit[];
  appliedAt: string;
};

export type Environment = {
  id: string;
  resourceLoadError?: string;
  projectId: string;
  name: string; // staging | production
  networkPool: string;
  networkCapacity: EnvironmentNetworkCapacity;
  status: HealthState;
  provisioningState: "provisioning" | "ready" | "failed";
  createTaskId: string | null;
  deletionTaskId: string | null;
  release: string; // tag of the most recent deployment
  previousRelease?: string;
  deploys: DeployRecord[];
  releaseGroups: ReleaseGroup[];
  zones: Zone[];
  services: Service[];
  attaches: Attach[];
  routes: Route[];
  components: EnvironmentComponent[];
  volumes: Volume[];
  volumeDir: string;
  entries: EnvironmentEntry[];
  envVars: EnvVar[]; // all-services env vars (exposure: all services)
  files: EnvFile[];
  scripts: Script[];
  backup?: BackupPolicy; // tenant environments only; backing environments never own a policy
  retention: { inactiveSlotDays: number; keepImages: number };
  lastDeployAt: string;
  lastAppliedBlueprint?: BlueprintApplyAudit;
};

export type ProvisionOp = {
  op: string;
  detail: string;
};

export type Adapter = {
  key: string; // "postgres" / "valkey" / "custom"
  label: string; // "PostgreSQL" / "Valkey" / "Custom"
  prefix: string;
  urlScheme: string; // "pgsql" / "redis"
  requires: { database: boolean; role: boolean };
  envVars: string[];
  provision: ProvisionOp[];
  custom?: boolean;
};

// A consumer is the triple (environment, service, attach): the same
// environment appears once per attaching service, and potentially twice for
// the same service (two attaches of the same shared project).
export type ConsumerLink = {
  tenant: string;
  project: string;
  environment: string;
  service: string;
  attachId: string;
  database: string;
  role: string;
  connectionFactKey?: string;
};

// A project is an application (tenant kind) or a backing service (backing
// kind — a datastore/cache the apps attach to). BOTH kinds follow the same
// hierarchy: project → environment(s) → services. A backing project holds
// exactly one environment ("main") with one adapter-backed service; the
// controller logic (envs, services, attaches, backups) is shared.
export type Project = {
  id: string;
  environmentLoadError?: string;
  slug: string;
  name: string;
  kind: "tenant" | "backing";
  tenantId: string | null;
  description: string;
  deletionTaskId: string | null;
  environments?: Environment[];
  status?: HealthState;
  consumers?: ConsumerLink[];
  createdAt?: string;
};

export type Runner = {
  software?: import("./api.generated").components["schemas"]["ContainerIdentity"];
  id: string;
  slug: string;
  tenantId: string;
  projectId: string | null;
  environmentId: string | null;
  githubUrl: string;
  name: string;
  labels: string[];
  lifecycle: "provisioning" | "ready" | "failed" | "deleting";
  createTaskId: string;
  removeTaskId: string | null;
  online: boolean;
  observedAt: string | null;
  createdAt: string;
};

// A connector credential is either a REFERENCE to a secret-store env var
// (e.g. R2_ACCESS_KEY_ID — never stored in desired state) or a DIRECT value
// baked into the connector (stored encrypted).
export type ConnectorCredentialInput =
  { kind: "ref"; name: string } | { kind: "value"; value: string };

export type ConnectorCredential =
  { kind: "ref"; name: string } | { kind: "direct" };

export type Connector = {
  id: string;
  name: string;
  kind: "s3-compatible";
  scope: "environment";
  scopeRef: string;
  endpoint: string;
  bucket: string;
  prefix: string;
  region: string;
  pathStyle: boolean;
  credentials: {
    accessKey: ConnectorCredential;
    secretKey: ConnectorCredential;
  };
};

export type ConnectorCreateInput = Omit<Connector, "id" | "credentials"> & {
  credentials: {
    accessKey: ConnectorCredentialInput;
    secretKey: ConnectorCredentialInput;
  };
};

export type ConnectorEditInput = Partial<
  Pick<
    Connector,
    "name" | "endpoint" | "bucket" | "prefix" | "region" | "pathStyle"
  >
> & {
  credentials?: {
    accessKey?: ConnectorCredentialInput;
    secretKey?: ConnectorCredentialInput;
  };
};

export type Tenant = {
  id: string;
  slug: string;
  name: string;
  description: string;
  deletionTaskId: string | null;
};

export type TaskType =
  | "deploy"
  | "rollback"
  | "backup"
  | "backup_prune"
  | "restore"
  | "attach"
  | "detach"
  | "run"
  | "script"
  | "provision"
  | "create"
  | "start"
  | "stop"
  | "destroy"
  | "remove"
  | "update"
  | "rotate"
  | "fetch"
  | "prepare"
  | "apply"
  | "check";

export type TaskStepState = "pending" | "running" | "done" | "failed";

export type TaskStep = {
  label: string;
  action?: string;
  description?: string;
  target?: string;
  timeoutSeconds?: number;
  state: TaskStepState;
  detail?: string;
  healthWait?: import("@/lib/api.generated").components["schemas"]["Readiness"];
  healthWaitReceivedAt?: string;
  failure?: import("@/lib/api.generated").components["schemas"]["TaskFailure"];
  healthchecks?: import("@/lib/api.generated").components["schemas"]["ConfiguredHealthcheck"][];
};

export type TaskStatus =
  "pending" | "running" | "completed" | "failed" | "timed_out" | "aborted";

export type TaskWorkspaceType = "platform" | "tenant";

export type TaskActor = "operator" | "system";

export type TaskJournalScope = (
  | { kind: "all" }
  | { kind: "workspace"; workspace: "platform" | string }
  | { kind: "project"; projectId: string }
  | { kind: "environment"; environmentId: string }
) & {
  status?: TaskStatus;
  resourceKind?: string;
  taskType?: TaskType;
};

export type TaskJournalSurface = "tasks" | "activity";
export type TaskPageSize = 5 | 10 | 25 | 50;

export type TaskJournalState = {
  entries: ActivityEntry[];
  pageSize: TaskPageSize;
  pageIndex: number;
  pageCursors: (string | undefined)[];
  nextCursor: string | null;
  loaded: boolean;
  loading: boolean;
  loadingMore: boolean;
  loadError: string | null;
  failedCursor: string | null;
};

export type ActivityEntry = {
  reconciliationRequired?: boolean;
  imageFetch?: TaskResponse["image_fetch"];
  interruptedDetachRetry?: TaskResponse["interrupted_detach_retry"];
  recoveryRetryAvailable?: boolean;
  diagnosticsExpiredAt?: string;
  id: string;
  operationId?: string;
  retryOf?: string;
  idempotencyKey?: string;
  planHash?: string;
  params?: Record<string, string>;
  timeout?: string;
  type: TaskType;
  title: string;
  target: string;
  targetName?: string;
  resourceKind?: string;
  executor?: TaskResponse["executor"];
  timeoutSeconds?: number;
  resultSummary?: string;
  failure?: TaskResponse["failure"];
  recovery?: TaskResponse["recovery"];
  preparation?: TaskResponse["preparation"];
  attemptHistory?: TaskResponse["attempt_history"];
  executions?: TaskResponse["executions"];
  workspace: string; // "platform" or tenant slug
  status: TaskStatus;
  actor: string;
  ts: string;
  workspaceType?: TaskWorkspaceType;
  tenantId?: string;
  projectId?: string;
  environmentId?: string;
  createdAt?: string;
  updatedAt?: string;
  startedAt?: string | null;
  finishedAt?: string | null;
  note?: string;
  taskState?: "queued" | "pulled" | "running" | "acked";
  steps?: TaskStep[];
};

// Platform runtime state rendered by the Controller. The Controller owns the
// local Agent container lifecycle; the Agent only executes assigned work.
export type PlatformComponent = {
  software?: import("./api.generated").components["schemas"]["ContainerIdentity"];
  id: string;
  name: string;
  kind: "agent" | "coredns" | "controller";
  status: HealthState;
  runtime: string;
  hostNetwork?: boolean;
  mounts: string[];
  notes: string[];
};

export type PlatformAgent = {
  id: string;
  enrollmentTaskId: string;
  host: string;
  status: HealthState;
  version: string | null;
  installedImage: string | null;
  source?: import("@/lib/api.generated").components["schemas"]["BuildProvenance"];
  labels: Record<string, string>;
  readyAt: string | null;
  lastReportAt: string | null;
  inFlight: number;
};

export type ManagedConfigFile = {
  generatedDirectives?: string;
  path: string;
  template: string;
  rendered: string;
};

export type PlatformInfra = {
  project: string;
  components: PlatformComponent[];
  agents: PlatformAgent[];
  controllerHistory: {
    version: string;
    when: string;
    status: "completed" | "failed";
  }[];
  dns: {
    records?: DNSRecord[];
    enabled: boolean;
    listen?: string;
    // resolver addresses only (e.g. "1.1.1.1 8.8.8.8"); when upstreamAuto
    // is true they are a fallback read from /etc/resolv.conf at render time
    upstream?: string;
    upstreamAuto?: boolean;
    tailnetDelegation?: boolean;
    corefileTemplate?: string;
    // per-zone forwarders: domain routed to specific resolvers
    // (rendered as `forward <domain> <resolvers>` lines in the Corefile)
    forwarders?: { id?: string; domain: string; upstream: string }[];
    staticEntries?: number;
    corefileRev?: number;
    reloaded?: string;
  };
};

export type {
  Volume,
  VolumeDeletionImpactItem,
  VolumeDeletionImpactPage,
} from "@/features/volume/types";
