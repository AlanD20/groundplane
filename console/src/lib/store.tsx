"use client";
import {
  createServiceActions,
  type ServiceActions,
} from "@/features/service/actions";
import { findEnvironment } from "@/features/environment/environment-removal-model";
import {
  useRunnerStore,
  type RunnerState,
  type RunnerActions,
} from "@/features/runner/use-runner-store";
import {
  zoneFromAPI,
  listAllZones,
  routeFromAPI,
  listAllRoutes,
  type ZoneCreateRequest,
  type ZoneCreateResponse,
  type ZoneRemoveResponse,
  type ZoneRemovalImpactResponse,
  type RouteCreateRequest,
  type RouteCreateAccepted,
  type RouteEditRequest,
  type RouteEditAccepted,
  type RouteShowResponse,
  type ZoneShowResponse,
} from "@/features/environment/network-api";
import {
  listAllComponents,
  listPlatformComponents,
} from "@/features/component/api";
import { listAllReleases, projectReleaseSummary } from "@/features/release/api";
import { listAllAgents } from "@/features/agent/api";
import {
  useConnectorStore,
  type ConnectorState,
  type ConnectorActions,
} from "@/features/connectors/use-connector-store";
import {
  scriptFromAPI,
  scriptCreateToAPI,
  scriptPatchToAPI,
  type ScriptCreateResponse,
  type ScriptEditResponse,
} from "./script-api";
import type { ScriptInput, ScriptPatch } from "./script-types";
import { entryFromAPI } from "./entry-api";
import {
  emptyTaskJournal,
  taskJournalKey,
  taskJournalQuery,
  taskFromAPI,
  parseTaskEvent,
  requireTaskId,
  type TaskPageResponse,
  type TaskEventResponse,
} from "@/features/task/journal-model";
import { controllerRequest, waitForRequest } from "./controller-json-request";
import { useBackupStore } from "@/features/backup/use-backup-store";
import {
  createSecretActions,
  useSecretRefresh,
  type ReusableSecretState,
  type ReusableSecretActions,
} from "@/features/secrets/secret-store";
import { controllerUpdateRejected } from "./controller-request-errors";
import { applyServiceObservations } from "@/features/service/service-observation";
import {
  releaseForServiceName,
  serviceFromAPI,
  listAllServices,
  type ServiceShowResponse,
} from "@/features/service/api";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type { operations } from "./api.generated";
import {
  watchTransientLogs,
  type LogTarget,
  type TransientLogEvent,
} from "./transient-logs";
import type {
  BackingServiceCreateRequest,
  BackingServiceCreatedResponse,
} from "@/features/backing-service/api";
import { hydratePlatformComponents } from "./platform-component-hydration";
import { useControllerPlatform } from "@/features/platform-controller/use-controller-platform";
import type {
  HealthState,
  ActivityEntry,
  Attach,
  EnvFile,
  Environment,
  EnvironmentEntry,
  EnvironmentComponent,
  Project,
  ReleaseGroup,
  Route,
  Script,
  Service,
  ServiceRuntimeIntent,
  TaskJournalScope,
  TaskJournalState,
  TaskJournalSurface,
  Tenant,
  Volume,
  VolumeDeletionImpactPage,
  Zone,
  PlatformInfra,
  ManagedConfigFile,
} from "./types";
import {
  createBlueprintActions,
  type BlueprintActions,
} from "@/features/blueprint/api";
import {
  adapters as seedAdapters,
  platform as seedPlatform,
} from "./mock-data";
import { createEnvironmentComponents, routerProjection } from "./components";
import { applyAuthoritativeEnvironmentScalars } from "./environment-authoritative";
import { useEnvironmentLifecycle } from "./environment-lifecycle";
import type {
  EnvironmentDeletionFailure,
  TaskResponse,
} from "@/features/environment/environment-removal-model";
import {
  environmentMutationKey,
  loadEnvironmentMutationIntents,
  persistEnvironmentMutationIntents,
  type EnvironmentMutationIntent,
} from "./environment-storage";
import { environmentFromAPI } from "./environment-projection";
import {
  environmentGenerationSnapshot,
  mergeEnvironmentProjectLoads,
} from "./environment-hydration";
import { environmentDeletionGuard } from "./environment-guard";
import { observeEnvironmentTask } from "./environment-task-observation";
import {
  createVolume,
  editVolume,
  getVolume,
  getVolumeDeletionImpact,
  listAllVolumes,
  removeVolume,
} from "@/features/volume/api";
import { newId, newULID } from "./utils";
type TenantPageResponse =
  operations["tenant.list"]["responses"][200]["content"]["application/json"];
type TenantCreateRequest =
  operations["tenant.create"]["requestBody"]["content"]["application/json"];
type TenantCreateResponse =
  operations["tenant.create"]["responses"][201]["content"]["application/json"];
type TenantEditRequest =
  operations["tenant.edit"]["requestBody"]["content"]["application/json"];
type TenantEditResponse =
  operations["tenant.edit"]["responses"][200]["content"]["application/json"];
type TenantRenameRequest =
  operations["tenant.rename"]["requestBody"]["content"]["application/json"];
type TenantRenameResponse =
  operations["tenant.rename"]["responses"][200]["content"]["application/json"];
type ProjectPageResponse =
  operations["project.list"]["responses"][200]["content"]["application/json"];
type ProjectCreateRequest =
  operations["project.create"]["requestBody"]["content"]["application/json"];
type ProjectCreateResponse =
  operations["project.create"]["responses"][201]["content"]["application/json"];
type ProjectShowResponse =
  operations["project.show"]["responses"][200]["content"]["application/json"];
type ProjectEditRequest =
  operations["project.edit"]["requestBody"]["content"]["application/json"];
type ProjectEditResponse =
  operations["project.edit"]["responses"][200]["content"]["application/json"];
type ProjectRenameRequest =
  operations["project.rename"]["requestBody"]["content"]["application/json"];
type ProjectRenameResponse =
  operations["project.rename"]["responses"][200]["content"]["application/json"];
type EnvironmentPageResponse =
  operations["environment.list"]["responses"][200]["content"]["application/json"];
type EnvironmentResponse =
  operations["environment.show"]["responses"][200]["content"]["application/json"];
type EnvironmentCreateRequest =
  operations["environment.create"]["requestBody"]["content"]["application/json"];
type EnvironmentTaskAccepted =
  operations["environment.create"]["responses"][202]["content"]["application/json"];
type EnvironmentEditRequest =
  operations["environment.edit"]["requestBody"]["content"]["application/json"];
type EnvironmentEditResponse =
  operations["environment.edit"]["responses"][200]["content"]["application/json"];
type EnvironmentRenameRequest =
  operations["environment.rename"]["requestBody"]["content"]["application/json"];
type EnvironmentRenameResponse =
  operations["environment.rename"]["responses"][200]["content"]["application/json"];
type EnvironmentDeleteResponse =
  operations["environment.delete"]["responses"][202]["content"]["application/json"];
type BackupKeyRotateResponse =
  operations["backup.key.rotate"]["responses"][202]["content"]["application/json"];
type TaskRetryResponse =
  operations["task.retry"]["responses"][202]["content"]["application/json"];
type TaskAbortResponse =
  operations["task.abort"]["responses"][202]["content"]["application/json"];
type BackingRuntimeTaskAccepted =
  operations["backing-service.start"]["responses"][202]["content"]["application/json"];
type ScriptPageResponse =
  operations["script.list"]["responses"][200]["content"]["application/json"];
type ScriptRunResponse =
  operations["script.run"]["responses"][202]["content"]["application/json"];
type EntryPageResponse =
  operations["entry.list"]["responses"][200]["content"]["application/json"];
type EntryResponse =
  operations["entry.edit"]["responses"][200]["content"]["application/json"];
type EntryCreateRequest =
  operations["entry.create"]["requestBody"]["content"]["application/json"];
type EntryEditRequest =
  operations["entry.edit"]["requestBody"]["content"]["application/json"];
type EntryBulkUpsertRequest =
  operations["entry.bulk-upsert"]["requestBody"]["content"]["application/json"];
type EntryBulkUpsertResponse =
  operations["entry.bulk-upsert"]["responses"][202]["content"]["application/json"];
type EntryValueResponse =
  operations["entry.reveal"]["responses"][200]["content"]["application/json"];
type AttachPageResponse =
  operations["attach.list"]["responses"][200]["content"]["application/json"];
type AttachResponse = NonNullable<AttachPageResponse["items"]>[number];
type AttachCreateRequest =
  operations["attach.create"]["requestBody"]["content"]["application/json"];
type AttachTaskAccepted =
  operations["attach.create"]["responses"][202]["content"]["application/json"];
type AttachRenameRequest =
  operations["attach.rename"]["requestBody"]["content"]["application/json"];
type AttachRenameResponse =
  operations["attach.rename"]["responses"][200]["content"]["application/json"];
type AttachFactValueResponse =
  operations["attach.fact.reveal"]["responses"][200]["content"]["application/json"];
type BackingServicePageResponse =
  operations["backing-service.list"]["responses"][200]["content"]["application/json"];
type BackingServiceResponse = NonNullable<
  BackingServicePageResponse["items"]
>[number];
type AgentTaskAccepted =
  operations["agent.join"]["responses"][202]["content"]["application/json"];
type AgentConfigResponse =
  operations["agent.config.show"]["responses"][200]["content"]["application/json"];
type AgentConfigRequest =
  operations["agent.config.set"]["requestBody"]["content"]["application/json"];
type HierarchyTaskAccepted = { task_id: string };
type ComponentTaskAccepted =
  operations["component.enable"]["responses"][202]["content"]["application/json"];
type ComponentConfigResponse =
  operations["component-config.show"]["responses"][200]["content"]["application/json"];
type ComponentConfigMutationResponse =
  operations["component-config.set"]["responses"][200]["content"]["application/json"];
type ReleaseGroupMutationAccepted =
  operations["release-group.remove"]["responses"][202]["content"]["application/json"];
type ReleaseGroupTaskAccepted =
  operations["release-group.deploy"]["responses"][202]["content"]["application/json"];
type ReleaseGroupPageResponse =
  operations["release-group.list"]["responses"][200]["content"]["application/json"];
type ReleaseGroupResponse =
  operations["release-group.show"]["responses"][200]["content"]["application/json"];
export type ReleaseGroupRollbackPreviewResponse =
  operations["release-group.rollback-preview"]["responses"][200]["content"]["application/json"];
type ComponentConfigInput =
  | { zone_ids: string[]; caddyfile_template?: string; alias?: string }
  | {
      zone_ids: string[];
      credential:
        | { mode: "existing"; secret_id: string }
        | { mode: "new"; secret_name: string; token: string };
    }
  | {
      upstream_auto: boolean;
      upstream_resolvers: string[];
      forwarders: { domain: string; resolvers: string[] }[];
      tailnet_delegation: boolean;
      corefile_template: string;
    };

function tenantFromAPI(tenant: TenantCreateResponse): Tenant {
  return {
    id: tenant.id,
    slug: tenant.slug,
    name: tenant.name,
    description: tenant.description,
    deletionTaskId: tenant.deletion_task_id,
  };
}

type AttachCreateInput = {
  serviceId: string;
  backingServiceId: string;
  name?: string;
  credential: { mode: "new" } | { mode: "existing"; attachId: string };
  grantAttachIds?: string[];
};

async function listAllTenants(signal: AbortSignal): Promise<Tenant[]> {
  const tenants: Tenant[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({ limit: "200" });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<TenantPageResponse>(
      `/tenants?${query}`,
      200,
      { signal },
    );
    tenants.push(...(page.items ?? []).map(tenantFromAPI));
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return tenants;
}

function projectFromAPI(
  project: ProjectCreateResponse | ProjectShowResponse,
): Project {
  if (project.kind !== "tenant" && project.kind !== "backing") {
    throw new Error(`Controller returned unknown project kind ${project.kind}`);
  }
  return {
    id: project.id,
    tenantId: project.tenant_id ?? null,
    slug: project.slug,
    name: project.name,
    description: project.description,
    kind: project.kind,
    deletionTaskId: project.deletion_task_id,
  };
}

async function listAllEntries(
  environmentId: string,
  signal?: AbortSignal,
): Promise<EnvironmentEntry[]> {
  const entries: EnvironmentEntry[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({
      environment: environmentId,
      limit: "200",
    });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<EntryPageResponse>(
      `/entries?${query}`,
      200,
      { signal },
    );
    entries.push(...(page.items ?? []).map(entryFromAPI));
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return entries;
}

async function listAllScripts(
  environmentId: string,
  signal?: AbortSignal,
): Promise<Script[]> {
  const scripts: Script[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({
      environment: environmentId,
      limit: "200",
    });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<ScriptPageResponse>(
      `/scripts?${query}`,
      200,
      { signal },
    );
    scripts.push(...(page.items ?? []).map(scriptFromAPI));
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return scripts;
}

async function listAllReleaseGroups(
  environmentId: string,
  services: Service[],
  signal?: AbortSignal,
): Promise<ReleaseGroup[]> {
  const groups: ReleaseGroup[] = [];
  const names = new Map(services.map((service) => [service.id, service.name]));
  let cursor = "";
  do {
    const query = new URLSearchParams({
      environment_id: environmentId,
      limit: "200",
    });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<ReleaseGroupPageResponse>(
      `/release-groups?${query}`,
      200,
      { signal },
    );
    groups.push(
      ...(page.items ?? []).map((group) => ({
        id: group.id,
        name: group.name,
        services: (group.service_ids ?? []).map((id) => names.get(id) ?? id),
        order: (group.order ?? []).map((id) => names.get(id) ?? id),
        tag: group.tag,
        onFailure:
          group.on_failure === "leave_active"
            ? ("leave_active" as const)
            : ("switch_back" as const),
      })),
    );
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return groups;
}

function attachHealth(status: string): HealthState {
  if (status === "ready") return "healthy";
  if (status === "failed") return "failed";
  if (status === "detached") return "stopped";
  return "pending";
}

async function revealAttachFactValue(
  attachId: string,
  key: string,
  grantAttachId?: string,
  signal?: AbortSignal,
): Promise<string> {
  const query = new URLSearchParams();
  if (grantAttachId) query.set("grant_attach_id", grantAttachId);
  const suffix = query.size > 0 ? `?${query}` : "";
  const response = await controllerRequest<AttachFactValueResponse>(
    `/attaches/${encodeURIComponent(attachId)}/facts/${encodeURIComponent(key)}${suffix}`,
    200,
    { signal },
  );
  return response.value;
}

async function attachFromAPI(
  attach: AttachResponse,
  services: Service[],
  signal?: AbortSignal,
): Promise<Attach> {
  const factSets = (attach.fact_sets ?? []).map((set) => ({
    grantAttachId: set.grant_attach_id,
    facts: (set.facts ?? []).map((fact) => ({
      key: fact.key,
      secret: fact.secret,
    })),
  }));
  const ready = attach.status === "ready";
  const revealSuffix = async (
    set: (typeof factSets)[number] | undefined,
    suffix: string,
  ) => {
    const fact = set?.facts.find(
      (candidate) => !candidate.secret && candidate.key.endsWith(suffix),
    );
    if (!ready || !fact) return "";
    return revealAttachFactValue(
      attach.id,
      fact.key,
      set?.grantAttachId,
      signal,
    );
  };
  const own = factSets.find((set) => !set.grantAttachId);
  const [database, role, ...grants] = await Promise.all([
    revealSuffix(own, "_DATABASE"),
    revealSuffix(own, "_ROLE"),
    ...factSets
      .filter((set) => set.grantAttachId)
      .map((set) => revealSuffix(set, "_DATABASE")),
  ]);
  const serviceId = attach.service_id;
  return {
    id: attach.id,
    name: attach.name,
    backingProjectId: attach.backing_project_id,
    backingServiceId: attach.backing_service_id,
    backingEnvironmentId: attach.backing_environment_id,
    backingNetworkId: attach.backing_network_id,
    serviceId,
    credential: {
      mode: attach.credential.mode,
      attachId: attach.credential.attach_id,
    },
    grantAttachIds: [...(attach.grant_attach_ids ?? [])],
    factSets,
    projectId: attach.backing_project_id,
    database: database || "—",
    role,
    service:
      services.find((service) => service.id === serviceId)?.name ?? serviceId,
    grants: grants.filter(Boolean),
    status: attachHealth(attach.status),
  };
}

async function listAllAttaches(
  environmentId: string,
  services: Service[],
  signal?: AbortSignal,
): Promise<Attach[]> {
  const attaches: AttachResponse[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({
      environment: environmentId,
      limit: "200",
    });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<AttachPageResponse>(
      `/attaches?${query}`,
      200,
      { signal },
    );
    attaches.push(...(page.items ?? []));
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return Promise.all(
    attaches.map((attach) => attachFromAPI(attach, services, signal)),
  );
}

async function listAllEnvironments(
  projectId: string,
  signal?: AbortSignal,
): Promise<Environment[]> {
  const environments: Environment[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({ project: projectId, limit: "200" });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<EnvironmentPageResponse>(
      `/environments?${query}`,
      200,
      { signal },
    );
    for (const item of page.items ?? []) {
      const environment = environmentFromAPI(item);
      environments.push(environment);
    }
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return Promise.all(
    environments.map(async (environment) => {
      const [zones, routes, services, entries, scripts, volumes, components] =
        await Promise.all([
          listAllZones(environment.id, signal),
          listAllRoutes(environment.id, signal),
          listAllServices(environment.id, signal),
          listAllEntries(environment.id, signal),
          listAllScripts(environment.id, signal),
          listAllVolumes(controllerRequest, environment.id, signal),
          listAllComponents(environment.id, signal),
        ]);
      const [attaches, deploys, releaseGroups] = await Promise.all([
        listAllAttaches(environment.id, services, signal),
        listAllReleases(environment.id, services, signal),
        listAllReleaseGroups(environment.id, services, signal),
      ]);
      return {
        ...environment,
        ...projectReleaseSummary(deploys),
        zones,
        routes,
        services,
        entries,
        scripts,
        attaches,
        deploys,
        releaseGroups,
        volumes,
        components,
      };
    }),
  );
}

async function listAllTenantProjects(signal: AbortSignal): Promise<Project[]> {
  const projects: Project[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({ kind: "tenant", limit: "200" });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<ProjectPageResponse>(
      `/projects?${query}`,
      200,
      { signal },
    );
    projects.push(...(page.items ?? []).map(projectFromAPI));
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return Promise.all(
    projects.map(async (project) => ({
      ...project,
      environments: await listAllEnvironments(project.id, signal),
    })),
  );
}

function backingConsumers(
  backingProjectId: string,
  projects: Project[],
  tenants: Tenant[],
): NonNullable<Project["consumers"]> {
  return projects.flatMap((project) => {
    const tenant = tenants.find(
      (candidate) => candidate.id === project.tenantId,
    );
    return (project.environments ?? []).flatMap((environment) =>
      environment.attaches
        .filter((attach) => attach.backingProjectId === backingProjectId)
        .flatMap((attach) => {
          const connectionFactKey = attach.factSets
            .find((set) => !set.grantAttachId)
            ?.facts.find((fact) => fact.key.endsWith("_URL"))?.key;
          return [
            {
              tenant: tenant?.slug ?? project.tenantId ?? "",
              project: project.slug,
              environment: environment.name,
              service: attach.service,
              attachId: attach.id,
              database: attach.database,
              role: attach.role,
              connectionFactKey,
            },
          ];
        }),
    );
  });
}

function backingConsumerRevision(
  projects: Project[],
  tenants: Tenant[],
): string {
  return JSON.stringify({
    tenants: tenants.map((tenant) => [tenant.id, tenant.slug]),
    projects: projects.map((project) => [
      project.id,
      project.tenantId ?? "",
      project.slug,
      (project.environments ?? []).map((environment) => [
        environment.id,
        environment.name,
        environment.attaches.map((attach) => [
          attach.id,
          attach.backingProjectId,
          attach.service,
          attach.database,
          attach.role,
          attach.factSets.map((set) => [
            set.grantAttachId ?? "",
            set.facts
              .filter((fact) => fact.key.endsWith("_URL"))
              .map((fact) => fact.key),
          ]),
        ]),
      ]),
    ]),
  });
}

async function listAllBackingProjects(
  tenantProjects: Project[],
  tenants: Tenant[],
  signal: AbortSignal,
): Promise<Project[]> {
  const facades: BackingServiceResponse[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({ limit: "200" });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<BackingServicePageResponse>(
      `/backing-services?${query}`,
      200,
      { signal },
    );
    facades.push(...(page.items ?? []));
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return Promise.all(
    facades.map(async (facade) => {
      const [
        projectResponse,
        environmentResponse,
        serviceResponse,
        zones,
        entries,
      ] = await Promise.all([
        controllerRequest<ProjectShowResponse>(
          `/projects/${encodeURIComponent(facade.project_id)}`,
          200,
          { signal },
        ),
        controllerRequest<EnvironmentResponse>(
          `/environments/${encodeURIComponent(facade.environment_id)}`,
          200,
          { signal },
        ),
        controllerRequest<ServiceShowResponse>(
          `/services/${encodeURIComponent(facade.service_id)}`,
          200,
          { signal },
        ),
        listAllZones(facade.environment_id, signal),
        listAllEntries(facade.environment_id, signal),
      ]);
      const service = {
        ...serviceFromAPI(serviceResponse),
        authentication: facade.authentication,
      };
      const environment = {
        ...environmentFromAPI(environmentResponse),
        zones,
        services: [service],
        entries,
        routes: [],
        attaches: [],
        components: [],
        backup: undefined,
      };
      const project = projectFromAPI(projectResponse);
      return {
        ...project,
        environments: [environment],
        status: environment.status,
        consumers: backingConsumers(facade.project_id, tenantProjects, tenants),
      };
    }),
  );
}

function refreshReleaseGroupTags(environment: Environment) {
  for (const group of environment.releaseGroups) {
    const activeTags = group.order.map(
      (service) =>
        environment.deploys.find(
          (record) => record.service === service && record.status === "active",
        )?.tag,
    );
    const distinct = new Set(activeTags);
    group.tag =
      activeTags.every(Boolean) && distinct.size === 1
        ? activeTags[0]
        : undefined;
  }
}

type State = ReusableSecretState &
  ConnectorState &
  RunnerState & {
    // UI preference: typed confirmation before revealing a secret value
    requireRevealConfirm: boolean;
    tenants: Tenant[];
    tenantsLoading: boolean;
    tenantError: string | null;
    tenantProjects: Project[];
    projectsLoading: boolean;
    projectError: string | null;
    environmentDeletionRevision: number;
    backingProjects: Project[];
    backingProjectsLoading: boolean;
    backingProjectError: string | null;
    activity: ActivityEntry[];
    taskJournals: Record<string, TaskJournalState>;
    platform: PlatformInfra;
    platformComponentsLoading: boolean;
    platformComponentError: string | null;
    managedConfigFiles: ManagedConfigFile[];
    managedConfigLoading: boolean;
    managedConfigError: string | null;
    agentsLoading: boolean;
    agentError: string | null;
    agentConfig: AgentConfigResponse | null;
    agentConfigLoading: boolean;
    agentConfigError: string | null;
  };

function seed(): State {
  let requireRevealConfirm = false;
  try {
    requireRevealConfirm =
      typeof window !== "undefined" &&
      localStorage.getItem("groundplane-reveal-confirm") === "1";
  } catch {
    /* private mode */
  }
  return structuredClone({
    requireRevealConfirm,
    tenants: [],
    tenantsLoading: true,
    tenantError: null,
    tenantProjects: [],
    projectsLoading: true,
    projectError: null,
    environmentDeletionRevision: 0,
    backingProjects: [],
    backingProjectsLoading: true,
    backingProjectError: null,
    runners: [],
    runnersLoading: true,
    runnerError: null,
    connectors: [],
    connectorsLoading: true,
    connectorError: null,
    reusableSecrets: [],
    reusableSecretsLoading: true,
    secretError: null,
    activity: [],
    taskJournals: { all: emptyTaskJournal() },
    platform: { ...seedPlatform, components: [], agents: [] },
    platformComponentsLoading: true,
    platformComponentError: null,
    managedConfigFiles: [],
    managedConfigLoading: false,
    managedConfigError: null,
    agentsLoading: true,
    agentError: null,
    agentConfig: null,
    agentConfigLoading: true,
    agentConfigError: null,
  });
}

type StoreContext = State &
  BlueprintActions &
  ReusableSecretActions &
  ConnectorActions &
  RunnerActions &
  ServiceActions &
  ReturnType<typeof useControllerPlatform> &
  ReturnType<typeof useBackupStore> & {
    adapters: typeof seedAdapters;
    platform: typeof seedPlatform;
    watchLogs: (
      target: LogTarget,
      options: { tail: number; follow: boolean; signal: AbortSignal },
      onEvent: (event: TransientLogEvent) => void,
    ) => Promise<void>;
    setRequireRevealConfirm: (v: boolean) => void;
    refreshPlatformComponents: (
      signal?: AbortSignal,
    ) => Promise<PlatformInfra["components"]>;
    refreshComponentConfig: (
      componentId: string,
      signal?: AbortSignal,
    ) => Promise<ManagedConfigFile[]>;
    refreshEnvironmentComponents: (
      environmentId: string,
      signal?: AbortSignal,
    ) => Promise<EnvironmentComponent[]>;
    refreshEnvironmentReleases: (
      environmentId: string,
      signal?: AbortSignal,
    ) => Promise<void>;
    refreshEnvironmentServices: (
      environmentId: string,
      signal?: AbortSignal,
    ) => Promise<void>;
    refreshAgents: (signal?: AbortSignal) => Promise<PlatformInfra["agents"]>;
    setAgentConfig: (
      agentId: string,
      config: AgentConfigRequest,
    ) => Promise<AgentConfigResponse>;
    joinAgent: () => Promise<AgentTaskAccepted>;
    updateAgent: (agentId: string, image: string) => Promise<AgentTaskAccepted>;
    removeAgent: (agentId: string) => Promise<AgentTaskAccepted>;
    // selectors
    getTenant: (slug: string) => Tenant | undefined;
    getProject: (tenantSlug: string, slug: string) => Project | undefined;
    getProjectById: (id: string) => Project | undefined;
    getBackingProject: (id: string) => Project | undefined;
    getEnvironment: (
      tenantSlug: string,
      projectSlug: string,
      envName: string,
    ) => Environment | undefined;
    getTaskJournal: (scope: TaskJournalScope) => TaskJournalState;
    loadTaskJournal: (
      surface: TaskJournalSurface,
      scope: TaskJournalScope,
      cursor?: string,
    ) => Promise<void>;
    getTaskJournalDetail: (
      taskId: string,
      signal?: AbortSignal,
    ) => Promise<ActivityEntry>;
    getEnvironmentDeletionFailure: (
      environmentId: string,
    ) => EnvironmentDeletionFailure | null;
    refreshEnvironmentDeletion: (environmentId: string) => Promise<void>;
    isEnvironmentDeletionPending: (environmentId: string) => boolean;
    // mutations
    retryTask: (taskId: string) => Promise<string>;
    commitDeploy: (
      envId: string,
      service: string,
      tag: string,
      strategy: Service["strategy"],
    ) => Promise<string>;
    commitRollback: (
      envId: string,
      service: string,
      tag: string,
    ) => Promise<string>;
    addReleaseGroup: (
      envId: string,
      group: ReleaseGroup,
    ) => Promise<ReleaseGroup>;
    updateReleaseGroup: (
      envId: string,
      groupId: string,
      patch: Pick<ReleaseGroup, "name" | "services" | "order" | "onFailure">,
    ) => Promise<ReleaseGroup>;
    removeReleaseGroup: (envId: string, groupId: string) => Promise<string>;
    deployReleaseGroup: (
      envId: string,
      groupId: string,
      tag?: string,
    ) => Promise<string>;
    previewReleaseGroupRollback: (
      envId: string,
      groupId: string,
      tag?: string,
    ) => Promise<ReleaseGroupRollbackPreviewResponse>;
    rollbackReleaseGroup: (
      envId: string,
      groupId: string,
      tag: string | undefined,
      previewRevision: string,
    ) => Promise<string>;
    addTenant: (t: {
      slug: string;
      name: string;
      description: string;
    }) => Promise<Tenant>;
    updateTenant: (
      slug: string,
      patch: { name: string; description: string },
    ) => Promise<Tenant>;
    renameTenant: (slug: string, nextSlug: string) => Promise<Tenant>;
    removeTenant: (tenantId: string) => Promise<string>;
    addProject: (p: {
      tenantId: string;
      slug: string;
      name: string;
      description: string;
    }) => Promise<Project>;
    editProject: (projectId: string, name: string) => Promise<Project>;
    renameProject: (projectId: string, slug: string) => Promise<Project>;
    deleteProject: (projectId: string) => Promise<string>;
    addEnvironment: (
      projectId: string,
      name: string,
      networkPool: string,
    ) => Promise<EnvironmentTaskAccepted>;
    editEnvironment: (
      envId: string,
      networkPool: string,
    ) => Promise<Environment>;
    renameEnvironment: (envId: string, name: string) => Promise<Environment>;
    getTask: (taskId: string, signal?: AbortSignal) => Promise<TaskResponse>;
    abortTask: (taskId: string) => Promise<void>;
    watchTaskEvents: (
      taskId: string,
      onEvent: (event: TaskEventResponse) => void,
      onMalformed: (message: string) => void,
    ) => () => void;
    deleteEnvironment: (envId: string) => Promise<string>;
    addZone: (
      envId: string,
      input: { name: string; subnet: string; internal: boolean },
    ) => Promise<Zone>;
    getZone: (zoneId: string) => Promise<Zone>;
    getZoneRemovalImpact: (
      zoneId: string,
    ) => Promise<ZoneRemovalImpactResponse>;
    removeZone: (
      envId: string,
      zoneId: string,
      impactToken: string,
    ) => Promise<string>;
    addRoute: (
      envId: string,
      route: Omit<Route, "id" | "environmentId" | "status">,
    ) => Promise<Route>;
    getRoute: (routeId: string) => Promise<Route>;
    updateRoute: (
      envId: string,
      routeId: string,
      patch: Pick<Route, "exposure">,
    ) => Promise<Route>;
    removeRoute: (envId: string, routeId: string) => Promise<string>;
    addVolume: (
      envId: string,
      input: { slug: string; key?: string },
    ) => Promise<Volume>;
    getVolume: (volumeId: string) => Promise<Volume>;
    updateVolume: (
      envId: string,
      volumeId: string,
      patch: Pick<Volume, "slug">,
    ) => Promise<Volume>;
    getVolumeDeletionImpact: (
      volumeId: string,
      cursor?: string,
      limit?: number,
    ) => Promise<VolumeDeletionImpactPage>;
    removeVolume: (
      envId: string,
      volumeId: string,
      impactToken: string,
      confirmKey: string,
    ) => Promise<string>;
    addScript: (envId: string, script: ScriptInput) => Promise<Script>;
    updateScript: (
      envId: string,
      scriptId: string,
      patch: ScriptPatch,
    ) => Promise<Script>;
    runScript: (scriptId: string) => Promise<string>;
    removeScript: (envId: string, scriptId: string) => Promise<string>;
    addAttach: (envId: string, input: AttachCreateInput) => Promise<string>;
    renameAttach: (
      envId: string,
      attachId: string,
      name: string,
    ) => Promise<void>;
    removeAttach: (envId: string, attachId: string) => Promise<string>;
    revealAttachFact: (
      attachId: string,
      key: string,
      grantAttachId?: string,
    ) => Promise<string>;
    addEntry: (
      envId: string,
      input: Omit<EntryCreateRequest, "environment_id">,
    ) => Promise<EnvironmentEntry>;
    bulkUpsertEntries: (
      envId: string,
      input: Omit<EntryBulkUpsertRequest, "environment_id">,
    ) => Promise<EntryBulkUpsertResponse>;
    updateEntry: (
      envId: string,
      entryId: string,
      input: EntryEditRequest,
    ) => Promise<EnvironmentEntry>;
    removeEntry: (envId: string, entryId: string) => Promise<string>;
    revealEntry: (entryId: string) => Promise<string>;
    runBackingRuntimeAction: (
      id: string,
      action: "start" | "stop" | "destroy",
    ) => Promise<string>;
    addBackingProject: (
      input: BackingServiceCreateRequest,
    ) => Promise<BackingServiceCreatedResponse>;
    setComponentEnabled: (
      componentId: string,
      enabled: boolean,
      config?: ComponentConfigInput,
    ) => Promise<string>;
    reconcileEnvironmentComponent: (componentId: string) => Promise<string>;
    updateComponentConfig: (
      componentId: string,
      config: ComponentConfigInput,
    ) => Promise<string | null>;
  };

const Ctx = createContext<StoreContext | null>(null);

export function StoreProvider({ children }: { children: React.ReactNode }) {
  const controllerPlatform = useControllerPlatform(
    controllerRequest,
    controllerUpdateRejected,
  );
  const providerActive = useRef(true);
  const environmentTaskControllers = useRef(new Set<AbortController>());
  const [state, setState] = useState<State>(seed);
  const pendingZoneRemovals = useRef(
    new Map<string, { envId: string; zoneId: string }>(),
  );
  const taskEventSources = useRef(new Set<EventSource>());
  const environmentMutationIntents = useRef(loadEnvironmentMutationIntents());
  const taskJournalEpochs = useRef(new Map<string, number>());

  const update = useCallback((fn: (draft: State) => void) => {
    if (!providerActive.current) return;
    setState((prev) => {
      if (!providerActive.current) return prev;
      const next = structuredClone(prev);
      fn(next);
      return next;
    });
  }, []);
  const refreshEnvironmentServices = useCallback(
    async (environmentId: string, signal?: AbortSignal) => {
      const refreshed = await listAllServices(environmentId, signal);
      if (signal?.aborted) return;
      update((draft) => {
        applyServiceObservations(
          [...draft.tenantProjects, ...draft.backingProjects],
          environmentId,
          refreshed,
        );
      });
    },
    [update],
  );
  const persistEnvironmentMutationIntent = useCallback(
    (intent: EnvironmentMutationIntent) => {
      environmentMutationIntents.current.set(intent.key, intent);
      persistEnvironmentMutationIntents(environmentMutationIntents.current);
    },
    [],
  );
  const clearEnvironmentMutationIntent = useCallback((key: string) => {
    environmentMutationIntents.current.delete(key);
    persistEnvironmentMutationIntents(environmentMutationIntents.current);
  }, []);
  const {
    refreshRunners,
    createRunner,
    renameRunner,
    retryRunner,
    removeRunner,
  } = useRunnerStore(update);

  const requestEnvironmentTask = useCallback(
    (taskId: string, signal?: AbortSignal) =>
      controllerRequest<TaskResponse>(
        `/tasks/${encodeURIComponent(taskId)}`,
        200,
        { signal },
      ),
    [],
  );
  const deleteEnvironmentResource = useCallback(
    (
      resource: "environments" | "services" | "routes" | "entries" | "scripts",
      resourceId: string,
      idempotencyKey: string,
    ) =>
      controllerRequest<EnvironmentDeleteResponse>(
        `/${resource}/${encodeURIComponent(resourceId)}`,
        202,
        { method: "DELETE", idempotencyKey },
      ),
    [],
  );
  const retryEnvironmentResource = useCallback(
    (taskId: string, idempotencyKey: string) =>
      controllerRequest<TaskRetryResponse>(
        `/tasks/${encodeURIComponent(taskId)}/retry`,
        202,
        { method: "POST", idempotencyKey },
      ),
    [],
  );
  const environmentLifecycle = useEnvironmentLifecycle<State>({
    active: providerActive,
    update,
    requestTask: requestEnvironmentTask,
    deleteResource: deleteEnvironmentResource,
    retryResource: retryEnvironmentResource,
    listEnvironments: listAllEnvironments,
    listServices: listAllServices,
    listRoutes: listAllRoutes,
    listEntries: listAllEntries,
    listScripts: listAllScripts,
  });
  const {
    pendingResourceRemovals,
    requestResourceRemovalTask,
    monitorResourceRemoval,
    reconcileResourceRemoval,
    dispatchResourceRemoval,
    retryResourceRemoval,
    waitForResourceRemoval,
    nextEnvironmentGeneration,
    settleEnvironmentMutation,
    shouldPreserveEnvironmentOnLoad,
    getEnvironmentDeletionFailure,
    refreshEnvironmentDeletion,
    isEnvironmentDeletionPending,
    observeEnvironmentDeletionTasks,
    environmentGenerations,
  } = environmentLifecycle;

  const assertEnvironmentMutable = useCallback(
    (environmentId: string, action: string) => {
      if (
        environmentDeletionGuard(
          [...state.tenantProjects, ...state.backingProjects],
          environmentId,
          isEnvironmentDeletionPending,
        )
      ) {
        throw new Error(
          `Environment ${environmentId} deletion is in progress; ${action} is disabled`,
        );
      }
    },
    [isEnvironmentDeletionPending, state.backingProjects, state.tenantProjects],
  );

  const requestBackupKeyRotation = useCallback(
    (environmentId: string) =>
      controllerRequest<BackupKeyRotateResponse>(
        `/environments/${encodeURIComponent(environmentId)}/rotate-key`,
        202,
        { method: "POST" },
      ),
    [],
  );
  const backupStore = useBackupStore({
    active: providerActive,
    request: controllerRequest,
    requestKeyRotation: requestBackupKeyRotation,
    assertEnvironmentMutable,
  });

  useEffect(
    () => () => {
      for (const source of taskEventSources.current) source.close();
      taskEventSources.current.clear();
      for (const controller of environmentTaskControllers.current)
        controller.abort();
      environmentTaskControllers.current.clear();
    },
    [],
  );

  const refreshPlatformComponents = useCallback(
    async (signal?: AbortSignal) => {
      const hydrated = hydratePlatformComponents(
        await listPlatformComponents(signal),
      );
      update((draft) => {
        draft.platform.components = hydrated.components;
        if (hydrated.dns) draft.platform.dns = hydrated.dns;
        draft.platformComponentsLoading = false;
        draft.platformComponentError = null;
      });
      return hydrated.components;
    },
    [update],
  );

  const refreshComponentConfig = useCallback(
    async (componentId: string, signal?: AbortSignal) => {
      update((draft) => {
        draft.managedConfigLoading = true;
        draft.managedConfigError = null;
      });
      try {
        const response = await controllerRequest<ComponentConfigResponse>(
          `/components/${encodeURIComponent(componentId)}/config`,
          200,
          { signal },
        );
        const files = response.managed_files.map((file) => ({
          path: file.path,
          template: file.template,
          rendered: file.rendered,
        }));
        update((draft) => {
          draft.managedConfigFiles = files;
          draft.managedConfigLoading = false;
          draft.managedConfigError = null;
        });
        return files;
      } catch (error) {
        update((draft) => {
          draft.managedConfigLoading = false;
          draft.managedConfigError =
            error instanceof Error
              ? error.message
              : "Unable to load managed Component config";
        });
        throw error;
      }
    },
    [update],
  );

  const refreshEnvironmentComponents = useCallback(
    async (environmentId: string, signal?: AbortSignal) => {
      const components = await listAllComponents(environmentId, signal);
      update((draft) => {
        for (const project of [
          ...draft.tenantProjects,
          ...draft.backingProjects,
        ]) {
          const environment = project.environments?.find(
            (candidate) => candidate.id === environmentId,
          );
          if (!environment) continue;
          environment.components = components;
          return;
        }
      });
      return components;
    },
    [update],
  );

  const refreshEnvironmentReleases = useCallback(
    async (environmentId: string, signal?: AbortSignal) => {
      const environment = findEnvironment(state, environmentId);
      if (!environment)
        throw new Error(`Environment ${environmentId} is not loaded`);
      try {
        const [deploys, releaseGroups] = await Promise.all([
          listAllReleases(environmentId, environment.services, signal),
          listAllReleaseGroups(environmentId, environment.services, signal),
        ]);
        update((draft) => {
          const current = findEnvironment(draft, environmentId);
          if (!current) return;
          Object.assign(current, projectReleaseSummary(deploys));
          current.deploys = deploys;
          current.releaseGroups = releaseGroups;
          refreshReleaseGroupTags(current);
        });
      } catch (error) {
        update((draft) => {
          draft.projectError =
            error instanceof Error
              ? error.message
              : "Unable to refresh release state";
        });
        throw error;
      }
    },
    [state, update],
  );

  const refreshAgents = useCallback(
    async (signal?: AbortSignal) => {
      const agents = await listAllAgents(signal);
      update((draft) => {
        draft.platform.agents = agents;
        draft.agentsLoading = false;
        draft.agentError = null;
      });
      return agents;
    },
    [update],
  );

  const reusableSecretProjectIds = useMemo(
    () =>
      state.tenantProjects
        .map((project) => project.id)
        .sort()
        .join(","),
    [state.tenantProjects],
  );

  const refreshReusableSecrets = useSecretRefresh(
    reusableSecretProjectIds,
    update,
  );

  const connectorEnvironmentIds = useMemo(
    () =>
      [...state.tenantProjects, ...state.backingProjects]
        .flatMap((project) => project.environments ?? [])
        .map((environment) => environment.id)
        .sort()
        .join(","),
    [state.backingProjects, state.tenantProjects],
  );

  const backingConsumerStateRevision = backingConsumerRevision(
    state.tenantProjects,
    state.tenants,
  );
  const backingConsumerInput = useMemo(
    () => ({
      projects: state.tenantProjects,
      tenants: state.tenants,
    }),
    [backingConsumerStateRevision],
  );

  useEffect(() => {
    const controller = new AbortController();
    void refreshPlatformComponents(controller.signal).catch(
      (error: unknown) => {
        if (controller.signal.aborted) return;
        update((draft) => {
          draft.platformComponentsLoading = false;
          draft.platformComponentError =
            error instanceof Error
              ? error.message
              : "Unable to load Platform Components";
        });
      },
    );
    return () => controller.abort();
  }, [refreshPlatformComponents, update]);

  useEffect(() => {
    const controller = new AbortController();
    void listAllTenants(controller.signal).then(
      (tenants) =>
        update((draft) => {
          draft.tenants = tenants;
          draft.tenantsLoading = false;
          draft.tenantError = null;
        }),
      (error: unknown) => {
        if (controller.signal.aborted) return;
        update((draft) => {
          draft.tenantsLoading = false;
          draft.tenantError =
            error instanceof Error ? error.message : "Unable to load tenants";
        });
      },
    );
    return () => controller.abort();
  }, [update]);

  useEffect(() => {
    if (state.projectsLoading || state.tenantsLoading) return;
    const loadGenerations = environmentGenerationSnapshot(
      state.backingProjects,
      environmentGenerations.current,
    );
    const controller = new AbortController();
    void listAllBackingProjects(
      backingConsumerInput.projects,
      backingConsumerInput.tenants,
      controller.signal,
    ).then(
      (projects) => {
        observeEnvironmentDeletionTasks(projects);
        update((draft) => {
          draft.backingProjects = mergeEnvironmentProjectLoads(
            draft.backingProjects,
            projects,
            loadGenerations,
            shouldPreserveEnvironmentOnLoad,
          );
          draft.backingProjectsLoading = false;
          draft.backingProjectError = null;
        });
      },
      (error: unknown) => {
        if (controller.signal.aborted) return;
        update((draft) => {
          draft.backingProjects = [];
          draft.backingProjectsLoading = false;
          draft.backingProjectError =
            error instanceof Error
              ? error.message
              : "Unable to load backing services";
        });
      },
    );
    return () => controller.abort();
  }, [
    backingConsumerInput,
    observeEnvironmentDeletionTasks,
    shouldPreserveEnvironmentOnLoad,
    state.projectsLoading,
    state.tenantsLoading,
    update,
  ]);

  useEffect(() => {
    const controller = new AbortController();
    void refreshAgents(controller.signal).then(
      () => undefined,
      (error: unknown) => {
        if (controller.signal.aborted) return;
        update((draft) => {
          draft.agentsLoading = false;
          draft.agentError =
            error instanceof Error ? error.message : "Unable to load Agents";
        });
      },
    );
    return () => controller.abort();
  }, [refreshAgents, update]);

  const localAgentID = state.platform.agents[0]?.id ?? "";

  useEffect(() => {
    if (state.agentsLoading) return;
    if (!localAgentID) {
      update((draft) => {
        draft.agentConfig = null;
        draft.agentConfigLoading = false;
        draft.agentConfigError = null;
      });
      return;
    }
    const controller = new AbortController();
    const path = `/agents/${encodeURIComponent(localAgentID)}/config`;
    void controllerRequest<AgentConfigResponse>(path, 200, {
      signal: controller.signal,
    }).then(
      (config) =>
        update((draft) => {
          draft.agentConfig = config;
          draft.agentConfigLoading = false;
          draft.agentConfigError = null;
        }),
      (error: unknown) => {
        if (controller.signal.aborted) return;
        update((draft) => {
          draft.agentConfigLoading = false;
          draft.agentConfigError =
            error instanceof Error
              ? error.message
              : "Unable to load Agent config";
        });
      },
    );
    return () => controller.abort();
  }, [localAgentID, state.agentsLoading, update]);

  useEffect(() => {
    const controller = new AbortController();
    const loadGenerations = new Map(environmentGenerations.current);
    void listAllTenantProjects(controller.signal).then(
      (projects) => {
        observeEnvironmentDeletionTasks(projects);
        update((draft) => {
          draft.tenantProjects = projects.map((project) => {
            const currentProject = draft.tenantProjects.find(
              (candidate) => candidate.id === project.id,
            );
            const currentEnvironments = currentProject?.environments ?? [];
            const loadedIds = new Set(
              (project.environments ?? []).map((environment) => environment.id),
            );
            const environments = (project.environments ?? []).flatMap(
              (environment) => {
                const currentGeneration =
                  environmentGenerations.current.get(environment.id) ?? 0;
                if (
                  currentGeneration !==
                    (loadGenerations.get(environment.id) ?? 0) &&
                  !shouldPreserveEnvironmentOnLoad(
                    environment.id,
                    loadGenerations.get(environment.id) ?? 0,
                  )
                )
                  return [];
                return [
                  currentGeneration !==
                  (loadGenerations.get(environment.id) ?? 0)
                    ? (currentEnvironments.find(
                        (candidate) => candidate.id === environment.id,
                      ) ?? environment)
                    : environment,
                ];
              },
            );
            for (const current of currentEnvironments) {
              const currentGeneration =
                environmentGenerations.current.get(current.id) ?? 0;
              if (
                !loadedIds.has(current.id) &&
                currentGeneration !== (loadGenerations.get(current.id) ?? 0) &&
                shouldPreserveEnvironmentOnLoad(
                  current.id,
                  loadGenerations.get(current.id) ?? 0,
                )
              ) {
                environments.push(current);
              }
            }
            return { ...project, environments };
          });
          draft.projectsLoading = false;
          const deletionFailure = projects
            .flatMap((project) => project.environments ?? [])
            .map((environment) => getEnvironmentDeletionFailure(environment.id))
            .find(
              (
                failure,
              ): failure is NonNullable<
                ReturnType<typeof getEnvironmentDeletionFailure>
              > => failure !== null,
            );
          draft.projectError = deletionFailure?.message ?? null;
        });
      },
      (error: unknown) => {
        if (controller.signal.aborted) return;
        update((draft) => {
          draft.projectsLoading = false;
          draft.projectError =
            error instanceof Error ? error.message : "Unable to load projects";
        });
      },
    );
    return () => controller.abort();
  }, [
    observeEnvironmentDeletionTasks,
    environmentGenerations,
    getEnvironmentDeletionFailure,
    shouldPreserveEnvironmentOnLoad,
    update,
  ]);

  useEffect(() => {
    if (state.projectsLoading || state.backingProjectsLoading) return;
    const controller = new AbortController();
    void refreshReusableSecrets(controller.signal).catch(() => undefined);
    return () => controller.abort();
  }, [
    refreshReusableSecrets,
    state.backingProjectsLoading,
    state.projectsLoading,
  ]);

  const { actions: connectorActions, reconcileConnectorRemoval } =
    useConnectorStore(state, update, connectorEnvironmentIds);

  const loadTaskJournal = useCallback<StoreContext["loadTaskJournal"]>(
    async (surface, scope, cursor) => {
      const key = taskJournalKey(scope);
      const epoch = (taskJournalEpochs.current.get(key) ?? 0) + 1;
      taskJournalEpochs.current.set(key, epoch);
      update((draft) => {
        const journal = draft.taskJournals[key] ?? emptyTaskJournal();
        journal.loadError = null;
        journal.failedCursor = null;
        journal.loading = !cursor;
        journal.loadingMore = !!cursor;
        draft.taskJournals[key] = journal;
      });
      try {
        const page = await controllerRequest<TaskPageResponse>(
          `/${surface}?${taskJournalQuery(scope, cursor)}`,
          200,
        );
        const entries = (page.items ?? []).map(taskFromAPI);
        if (taskJournalEpochs.current.get(key) !== epoch) return;
        update((draft) => {
          const journal = draft.taskJournals[key] ?? emptyTaskJournal();
          journal.entries = cursor ? [...journal.entries, ...entries] : entries;
          journal.nextCursor = page.next_cursor ?? null;
          journal.loaded = true;
          journal.loading = false;
          journal.loadingMore = false;
          journal.loadError = null;
          journal.failedCursor = null;
          draft.taskJournals[key] = journal;
        });
      } catch (error) {
        if (taskJournalEpochs.current.get(key) !== epoch) return;
        update((draft) => {
          const journal = draft.taskJournals[key] ?? emptyTaskJournal();
          journal.loaded = true;
          journal.loading = false;
          journal.loadingMore = false;
          journal.loadError =
            error instanceof Error ? error.message : "Unable to load Tasks";
          journal.failedCursor = cursor ?? null;
          draft.taskJournals[key] = journal;
        });
        throw error;
      }
    },
    [update],
  );

  useEffect(() => {
    void loadTaskJournal("tasks", { kind: "all" }).catch(() => undefined);
  }, [loadTaskJournal]);

  const value = useMemo<StoreContext>(() => {
    return {
      ...state,
      ...controllerPlatform,
      ...backupStore,
      adapters: seedAdapters,
      platform: state.platform,
      watchLogs: watchTransientLogs,
      setRequireRevealConfirm: (v) => {
        update((d) => {
          d.requireRevealConfirm = v;
        });
        try {
          localStorage.setItem("groundplane-reveal-confirm", v ? "1" : "0");
        } catch {
          /* private mode */
        }
      },
      refreshPlatformComponents,
      refreshComponentConfig,
      refreshEnvironmentComponents,
      refreshEnvironmentReleases,
      refreshEnvironmentServices,
      refreshAgents,
      setAgentConfig: async (agentId, config) => {
        const path = `/agents/${encodeURIComponent(agentId)}/config`;
        const updated = await controllerRequest<AgentConfigResponse>(
          path,
          200,
          { method: "PUT", body: config },
        );
        update((draft) => {
          draft.agentConfig = updated;
          draft.agentConfigError = null;
        });
        return updated;
      },
      joinAgent: async () => {
        const accepted = await controllerRequest<AgentTaskAccepted>(
          "/agents",
          202,
          { method: "POST" },
        );
        await refreshAgents();
        return accepted;
      },
      updateAgent: async (agentId, image) => {
        const accepted = await controllerRequest<AgentTaskAccepted>(
          `/agents/${encodeURIComponent(agentId)}/update`,
          202,
          { method: "POST", body: { image } },
        );
        await refreshAgents();
        return accepted;
      },
      removeAgent: async (agentId) => {
        const accepted = await controllerRequest<AgentTaskAccepted>(
          `/agents/${encodeURIComponent(agentId)}`,
          202,
          { method: "DELETE" },
        );
        await refreshAgents();
        return accepted;
      },
      getTenant: (slug) => state.tenants.find((t) => t.slug === slug),
      getProject: (tenantSlug, slug) => {
        const tenantId = state.tenants.find(
          (tenant) => tenant.slug === tenantSlug,
        )?.id;
        return state.tenantProjects.find(
          (project) => project.tenantId === tenantId && project.slug === slug,
        );
      },
      getProjectById: (id) => state.tenantProjects.find((p) => p.id === id),
      getBackingProject: (id) => state.backingProjects.find((p) => p.id === id),
      getEnvironment: (tenantSlug, projectSlug, envName) => {
        const tenantId = state.tenants.find(
          (tenant) => tenant.slug === tenantSlug,
        )?.id;
        return state.tenantProjects
          .find(
            (project) =>
              project.tenantId === tenantId && project.slug === projectSlug,
          )
          ?.environments?.find((environment) => environment.name === envName);
      },
      getTaskJournal: (scope) =>
        state.taskJournals[taskJournalKey(scope)] ?? emptyTaskJournal(),
      loadTaskJournal,
      getTaskJournalDetail: async (taskId, signal) =>
        taskFromAPI(
          await controllerRequest<TaskResponse>(
            `/tasks/${encodeURIComponent(taskId)}`,
            200,
            { signal },
          ),
        ),
      getEnvironmentDeletionFailure,
      refreshEnvironmentDeletion,
      isEnvironmentDeletionPending,
      retryTask: (taskId) =>
        retryResourceRemoval(taskId) ??
        controllerRequest<TaskRetryResponse>(
          `/tasks/${encodeURIComponent(taskId)}/retry`,
          202,
          { method: "POST" },
        ).then((accepted) => {
          if (!accepted.task_id)
            throw new Error("Controller response is missing task_id");
          return accepted.task_id;
        }),
      commitDeploy: async (envId, service, tag, strategy) => {
        assertEnvironmentMutable(envId, "deployment");
        const target = findEnvironment(state, envId)?.services.find(
          (candidate) => candidate.name === service,
        );
        if (!target) throw new Error(`Service ${service} no longer exists`);
        const body: operations["service.deploy"]["requestBody"]["content"]["application/json"] =
          {
            tag,
            strategy,
            on_failure: "switch_back",
          };
        const accepted = await controllerRequest<
          operations["service.deploy"]["responses"][202]["content"]["application/json"]
        >(`/services/${encodeURIComponent(target.id)}/deploy`, 202, {
          method: "POST",
          body,
        });
        if (!accepted.task_id)
          throw new Error("Controller response is missing deploy task_id");
        return accepted.task_id;
      },
      commitRollback: async (envId, service, tag) => {
        assertEnvironmentMutable(envId, "rollback");
        const target = findEnvironment(state, envId)?.services.find(
          (candidate) => candidate.name === service,
        );
        if (!target) throw new Error(`Service ${service} no longer exists`);
        const body: operations["service.rollback"]["requestBody"]["content"]["application/json"] =
          { tag };
        const accepted = await controllerRequest<
          operations["service.rollback"]["responses"][202]["content"]["application/json"]
        >(`/services/${encodeURIComponent(target.id)}/rollback`, 202, {
          method: "POST",
          body,
        });
        if (!accepted.task_id)
          throw new Error("Controller response is missing rollback task_id");
        return accepted.task_id;
      },
      addTenant: async (tenant) => {
        const body: TenantCreateRequest = tenant;
        const created = tenantFromAPI(
          await controllerRequest<TenantCreateResponse>("/tenants", 201, {
            method: "POST",
            body,
          }),
        );
        update((draft) => {
          draft.tenants.push(created);
        });
        return created;
      },
      updateTenant: async (slug, patch) => {
        const current = state.tenants.find((tenant) => tenant.slug === slug);
        if (!current) throw new Error(`Tenant ${slug} no longer exists`);
        const body: TenantEditRequest = patch;
        const updated = tenantFromAPI(
          await controllerRequest<TenantEditResponse>(
            `/tenants/${encodeURIComponent(current.id)}`,
            200,
            { method: "PATCH", body },
          ),
        );
        update((draft) => {
          const index = draft.tenants.findIndex(
            (tenant) => tenant.id === updated.id,
          );
          if (index >= 0) draft.tenants[index] = updated;
        });
        return updated;
      },
      renameTenant: async (slug, nextSlug) => {
        const current = state.tenants.find((tenant) => tenant.slug === slug);
        if (!current) throw new Error(`Tenant ${slug} no longer exists`);
        const body: TenantRenameRequest = { slug: nextSlug };
        const renamed = tenantFromAPI(
          await controllerRequest<TenantRenameResponse>(
            `/tenants/${encodeURIComponent(current.id)}/rename`,
            200,
            { method: "POST", body },
          ),
        );
        update((draft) => {
          const index = draft.tenants.findIndex(
            (tenant) => tenant.id === renamed.id,
          );
          if (index >= 0) draft.tenants[index] = renamed;
        });
        return renamed;
      },
      removeTenant: async (tenantId) => {
        const accepted = await controllerRequest<HierarchyTaskAccepted>(
          `/tenants/${encodeURIComponent(tenantId)}`,
          202,
          { method: "DELETE" },
        );
        if (!accepted.task_id)
          throw new Error("Controller response is missing task_id");
        return accepted.task_id;
      },
      addProject: async (project) => {
        const body: ProjectCreateRequest = {
          tenant_id: project.tenantId,
          slug: project.slug,
          name: project.name,
          description: project.description,
        };
        const created = projectFromAPI(
          await controllerRequest<ProjectCreateResponse>("/projects", 201, {
            method: "POST",
            body,
          }),
        );
        update((draft) => {
          draft.tenantProjects.push(created);
        });
        return created;
      },
      editProject: async (projectId, name) => {
        const body: ProjectEditRequest = { name };
        const updated = projectFromAPI(
          await controllerRequest<ProjectEditResponse>(
            `/projects/${encodeURIComponent(projectId)}`,
            200,
            { method: "PATCH", body },
          ),
        );
        update((draft) => {
          const index = draft.tenantProjects.findIndex(
            (project) => project.id === updated.id,
          );
          if (index >= 0) draft.tenantProjects[index] = updated;
        });
        return updated;
      },
      renameProject: async (projectId, slug) => {
        const body: ProjectRenameRequest = { slug };
        const renamed = projectFromAPI(
          await controllerRequest<ProjectRenameResponse>(
            `/projects/${encodeURIComponent(projectId)}/rename`,
            200,
            { method: "POST", body },
          ),
        );
        update((draft) => {
          const index = draft.tenantProjects.findIndex(
            (project) => project.id === renamed.id,
          );
          if (index >= 0) draft.tenantProjects[index] = renamed;
        });
        return renamed;
      },
      deleteProject: async (projectId) => {
        const accepted = await controllerRequest<HierarchyTaskAccepted>(
          `/projects/${encodeURIComponent(projectId)}`,
          202,
          { method: "DELETE" },
        );
        if (!accepted.task_id)
          throw new Error("Controller response is missing task_id");
        return accepted.task_id;
      },
      addEnvironment: async (projectId, name, networkPool) => {
        const body: EnvironmentCreateRequest = {
          project_id: projectId,
          name,
          network_pool: networkPool,
        };
        const key = environmentMutationKey("create", [
          projectId,
          name,
          networkPool,
        ]);
        let intent = environmentMutationIntents.current.get(key) ?? {
          key,
          kind: "create" as const,
          idempotencyKey: `groundplane:${newULID()}`,
          projectId,
          name,
          networkPool,
        };
        persistEnvironmentMutationIntent(intent);
        let observedTaskStatus: TaskResponse["status"] | undefined;
        try {
          const accepted = intent.taskId
            ? { task_id: intent.taskId }
            : await controllerRequest<EnvironmentTaskAccepted>(
                "/environments",
                202,
                {
                  method: "POST",
                  body,
                  idempotencyKey: intent.idempotencyKey,
                },
              );
          if (!accepted.task_id)
            throw new Error("Controller response is missing create task_id");
          if (!intent.taskId) {
            intent = { ...intent, taskId: accepted.task_id };
            persistEnvironmentMutationIntent(intent);
          }
          const observationController = new AbortController();
          environmentTaskControllers.current.add(observationController);
          let task: TaskResponse;
          try {
            task = await observeEnvironmentTask(
              requestEnvironmentTask,
              accepted.task_id,
              providerActive,
              observationController.signal,
            );
          } finally {
            environmentTaskControllers.current.delete(observationController);
          }
          observedTaskStatus = task.status;
          if (
            task.id !== accepted.task_id ||
            task.type !== "create" ||
            !task.target
          ) {
            clearEnvironmentMutationIntent(key);
            throw new Error(
              `Controller create Task ${accepted.task_id} did not identify an Environment target`,
            );
          }
          if (task.project_id !== projectId) {
            clearEnvironmentMutationIntent(key);
            throw new Error(
              `Controller create Task ${accepted.task_id} belongs to a different Project`,
            );
          }
          if (task.status !== "completed") {
            clearEnvironmentMutationIntent(key);
            throw new Error(
              `Controller create Task ${accepted.task_id} ${task.status}`,
            );
          }
          const environments = await listAllEnvironments(projectId);
          const created = environments.find(
            (environment) =>
              environment.id === task.target &&
              environment.projectId === projectId,
          );
          if (!created) {
            clearEnvironmentMutationIntent(key);
            throw new Error(
              `Controller did not publish Environment for task ${accepted.task_id}`,
            );
          }
          const generation = nextEnvironmentGeneration(created.id, "create");
          update((draft) => {
            if (
              (environmentGenerations.current.get(created.id) ?? 0) !==
              generation
            )
              return;
            const project = draft.tenantProjects.find(
              (candidate) => candidate.id === projectId,
            );
            if (!project) return;
            project.environments = project.environments ?? [];
            const index = project.environments.findIndex(
              (environment) => environment.id === created.id,
            );
            if (index >= 0) project.environments[index] = created;
            else project.environments.push(created);
          });
          if (created.provisioningState === "failed") {
            settleEnvironmentMutation(created.id, generation, "failed");
            clearEnvironmentMutationIntent(key);
            throw new Error(
              `Controller Environment ${created.id} provisioning failed`,
            );
          }
          settleEnvironmentMutation(created.id, generation, "succeeded");
          clearEnvironmentMutationIntent(key);
          return accepted;
        } catch (error) {
          if (observedTaskStatus && observedTaskStatus !== "completed")
            clearEnvironmentMutationIntent(key);
          const message =
            error instanceof Error
              ? error.message
              : "Unable to create Environment";
          update((draft) => {
            draft.projectError = message;
          });
          throw error;
        }
      },
      editEnvironment: async (envId, networkPool) => {
        assertEnvironmentMutable(envId, "Environment edit");
        const body: EnvironmentEditRequest = { network_pool: networkPool };
        const project = [
          ...state.tenantProjects,
          ...state.backingProjects,
        ].find((candidate) =>
          candidate.environments?.some(
            (environment) => environment.id === envId,
          ),
        );
        if (!project) throw new Error(`Environment ${envId} is not loaded`);
        const key = environmentMutationKey("edit", [envId, networkPool]);
        const intent = environmentMutationIntents.current.get(key) ?? {
          key,
          kind: "edit" as const,
          idempotencyKey: `groundplane:${newULID()}`,
          projectId: project.id,
          environmentId: envId,
          networkPool,
        };
        persistEnvironmentMutationIntent(intent);
        const generation = nextEnvironmentGeneration(envId, "edit");
        try {
          const edited = environmentFromAPI(
            await controllerRequest<EnvironmentEditResponse>(
              `/environments/${encodeURIComponent(envId)}`,
              200,
              { method: "PATCH", body, idempotencyKey: intent.idempotencyKey },
            ),
          );
          if (edited.id !== envId || edited.projectId !== project.id)
            throw new Error(
              `Controller returned Environment ${edited.id} for a different resource`,
            );
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation) {
            clearEnvironmentMutationIntent(key);
            throw new Error(`Environment edit ${envId} was superseded`);
          }
          if (!findEnvironment(state, envId)) {
            clearEnvironmentMutationIntent(key);
            throw new Error(`Environment edit ${envId} was not applied`);
          }
          update((draft) => {
            if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
              return;
            const environment = findEnvironment(draft, envId);
            if (!environment) return;
            applyAuthoritativeEnvironmentScalars(environment, edited);
          });
          settleEnvironmentMutation(envId, generation, "succeeded");
          clearEnvironmentMutationIntent(key);
          return edited;
        } catch (error) {
          settleEnvironmentMutation(envId, generation, "failed");
          throw error;
        }
      },
      renameEnvironment: async (envId, name) => {
        assertEnvironmentMutable(envId, "Environment rename");
        const body: EnvironmentRenameRequest = { name };
        const project = [
          ...state.tenantProjects,
          ...state.backingProjects,
        ].find((candidate) =>
          candidate.environments?.some(
            (environment) => environment.id === envId,
          ),
        );
        if (!project) throw new Error(`Environment ${envId} is not loaded`);
        const key = environmentMutationKey("rename", [envId, name]);
        const intent = environmentMutationIntents.current.get(key) ?? {
          key,
          kind: "rename" as const,
          idempotencyKey: `groundplane:${newULID()}`,
          projectId: project.id,
          environmentId: envId,
          name,
        };
        persistEnvironmentMutationIntent(intent);
        const generation = nextEnvironmentGeneration(envId, "rename");
        try {
          const renamed = environmentFromAPI(
            await controllerRequest<EnvironmentRenameResponse>(
              `/environments/${encodeURIComponent(envId)}/rename`,
              200,
              { method: "POST", body, idempotencyKey: intent.idempotencyKey },
            ),
          );
          if (renamed.id !== envId || renamed.projectId !== project.id)
            throw new Error(
              `Controller returned Environment ${renamed.id} for a different resource`,
            );
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation) {
            clearEnvironmentMutationIntent(key);
            throw new Error(`Environment rename ${envId} was superseded`);
          }
          if (!findEnvironment(state, envId)) {
            clearEnvironmentMutationIntent(key);
            throw new Error(`Environment rename ${envId} was not applied`);
          }
          update((draft) => {
            if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
              return;
            const environment = findEnvironment(draft, envId);
            if (!environment) return;
            applyAuthoritativeEnvironmentScalars(environment, renamed);
          });
          settleEnvironmentMutation(envId, generation, "succeeded");
          clearEnvironmentMutationIntent(key);
          return renamed;
        } catch (error) {
          settleEnvironmentMutation(envId, generation, "failed");
          throw error;
        }
      },
      watchTaskEvents: (taskId, onEvent, onMalformed) => {
        const source = new EventSource(
          `/api/v1/tasks/${encodeURIComponent(taskId)}/events`,
        );
        taskEventSources.current.add(source);
        const close = () => {
          source.close();
          taskEventSources.current.delete(source);
        };
        source.onmessage = (message) => {
          try {
            onEvent(parseTaskEvent(message.data));
          } catch {
            close();
            onMalformed("Task event stream returned malformed data");
          }
        };
        return close;
      },
      getTask: async (taskId, signal) => {
        const task = pendingResourceRemovals.current.has(taskId)
          ? await waitForRequest(requestResourceRemovalTask(taskId), signal)
          : await controllerRequest<TaskResponse>(
              `/tasks/${encodeURIComponent(taskId)}`,
              200,
              { signal },
            );
        const pending = pendingZoneRemovals.current.get(taskId);
        if (
          pending &&
          ["completed", "failed", "timed_out", "aborted"].includes(task.status)
        ) {
          pendingZoneRemovals.current.delete(taskId);
          if (task.status === "completed") {
            update((draft) => {
              const environment = findEnvironment(draft, pending.envId);
              if (!environment) return;
              const zone = environment.zones.find(
                (candidate) => candidate.id === pending.zoneId,
              );
              if (!zone) return;
              environment.zones = environment.zones.filter(
                (candidate) => candidate.id !== pending.zoneId,
              );
              environment.services.forEach((service) => {
                service.zones = service.zones.filter(
                  (name) => name !== zone.name,
                );
              });
            });
          }
        }
        if (pendingResourceRemovals.current.has(taskId)) {
          monitorResourceRemoval(taskId);
          void reconcileResourceRemoval(taskId, task).catch(() => undefined);
        }
        await reconcileConnectorRemoval(taskId, task);
        return task;
      },
      abortTask: async (taskId) => {
        const accepted = await controllerRequest<TaskAbortResponse>(
          `/tasks/${encodeURIComponent(taskId)}/abort`,
          202,
          { method: "POST" },
        );
        if (accepted.task_id !== taskId)
          throw new Error("Task abort returned a different Task id");
      },
      ...createBlueprintActions(assertEnvironmentMutable),
      deleteEnvironment: async (envId) => {
        if (!isEnvironmentDeletionPending(envId))
          assertEnvironmentMutable(envId, "Environment deletion");
        const project = [
          ...state.tenantProjects,
          ...state.backingProjects,
        ].find((candidate) =>
          candidate.environments?.some(
            (environment) => environment.id === envId,
          ),
        );
        if (!project)
          return Promise.reject(
            new Error(`Environment ${envId} is not loaded`),
          );
        const generation = nextEnvironmentGeneration(envId, "delete");
        const taskId = await dispatchResourceRemoval({
          kind: "environment",
          projectId: project.id,
          resourceId: envId,
          generation,
        });
        const task = await waitForResourceRemoval(taskId);
        if (task.status !== "completed") {
          throw new Error(`Environment deletion Task ${taskId} ${task.status}`);
        }
        return taskId;
      },
      addZone: async (envId, input) => {
        assertEnvironmentMutable(envId, "Zone mutation");
        const body: ZoneCreateRequest = {
          environment_id: envId,
          name: input.name,
          subnet: input.subnet,
          internal: input.internal,
        };
        const zone = zoneFromAPI(
          await controllerRequest<ZoneCreateResponse>("/zones", 201, {
            method: "POST",
            body,
          }),
        );
        update((draft) => {
          findEnvironment(draft, envId)?.zones.push(zone);
        });
        return zone;
      },
      getZone: async (zoneId) =>
        zoneFromAPI(
          await controllerRequest<ZoneShowResponse>(
            `/zones/${encodeURIComponent(zoneId)}`,
            200,
            { method: "GET" },
          ),
        ),
      getZoneRemovalImpact: (zoneId) =>
        controllerRequest<ZoneRemovalImpactResponse>(
          `/zones/${encodeURIComponent(zoneId)}/removal-impact`,
          200,
          { method: "GET" },
        ),
      removeZone: async (envId, zoneId, impactToken) => {
        assertEnvironmentMutable(envId, "Zone mutation");
        const accepted = await controllerRequest<ZoneRemoveResponse>(
          `/zones/${encodeURIComponent(zoneId)}${impactToken ? `?impact_token=${encodeURIComponent(impactToken)}` : ""}`,
          202,
          { method: "DELETE" },
        );
        if (!accepted.task_id)
          throw new Error("Controller response is missing task_id");
        pendingZoneRemovals.current.set(accepted.task_id, { envId, zoneId });
        return accepted.task_id;
      },
      ...createServiceActions(
        update,
        assertEnvironmentMutable,
        dispatchResourceRemoval,
      ),
      addRoute: async (envId, route) => {
        assertEnvironmentMutable(envId, "Route mutation");
        const generation = nextEnvironmentGeneration(envId, "child");
        const body: RouteCreateRequest = {
          environment_id: envId,
          host: route.host || undefined,
          path: route.path,
          exposure: route.exposure,
          target_service_id: route.targetServiceId,
          target_port: route.targetPort,
        };
        const accepted = await controllerRequest<RouteCreateAccepted>(
          "/routes",
          202,
          {
            method: "POST",
            body,
          },
        );
        const created = routeFromAPI(accepted.route);
        update((d) => {
          // Public Routes never auto-enable ingress; Component lifecycle is not authored by C07.
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
            return;
          findEnvironment(d, envId)?.routes.push(created);
        });
        return created;
      },
      getRoute: async (routeId) =>
        routeFromAPI(
          await controllerRequest<RouteShowResponse>(
            `/routes/${encodeURIComponent(routeId)}`,
            200,
            { method: "GET" },
          ),
        ),
      updateRoute: async (envId, routeId, patch) => {
        assertEnvironmentMutable(envId, "Route mutation");
        const generation = nextEnvironmentGeneration(envId, "child");
        const body: RouteEditRequest = { exposure: patch.exposure };
        const accepted = await controllerRequest<RouteEditAccepted>(
          `/routes/${encodeURIComponent(routeId)}`,
          202,
          { method: "PATCH", body },
        );
        const edited = routeFromAPI(accepted.route);
        update((d) => {
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
            return;
          const route = findEnvironment(d, envId)?.routes.find(
            (candidate) => candidate.id === routeId,
          );
          if (!route) return;
          Object.assign(route, edited);
        });
        return edited;
      },
      removeRoute: (envId, routeId) => (
        assertEnvironmentMutable(envId, "Route mutation"),
        dispatchResourceRemoval({
          kind: "route",
          environmentId: envId,
          resourceId: routeId,
        })
      ),
      addVolume: async (envId, input) => {
        assertEnvironmentMutable(envId, "Volume mutation");
        const created = await createVolume(controllerRequest, envId, input);
        update((draft) => {
          findEnvironment(draft, envId)?.volumes.push(created);
        });
        return created;
      },
      getVolume: async (volumeId) => {
        const volume = await getVolume(controllerRequest, volumeId);
        update((draft) => {
          const current = findEnvironment(
            draft,
            volume.environmentId,
          )?.volumes.find((candidate) => candidate.id === volume.id);
          if (current) Object.assign(current, volume);
        });
        return volume;
      },
      updateVolume: async (envId, volumeId, patch) => {
        assertEnvironmentMutable(envId, "Volume mutation");
        const edited = await editVolume(
          controllerRequest,
          volumeId,
          patch.slug,
        );
        update((draft) => {
          const volume = findEnvironment(draft, envId)?.volumes.find(
            (candidate) => candidate.id === volumeId,
          );
          if (volume) Object.assign(volume, edited);
        });
        return edited;
      },
      getVolumeDeletionImpact: (volumeId, cursor = "", limit = 40) =>
        getVolumeDeletionImpact(controllerRequest, volumeId, cursor, limit),
      removeVolume: (envId, volumeId, impactToken, confirmKey) => {
        assertEnvironmentMutable(envId, "Volume mutation");
        return removeVolume(
          controllerRequest,
          volumeId,
          impactToken,
          confirmKey,
        );
      },
      addScript: async (envId, script) => {
        assertEnvironmentMutable(envId, "Script mutation");
        const generation = nextEnvironmentGeneration(envId, "child");
        const targetService = (await listAllServices(envId)).find(
          (service) => service.name === script.service,
        );
        if (!targetService)
          throw new Error(
            `Service ${script.service} was not found in this Environment`,
          );
        const body = scriptCreateToAPI(envId, targetService.id, script);
        const created = scriptFromAPI(
          await controllerRequest<ScriptCreateResponse>("/scripts", 201, {
            method: "POST",
            body,
          }),
        );
        update((d) => {
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
            return;
          findEnvironment(d, envId)?.scripts.push(created);
        });
        return created;
      },
      updateScript: async (envId, scriptId, patch) => {
        assertEnvironmentMutable(envId, "Script mutation");
        const generation = nextEnvironmentGeneration(envId, "child");
        const body = scriptPatchToAPI(patch);
        const edited = scriptFromAPI(
          await controllerRequest<ScriptEditResponse>(
            `/scripts/${encodeURIComponent(scriptId)}`,
            200,
            { method: "PATCH", body },
          ),
        );
        update((d) => {
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
            return;
          const script = findEnvironment(d, envId)?.scripts.find(
            (candidate) => candidate.id === scriptId,
          );
          if (script) Object.assign(script, edited);
        });
        return edited;
      },
      runScript: async (scriptId) => {
        const accepted = await controllerRequest<ScriptRunResponse>(
          `/scripts/${encodeURIComponent(scriptId)}/run`,
          202,
          {
            method: "POST",
            body: undefined,
          },
        );
        return requireTaskId(accepted, "Script run");
      },
      removeScript: (envId, scriptId) => (
        assertEnvironmentMutable(envId, "Script mutation"),
        dispatchResourceRemoval({
          kind: "script",
          environmentId: envId,
          resourceId: scriptId,
        })
      ),
      addReleaseGroup: async (envId, group) => {
        assertEnvironmentMutable(envId, "Release group mutation");
        const environment = findEnvironment(state, envId);
        if (!environment) throw new Error(`Environment ${envId} was not found`);
        const serviceIDs = new Map(
          environment.services.map((service) => [service.name, service.id]),
        );
        const response = await controllerRequest<ReleaseGroupResponse>(
          "/release-groups",
          201,
          {
            method: "POST",
            body: {
              environment_id: envId,
              name: group.name,
              service_ids: group.services.map(
                (name) => serviceIDs.get(name) ?? name,
              ),
              order: group.order.map((name) => serviceIDs.get(name) ?? name),
              on_failure: group.onFailure,
            },
          },
        );
        const names = new Map(
          environment.services.map((service) => [service.id, service.name]),
        );
        const created: ReleaseGroup = {
          id: response.id,
          name: response.name,
          services: (response.service_ids ?? []).map(
            (id) => names.get(id) ?? id,
          ),
          order: (response.order ?? []).map((id) => names.get(id) ?? id),
          tag: response.tag,
          onFailure:
            response.on_failure === "leave_active"
              ? "leave_active"
              : "switch_back",
        };
        update((draft) => {
          findEnvironment(draft, envId)?.releaseGroups.push(created);
        });
        return created;
      },
      updateReleaseGroup: async (envId, groupId, patch) => {
        assertEnvironmentMutable(envId, "Release group mutation");
        const environment = findEnvironment(state, envId);
        if (!environment) throw new Error(`Environment ${envId} was not found`);
        const serviceIDs = new Map(
          environment.services.map((service) => [service.name, service.id]),
        );
        const response = await controllerRequest<ReleaseGroupResponse>(
          `/release-groups/${encodeURIComponent(groupId)}`,
          200,
          {
            method: "PATCH",
            body: {
              name: patch.name,
              service_ids: patch.services.map(
                (name) => serviceIDs.get(name) ?? name,
              ),
              order: patch.order.map((name) => serviceIDs.get(name) ?? name),
              on_failure: patch.onFailure,
            },
          },
        );
        const names = new Map(
          environment.services.map((service) => [service.id, service.name]),
        );
        const edited: ReleaseGroup = {
          id: response.id,
          name: response.name,
          services: (response.service_ids ?? []).map(
            (id) => names.get(id) ?? id,
          ),
          order: (response.order ?? []).map((id) => names.get(id) ?? id),
          tag: response.tag,
          onFailure:
            response.on_failure === "leave_active"
              ? "leave_active"
              : "switch_back",
        };
        update((draft) => {
          const groups = findEnvironment(draft, envId)?.releaseGroups;
          const index =
            groups?.findIndex((candidate) => candidate.id === groupId) ?? -1;
          if (groups && index >= 0) groups[index] = edited;
        });
        return edited;
      },
      removeReleaseGroup: async (_envId, groupId) => {
        assertEnvironmentMutable(_envId, "Release group mutation");
        const response = await controllerRequest<ReleaseGroupMutationAccepted>(
          `/release-groups/${encodeURIComponent(groupId)}`,
          202,
          { method: "DELETE" },
        );
        return requireTaskId(response, "Release group removal");
      },
      deployReleaseGroup: async (_envId, groupId, tag) => {
        assertEnvironmentMutable(_envId, "Release group mutation");
        const body = tag ? { tag } : {};
        const response = await controllerRequest<ReleaseGroupTaskAccepted>(
          `/release-groups/${encodeURIComponent(groupId)}/deploy`,
          202,
          { method: "POST", body },
        );
        return requireTaskId(response, "Release group deploy");
      },
      previewReleaseGroupRollback: async (_envId, groupId, tag) =>
        controllerRequest<ReleaseGroupRollbackPreviewResponse>(
          `/release-groups/${encodeURIComponent(groupId)}/rollback-preview${tag === undefined ? "" : `?tag=${encodeURIComponent(tag)}`}`,
          200,
        ),
      rollbackReleaseGroup: async (_envId, groupId, tag, previewRevision) => {
        assertEnvironmentMutable(_envId, "Release group mutation");
        const response = await controllerRequest<ReleaseGroupTaskAccepted>(
          `/release-groups/${encodeURIComponent(groupId)}/rollback`,
          202,
          {
            method: "POST",
            body: {
              ...(tag === undefined ? {} : { tag }),
              preview_revision: previewRevision,
            },
          },
        );
        return requireTaskId(response, "Release group rollback");
      },
      addAttach: async (_envId, input) => {
        assertEnvironmentMutable(_envId, "Attach mutation");
        const body: AttachCreateRequest = {
          service_id: input.serviceId,
          backing_service_id: input.backingServiceId,
          name: input.name,
          credential:
            input.credential.mode === "new"
              ? { mode: "new" }
              : { mode: "existing", attach_id: input.credential.attachId },
          grant_attach_ids:
            input.credential.mode === "new" ? input.grantAttachIds : undefined,
        };
        const response = await controllerRequest<AttachTaskAccepted>(
          "/attaches",
          202,
          { method: "POST", body },
        );
        return requireTaskId(response, "Attach creation");
      },
      renameAttach: async (envId, attachId, name) => {
        assertEnvironmentMutable(envId, "Attach mutation");
        const body: AttachRenameRequest = { name };
        const response = await controllerRequest<AttachRenameResponse>(
          `/attaches/${encodeURIComponent(attachId)}/rename`,
          200,
          { method: "POST", body },
        );
        update((draft) => {
          const attach = findEnvironment(draft, envId)?.attaches.find(
            (candidate) => candidate.id === attachId,
          );
          if (attach) attach.name = response.name;
        });
      },
      removeAttach: async (_envId, attachId) => {
        assertEnvironmentMutable(_envId, "Attach mutation");
        const response = await controllerRequest<AttachTaskAccepted>(
          `/attaches/${encodeURIComponent(attachId)}`,
          202,
          {
            method: "DELETE",
          },
        );
        return requireTaskId(response, "Attach removal");
      },
      revealAttachFact: (attachId, key, grantAttachId) =>
        revealAttachFactValue(attachId, key, grantAttachId),
      addEntry: async (envId, input) => {
        assertEnvironmentMutable(envId, "Entry mutation");
        const generation = nextEnvironmentGeneration(envId, "child");
        const response = await controllerRequest<EntryResponse>(
          "/entries",
          201,
          {
            method: "POST",
            body: { ...input, environment_id: envId },
          },
        );
        const entry = entryFromAPI(response);
        update((draft) => {
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
            return;
          findEnvironment(draft, envId)?.entries.push(entry);
        });
        return entry;
      },
      bulkUpsertEntries: async (envId, input) => {
        assertEnvironmentMutable(envId, "Entry mutation");
        const generation = nextEnvironmentGeneration(envId, "child");
        const response = await controllerRequest<EntryBulkUpsertResponse>(
          "/entries/bulk",
          202,
          {
            method: "POST",
            body: { ...input, environment_id: envId },
          },
        );
        const entries = (response.entries ?? []).map(entryFromAPI);
        update((draft) => {
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
            return;
          const environment = findEnvironment(draft, envId);
          if (!environment) return;
          const ids = new Set(entries.map((entry) => entry.id));
          environment.entries = environment.entries.filter(
            (entry) => !ids.has(entry.id),
          );
          environment.entries.push(...entries);
        });
        return response;
      },
      updateEntry: async (envId, entryId, input) => {
        assertEnvironmentMutable(envId, "Entry mutation");
        const generation = nextEnvironmentGeneration(envId, "child");
        const response = await controllerRequest<EntryResponse>(
          `/entries/${encodeURIComponent(entryId)}`,
          200,
          {
            method: "PATCH",
            body: input,
          },
        );
        const entry = entryFromAPI(response);
        update((draft) => {
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
            return;
          const environment = findEnvironment(draft, envId);
          if (!environment) return;
          const index = environment.entries.findIndex(
            (candidate) => candidate.id === entryId,
          );
          if (index >= 0) environment.entries[index] = entry;
        });
        return entry;
      },
      removeEntry: (envId, entryId) => (
        assertEnvironmentMutable(envId, "Entry mutation"),
        dispatchResourceRemoval({
          kind: "entry",
          environmentId: envId,
          resourceId: entryId,
        })
      ),
      revealEntry: async (entryId) => {
        const value = await controllerRequest<EntryValueResponse>(
          `/entries/${encodeURIComponent(entryId)}/value`,
          200,
        );
        return value.value;
      },
      ...createSecretActions(state, update, refreshReusableSecrets),
      ...connectorActions,
      refreshRunners,
      createRunner,
      renameRunner,
      retryRunner,
      removeRunner,
      runBackingRuntimeAction: async (id, action) => {
        const accepted = await controllerRequest<BackingRuntimeTaskAccepted>(
          `/backing-services/${encodeURIComponent(id)}/${action}`,
          202,
          { method: "POST" },
        );
        const intent: ServiceRuntimeIntent =
          action === "start"
            ? "running"
            : action === "stop"
              ? "stopped"
              : "absent";
        update((d) => {
          const project = d.backingProjects.find(
            (candidate) => candidate.id === id,
          );
          project?.environments?.forEach((environment) => {
            environment.services.forEach((service) => {
              service.runtimeIntent = intent;
            });
          });
        });
        return requireTaskId(accepted, `Backing service ${action}`);
      },
      addBackingProject: async (input) => {
        const created = await controllerRequest<BackingServiceCreatedResponse>(
          "/backing-services",
          201,
          { method: "POST", body: input },
        );
        requireTaskId(created, "Backing service creation");
        const backingProjects = await listAllBackingProjects(
          state.tenantProjects,
          state.tenants,
          new AbortController().signal,
        );
        update((draft) => {
          draft.backingProjects = backingProjects;
          draft.backingProjectError = null;
        });
        return created;
      },
      setComponentEnabled: async (componentId, enabled, config) => {
        const action = enabled ? "enable" : "disable";
        const accepted = await controllerRequest<ComponentTaskAccepted>(
          `/components/${encodeURIComponent(componentId)}/${action}`,
          202,
          {
            method: "POST",
            ...(enabled && config ? { body: { config } } : {}),
          },
        );
        if (!accepted.task_id)
          throw new Error(
            `Controller response is missing Component ${action} task_id`,
          );
        return accepted.task_id;
      },
      reconcileEnvironmentComponent: async (componentId) => {
        const accepted = await controllerRequest<ComponentTaskAccepted>(
          `/components/${encodeURIComponent(componentId)}/update`,
          202,
          { method: "POST" },
        );
        if (!accepted.task_id)
          throw new Error(
            "Controller response is missing Component update task_id",
          );
        return accepted.task_id;
      },
      updateComponentConfig: async (componentId, config) => {
        const result = await controllerRequest<ComponentConfigMutationResponse>(
          `/components/${encodeURIComponent(componentId)}/config`,
          200,
          { method: "PUT", body: { config } },
        );
        if (!result.reconcile_task_id)
          throw new Error(
            "Controller response is missing Component config reconcile_task_id",
          );
        return result.reconcile_task_id;
      },
    };
  }, [
    state,
    controllerPlatform,
    backupStore,
    update,
    loadTaskJournal,
    refreshPlatformComponents,
    refreshComponentConfig,
    refreshEnvironmentComponents,
    refreshEnvironmentReleases,
    refreshEnvironmentServices,
    refreshAgents,
    dispatchResourceRemoval,
    monitorResourceRemoval,
    reconcileResourceRemoval,
    requestResourceRemovalTask,
    nextEnvironmentGeneration,
    settleEnvironmentMutation,
    shouldPreserveEnvironmentOnLoad,
    getEnvironmentDeletionFailure,
    refreshEnvironmentDeletion,
    isEnvironmentDeletionPending,
    waitForResourceRemoval,
    retryResourceRemoval,
    observeEnvironmentDeletionTasks,
    environmentGenerations,
    assertEnvironmentMutable,
  ]);

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}
export function useStore() {
  const ctx = useContext(Ctx);
  if (!ctx) throw new Error("useStore must be used within StoreProvider");
  return ctx;
}
