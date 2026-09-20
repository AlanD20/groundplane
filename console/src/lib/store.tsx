"use client";
import {
  createEnvironmentActions,
  type EnvironmentActions,
} from "@/features/environment/actions";
import { useEnvironmentMutationIntents } from "@/features/environment/use-mutation-intents";
import {
  useTaskJournal,
  type TaskJournalStoreState,
  type TaskJournalActions,
} from "@/features/task/use-task-journal";
import {
  useTaskEventStreams,
  type TaskEventActions,
} from "@/features/task/use-task-event-streams";
import {
  createAgentActions,
  useAgentRefresh,
  useAgentLoading,
  type AgentActions,
  type AgentState,
} from "@/features/agent/agent-store";
import {
  createBackingServiceActions,
  type BackingServiceActions,
} from "@/features/backing-service/actions";
import { listAllBackingProjects } from "@/features/backing-service/workspace-read";
import { backingConsumerRevision } from "@/features/backing-service/consumer-projection";
import {
  createTenantActions,
  type TenantActions,
} from "@/features/tenant/actions";
import { listAllTenants } from "@/features/tenant/api";
import {
  createProjectActions,
  type ProjectActions,
} from "@/features/project/actions";
import { listAllTenantProjects } from "@/features/project/api";
import { listAllEnvironments } from "@/features/environment/workspace-read";
import {
  createComponentActions,
  type ComponentActions,
} from "@/features/component/actions";
import {
  useComponentRefresh,
  type ComponentState,
  type ComponentRefreshActions,
} from "@/features/component/use-component-refresh";
import {
  createAttachActions,
  type AttachActions,
} from "@/features/attach/actions";
import {
  createEntryActions,
  type EntryActions,
} from "@/features/entry/actions";
import { listAllEntries } from "@/features/entry/api";
import {
  createReleaseGroupActions,
  type ReleaseGroupActions,
} from "@/features/release-group/actions";
import { listAllReleaseGroups } from "@/features/release-group/api";
import { refreshReleaseGroupTags } from "@/features/release-group/projection";
import {
  createScriptActions,
  type ScriptActions,
} from "@/features/script/actions";
import { listAllScripts } from "@/features/script/api";
import {
  createVolumeActions,
  type VolumeActions,
} from "@/features/volume/actions";
import {
  useNetworkActions,
  type NetworkActions,
} from "@/features/environment/use-network-actions";
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
  listAllZones,
  listAllRoutes,
} from "@/features/environment/network-api";
import { listAllReleases, projectReleaseSummary } from "@/features/release/api";
import {
  useConnectorStore,
  type ConnectorState,
  type ConnectorActions,
} from "@/features/connectors/use-connector-store";
import { emptyTaskJournal, requireTaskId } from "@/features/task/journal-model";
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
import { releaseForServiceName, listAllServices } from "@/features/service/api";
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
import { useControllerPlatform } from "@/features/platform-controller/use-controller-platform";
import type {
  EnvFile,
  Environment,
  Project,
  Service,
  Tenant,
  PlatformInfra,
} from "./types";
import {
  createBlueprintActions,
  type BlueprintActions,
} from "@/features/blueprint/api";
import {
  adapters as seedAdapters,
  platform as seedPlatform,
} from "./mock-data";
import { useEnvironmentLifecycle } from "@/features/environment/use-environment-lifecycle";
import type {
  EnvironmentDeletionFailure,
  TaskResponse,
} from "@/features/environment/environment-removal-model";
import {
  environmentGenerationSnapshot,
  mergeEnvironmentProjectLoads,
} from "@/features/environment/workspace-reconciliation";
import { environmentDeletionGuard } from "@/features/environment/mutation-guard";
import { newId } from "./utils";
type EnvironmentDeleteResponse =
  operations["environment.delete"]["responses"][202]["content"]["application/json"];
type BackupKeyRotateResponse =
  operations["backup.key.rotate"]["responses"][202]["content"]["application/json"];
type TaskRetryResponse =
  operations["task.retry"]["responses"][202]["content"]["application/json"];
type TaskAbortResponse =
  operations["task.abort"]["responses"][202]["content"]["application/json"];
type State = ReusableSecretState &
  ConnectorState &
  RunnerState &
  TaskJournalStoreState &
  ComponentState &
  AgentState & {
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
    platform: PlatformInfra;
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
  NetworkActions &
  VolumeActions &
  ScriptActions &
  ReleaseGroupActions &
  AttachActions &
  EntryActions &
  TenantActions &
  ProjectActions &
  BackingServiceActions &
  AgentActions &
  TaskJournalActions &
  TaskEventActions &
  EnvironmentActions &
  ComponentActions &
  ComponentRefreshActions &
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
    refreshEnvironmentReleases: (
      environmentId: string,
      signal?: AbortSignal,
    ) => Promise<void>;
    refreshEnvironmentServices: (
      environmentId: string,
      signal?: AbortSignal,
    ) => Promise<void>;
    getTenant: (slug: string) => Tenant | undefined;
    getProject: (tenantSlug: string, slug: string) => Project | undefined;
    getProjectById: (id: string) => Project | undefined;
    getBackingProject: (id: string) => Project | undefined;
    getEnvironment: (
      tenantSlug: string,
      projectSlug: string,
      envName: string,
    ) => Environment | undefined;
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
    getTask: (taskId: string, signal?: AbortSignal) => Promise<TaskResponse>;
    abortTask: (taskId: string) => Promise<void>;
  };

const Ctx = createContext<StoreContext | null>(null);

export function StoreProvider({ children }: { children: React.ReactNode }) {
  const controllerPlatform = useControllerPlatform(
    controllerRequest,
    controllerUpdateRejected,
  );
  const providerActive = useRef(true);
  const [state, setState] = useState<State>(seed);
  const { watchTaskEvents, closeTaskStreams } = useTaskEventStreams();

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
  const environmentMutations = useEnvironmentMutationIntents();

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

  const { actions: networkActions, reconcileZoneRemoval } = useNetworkActions({
    update,
    assertEnvironmentMutable,
    nextEnvironmentGeneration,
    environmentGenerations,
    dispatchResourceRemoval,
  });

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
      closeTaskStreams();
      environmentMutations.close();
    },
    [],
  );

  const {
    refreshPlatformComponents,
    refreshComponentConfig,
    refreshEnvironmentComponents,
  } = useComponentRefresh(update);

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

  const refreshAgents = useAgentRefresh(update);

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

  useAgentLoading(state, update, refreshAgents);

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

  const taskJournalActions = useTaskJournal(state, update);

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
      ...createAgentActions(update, refreshAgents),
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
      getEnvironmentDeletionFailure,
      refreshEnvironmentDeletion,
      isEnvironmentDeletionPending,
      ...taskJournalActions,
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
      ...createTenantActions(state, update),
      ...createProjectActions(update),
      ...createEnvironmentActions({
        state,
        update,
        lifecycle: environmentLifecycle,
        providerActive,
        requestEnvironmentTask,
        assertEnvironmentMutable,
        mutations: environmentMutations,
      }),
      watchTaskEvents,
      getTask: async (taskId, signal) => {
        const task = pendingResourceRemovals.current.has(taskId)
          ? await waitForRequest(requestResourceRemovalTask(taskId), signal)
          : await controllerRequest<TaskResponse>(
              `/tasks/${encodeURIComponent(taskId)}`,
              200,
              { signal },
            );
        reconcileZoneRemoval(taskId, task);
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
      ...networkActions,
      ...createServiceActions(
        update,
        assertEnvironmentMutable,
        dispatchResourceRemoval,
      ),
      ...createVolumeActions(update, assertEnvironmentMutable),
      ...createScriptActions(
        update,
        assertEnvironmentMutable,
        nextEnvironmentGeneration,
        environmentGenerations,
        dispatchResourceRemoval,
      ),
      ...createReleaseGroupActions(state, update, assertEnvironmentMutable),
      ...createAttachActions(update, assertEnvironmentMutable),
      ...createEntryActions(
        update,
        assertEnvironmentMutable,
        nextEnvironmentGeneration,
        environmentGenerations,
        dispatchResourceRemoval,
      ),
      ...createSecretActions(state, update, refreshReusableSecrets),
      ...connectorActions,
      refreshRunners,
      createRunner,
      renameRunner,
      retryRunner,
      removeRunner,
      ...createBackingServiceActions(state, update),
      ...createComponentActions(),
    };
  }, [
    state,
    controllerPlatform,
    backupStore,
    update,
    taskJournalActions,
    watchTaskEvents,
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
