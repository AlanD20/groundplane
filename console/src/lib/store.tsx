import {
  useTenantLoading,
  useBackingProjectLoading,
  useTenantProjectLoading,
} from "./use-workspace-loading";
("use client");

import {
  createReleaseActions,
  useReleaseRefresh,
} from "@/features/release/actions";
import { createEnvironmentActions } from "@/features/environment/actions";
import { useEnvironmentMutationIntents } from "@/features/environment/use-mutation-intents";
import { useTaskJournal } from "@/features/task/use-task-journal";
import { useTaskEventStreams } from "@/features/task/use-task-event-streams";
import {
  createAgentActions,
  useAgentRefresh,
  useAgentLoading,
} from "@/features/agent/agent-store";
import { createBackingServiceActions } from "@/features/backing-service/actions";

import { backingConsumerRevision } from "@/features/backing-service/consumer-projection";
import { createTenantActions } from "@/features/tenant/actions";

import { createProjectActions } from "@/features/project/actions";

import { listAllEnvironments } from "@/features/environment/workspace-read";
import { createComponentActions } from "@/features/component/actions";
import { useComponentRefresh } from "@/features/component/use-component-refresh";
import { createAttachActions } from "@/features/attach/actions";
import { createEntryActions } from "@/features/entry/actions";
import { listAllEntries } from "@/features/entry/api";
import { createReleaseGroupActions } from "@/features/release-group/actions";
import { createScriptActions } from "@/features/script/actions";
import { listAllScripts } from "@/features/script/api";
import { createVolumeActions } from "@/features/volume/actions";
import { useNetworkActions } from "@/features/environment/use-network-actions";
import { createServiceActions } from "@/features/service/actions";
import { useRunnerStore } from "@/features/runner/use-runner-store";
import { listAllRoutes } from "@/features/environment/network-api";
import { useConnectorStore } from "@/features/connectors/use-connector-store";
import { controllerRequest, waitForRequest } from "./controller-json-request";
import { useBackupStore } from "@/features/backup/use-backup-store";
import {
  createSecretActions,
  useSecretRefresh,
} from "@/features/secrets/secret-store";
import { controllerUpdateRejected } from "./controller-request-errors";
import { applyServiceObservations } from "@/features/service/service-observation";
import { listAllServices } from "@/features/service/api";
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
import { watchTransientLogs } from "./transient-logs";
import { useControllerPlatform } from "@/features/platform-controller/use-controller-platform";
import { createBlueprintActions } from "@/features/blueprint/api";
import { adapters as seedAdapters } from "./workspace-seed";
import { useEnvironmentLifecycle } from "@/features/environment/use-environment-lifecycle";
import type { TaskResponse } from "@/features/environment/environment-removal-model";

import { environmentDeletionGuard } from "@/features/environment/mutation-guard";
import { seed, type State, type StoreContext } from "./store-model";
type EnvironmentDeleteResponse =
  operations["environment.delete"]["responses"][202]["content"]["application/json"];

type BackupKeyRotateResponse =
  operations["backup.key.rotate"]["responses"][202]["content"]["application/json"];

type TaskRetryResponse =
  operations["task.retry"]["responses"][202]["content"]["application/json"];

type TaskAbortResponse =
  operations["task.abort"]["responses"][202]["content"]["application/json"];

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

  const refreshEnvironmentReleases = useReleaseRefresh(state, update);

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

  useTenantLoading(update);

  useBackingProjectLoading({
    state,
    backingConsumerInput,
    environmentGenerations,
    observeEnvironmentDeletionTasks,
    shouldPreserveEnvironmentOnLoad,
    update,
  });

  useAgentLoading(state, update, refreshAgents);

  useTenantProjectLoading({
    environmentGenerations,
    observeEnvironmentDeletionTasks,
    shouldPreserveEnvironmentOnLoad,
    getEnvironmentDeletionFailure,
    update,
  });

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
      ...createReleaseActions(state, assertEnvironmentMutable),
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
