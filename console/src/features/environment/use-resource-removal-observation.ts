import { useCallback } from "react";

import {
  environmentDeletionTaskError,
  findEnvironment,
  terminalTaskStatuses,
  type TaskResponse,
} from "@/features/environment/environment-removal-model";

import { isAuthoritativeTaskUnavailable } from "@/features/environment/removal-outcome";

import type {
  EnvironmentLifecycleDraft,
  EnvironmentLifecycleOptions,
} from "./lifecycle-types";
import type { useResourceRemovalState } from "./use-resource-removal-state";
export function useResourceRemovalObservation<
  State extends EnvironmentLifecycleDraft,
>(
  options: EnvironmentLifecycleOptions<State>,
  state: ReturnType<typeof useResourceRemovalState<State>>,
) {
  const {
    active,
    update,
    requestTask,
    listEnvironments,
    listServices,
    listRoutes,
    listEntries,
    listScripts,
  } = options;
  const {
    pendingResourceRemovals,
    resourceRemovalTaskRequests,
    resourceRemovalTaskControllers,
    resourceRemovalObservations,
    resourceRemovalReconciliations,
    resourceRemovalReconciliationControllers,
    resourceRemovalErrors,
    environmentGenerations,
    successfulEnvironmentDeletions,
    settleEnvironmentMutation,
    forgetResourceRemoval,
    recordResourceRemovalError,
  } = state;
  const requestResourceRemovalTask = useCallback(
    (taskId: string) => {
      const current = resourceRemovalTaskRequests.current.get(taskId);
      if (current) return current;
      const removal = pendingResourceRemovals.current.get(taskId);
      const controller = new AbortController();
      resourceRemovalTaskControllers.current.set(taskId, controller);
      const request = requestTask(taskId, controller.signal)
        .then((task) => {
          const validationError = removal
            ? environmentDeletionTaskError(taskId, task, removal)
            : undefined;
          if (validationError) {
            recordResourceRemovalError(
              taskId,
              removal!,
              validationError,
              "refresh",
            );
            throw new Error(validationError);
          }
          if (
            active.current &&
            !controller.signal.aborted &&
            removal &&
            pendingResourceRemovals.current.get(taskId) === removal
          ) {
            resourceRemovalObservations.current.set(taskId, {
              task,
              observedAt: Date.now(),
            });
          }
          return task;
        })
        .catch((error) => {
          if (removal && isAuthoritativeTaskUnavailable(error)) {
            recordResourceRemovalError(
              taskId,
              removal,
              `Unable to observe deletion Task ${taskId}: Controller no longer has this Task; refresh the Environment to recover`,
              "refresh",
            );
          }
          throw error;
        })
        .finally(() => {
          if (resourceRemovalTaskRequests.current.get(taskId) === request)
            resourceRemovalTaskRequests.current.delete(taskId);
          if (resourceRemovalTaskControllers.current.get(taskId) === controller)
            resourceRemovalTaskControllers.current.delete(taskId);
        });
      resourceRemovalTaskRequests.current.set(taskId, request);
      return request;
    },
    [active, recordResourceRemovalError, requestTask],
  );

  const reconcileResourceRemoval = useCallback(
    (taskId: string, task: TaskResponse) => {
      const removal = pendingResourceRemovals.current.get(taskId);
      if (!removal || !terminalTaskStatuses.has(task.status))
        return Promise.resolve();
      const validationError = environmentDeletionTaskError(
        taskId,
        task,
        removal,
      );
      if (validationError) {
        recordResourceRemovalError(taskId, removal, validationError, "refresh");
        return Promise.reject(new Error(validationError));
      }
      if (task.status !== "completed") {
        const label = removal.kind[0].toUpperCase() + removal.kind.slice(1);
        const message = `${label} deletion task ${taskId} ${task.status}`;
        recordResourceRemovalError(taskId, removal, message);
        return Promise.resolve();
      }
      const current = resourceRemovalReconciliations.current.get(taskId);
      if (current) return current;
      const controller = new AbortController();
      resourceRemovalReconciliationControllers.current.set(taskId, controller);
      const reconciliation = (async () => {
        try {
          const refreshed =
            removal.kind === "environment"
              ? {
                  kind: "environment" as const,
                  projectId: removal.projectId,
                  resources: await listEnvironments(
                    removal.projectId,
                    controller.signal,
                  ),
                }
              : removal.kind === "service"
                ? {
                    kind: "service" as const,
                    environmentId: removal.environmentId,
                    resources: await listServices(
                      removal.environmentId,
                      controller.signal,
                    ),
                  }
                : removal.kind === "route"
                  ? {
                      kind: "route" as const,
                      environmentId: removal.environmentId,
                      resources: await listRoutes(
                        removal.environmentId,
                        controller.signal,
                      ),
                    }
                  : removal.kind === "entry"
                    ? {
                        kind: "entry" as const,
                        environmentId: removal.environmentId,
                        resources: await listEntries(
                          removal.environmentId,
                          controller.signal,
                        ),
                      }
                    : {
                        kind: "script" as const,
                        environmentId: removal.environmentId,
                        resources: await listScripts(
                          removal.environmentId,
                          controller.signal,
                        ),
                      };
          if (!active.current || controller.signal.aborted) return;
          if (
            refreshed.kind === "environment" &&
            refreshed.resources.some(
              (candidate) => candidate.id === removal.resourceId,
            )
          ) {
            throw new Error(
              `Authoritative Environment list still contains ${removal.resourceId} after deletion`,
            );
          }
          const currentGeneration =
            removal.kind === "environment"
              ? (environmentGenerations.current.get(removal.resourceId) ?? 0)
              : (environmentGenerations.current.get(removal.environmentId) ??
                0);
          const generationChanged =
            removal.generation !== undefined &&
            currentGeneration !== removal.generation;
          const relatedTaskIds = [...pendingResourceRemovals.current.entries()]
            .filter(([, candidate]) => candidate === removal)
            .map(([relatedTaskId]) => relatedTaskId);
          const clearedErrors = relatedTaskIds
            .map((relatedTaskId) =>
              resourceRemovalErrors.current.get(relatedTaskId),
            )
            .filter((message): message is string => !!message);
          for (const relatedTaskId of relatedTaskIds)
            forgetResourceRemoval(relatedTaskId, removal);
          if (removal.kind === "environment") {
            successfulEnvironmentDeletions.current.set(
              removal.resourceId,
              taskId,
            );
            settleEnvironmentMutation(
              removal.resourceId,
              removal.generation,
              "succeeded",
            );
          }
          update((draft) => {
            switch (refreshed.kind) {
              case "environment": {
                const project = [
                  ...draft.tenantProjects,
                  ...draft.backingProjects,
                ].find((candidate) => candidate.id === refreshed.projectId);
                if (project) {
                  project.environments = generationChanged
                    ? (project.environments ?? []).filter(
                        (candidate) => candidate.id !== removal.resourceId,
                      )
                    : refreshed.resources;
                }
                break;
              }
              case "route": {
                const environment = findEnvironment(
                  draft,
                  refreshed.environmentId,
                );
                if (environment && !generationChanged)
                  environment.routes = refreshed.resources;
                break;
              }
              case "service": {
                const environment = findEnvironment(
                  draft,
                  refreshed.environmentId,
                );
                if (environment && !generationChanged)
                  environment.services = refreshed.resources;
                break;
              }
              case "entry": {
                const environment = findEnvironment(
                  draft,
                  refreshed.environmentId,
                );
                if (environment && !generationChanged)
                  environment.entries = refreshed.resources;
                break;
              }
              case "script": {
                const environment = findEnvironment(
                  draft,
                  refreshed.environmentId,
                );
                if (environment && !generationChanged)
                  environment.scripts = refreshed.resources;
                break;
              }
            }
            if (
              draft.projectError &&
              clearedErrors.includes(draft.projectError)
            ) {
              draft.projectError =
                [...resourceRemovalErrors.current.values()].at(-1) ?? null;
            }
          });
        } catch (error) {
          if (!active.current || controller.signal.aborted) throw error;
          const label = removal.kind[0].toUpperCase() + removal.kind.slice(1);
          const detail =
            error instanceof Error
              ? error.message
              : `Unable to refresh ${label}s`;
          const message = `${label} removal completed, but authoritative refresh failed: ${detail}`;
          recordResourceRemovalError(taskId, removal, message, "refresh");
          throw error;
        }
      })();
      resourceRemovalReconciliations.current.set(taskId, reconciliation);
      void reconciliation
        .finally(() => {
          if (
            resourceRemovalReconciliations.current.get(taskId) ===
            reconciliation
          )
            resourceRemovalReconciliations.current.delete(taskId);
          if (
            resourceRemovalReconciliationControllers.current.get(taskId) ===
            controller
          )
            resourceRemovalReconciliationControllers.current.delete(taskId);
        })
        .catch(() => undefined);
      return reconciliation;
    },
    [
      active,
      forgetResourceRemoval,
      listEntries,
      listEnvironments,
      listRoutes,
      listScripts,
      listServices,
      recordResourceRemovalError,
      settleEnvironmentMutation,
      update,
    ],
  );

  return { requestResourceRemovalTask, reconcileResourceRemoval };
}
