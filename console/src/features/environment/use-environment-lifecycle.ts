import type {
  EnvironmentLifecycleDraft,
  EnvironmentLifecycleOptions,
} from "./lifecycle-types";
import type { EnvironmentLifecycle } from "./lifecycle-types";
import { useResourceRemovalState } from "./use-resource-removal-state";
import { useResourceRemovalObservation } from "./use-resource-removal-observation";
import { useCallback, useEffect } from "react";
import type { Project } from "@/lib/types";
import {
  findEnvironment,
  terminalTaskStatuses,
  type PendingResourceRemoval,
} from "@/features/environment/environment-removal-model";
import {
  persistPendingResourceRemovalIntents,
  persistPendingResourceRemovals,
  persistResourceRemovalRetryIntents,
  pendingEnvironmentDeletionReplays,
  resourceRemovalKey,
} from "@/features/environment/operation-storage";
import { newULID } from "@/lib/utils";
import { isDefinitiveRemovalRequestRejection } from "@/features/environment/removal-outcome";

const resourceRemovalObservationFreshMs = 1_500;
export function useEnvironmentLifecycle<
  State extends EnvironmentLifecycleDraft,
>(options: EnvironmentLifecycleOptions<State>): EnvironmentLifecycle<State> {
  const { active, update, deleteResource, retryResource } = options;
  const removalState = useResourceRemovalState<State>(update);
  const {
    pendingResourceRemovals,
    resourceRemovalTasks,
    resourceRemovalDispatches,
    resourceRemovalRetries,
    resourceRemovalRetryIntents,
    resourceRemovalTaskControllers,
    resourceRemovalObservations,
    resourceRemovalReconciliationControllers,
    resourceRemovalMonitors,
    resourceRemovalErrors,
    resourceRemovalIntents,
    environmentDeletionFailures,
    environmentGenerations,
    environmentMutationOutcomes,
    successfulEnvironmentDeletions,
    nextEnvironmentGeneration,
    settleEnvironmentMutation,
    shouldPreserveEnvironmentOnLoad,
    getEnvironmentDeletionFailure,
    isEnvironmentDeletionPending,
    forgetResourceRemoval,
  } = removalState;

  const { requestResourceRemovalTask, reconcileResourceRemoval } =
    useResourceRemovalObservation(options, removalState);

  const monitorResourceRemoval = useCallback(
    (taskId: string) => {
      if (
        !pendingResourceRemovals.current.has(taskId) ||
        resourceRemovalMonitors.current.has(taskId)
      )
        return;
      let stopped = false;
      let polling = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const stop = () => {
        if (stopped) return;
        stopped = true;
        if (timer) clearTimeout(timer);
        if (resourceRemovalMonitors.current.get(taskId) === stop)
          resourceRemovalMonitors.current.delete(taskId);
      };
      const schedule = () => {
        if (!stopped) timer = setTimeout(() => void poll(), 1_000);
      };
      const poll = async () => {
        if (stopped || polling || !active.current) return;
        if (!pendingResourceRemovals.current.has(taskId)) return stop();
        polling = true;
        try {
          const observation = resourceRemovalObservations.current.get(taskId);
          if (
            observation &&
            terminalTaskStatuses.has(observation.task.status)
          ) {
            await reconcileResourceRemoval(taskId, observation.task);
            return stop();
          }
          if (
            observation &&
            Date.now() - observation.observedAt <
              resourceRemovalObservationFreshMs
          )
            return schedule();
          const task = await requestResourceRemovalTask(taskId);
          if (!terminalTaskStatuses.has(task.status)) return schedule();
          await reconcileResourceRemoval(taskId, task);
          stop();
        } catch {
          if (resourceRemovalErrors.current.has(taskId)) stop();
          else schedule();
        } finally {
          polling = false;
        }
      };
      resourceRemovalMonitors.current.set(taskId, stop);
      void poll();
    },
    [active, reconcileResourceRemoval, requestResourceRemovalTask],
  );

  const dispatchResourceRemoval = useCallback(
    (removal: PendingResourceRemoval) => {
      if (
        removal.kind !== "environment" &&
        isEnvironmentDeletionPending(removal.environmentId)
      ) {
        return Promise.reject(
          new Error(
            `Environment ${removal.environmentId} deletion is in progress; ${removal.kind} mutation is disabled`,
          ),
        );
      }
      const key = resourceRemovalKey(removal);
      const existingTaskId = resourceRemovalTasks.current.get(key);
      if (
        existingTaskId &&
        pendingResourceRemovals.current.has(existingTaskId)
      ) {
        monitorResourceRemoval(existingTaskId);
        return Promise.resolve(existingTaskId);
      }
      const inFlight = resourceRemovalDispatches.current.get(key);
      if (inFlight) return inFlight;
      const trackedRemoval: PendingResourceRemoval =
        removal.kind === "environment"
          ? removal
          : {
              ...removal,
              generation: nextEnvironmentGeneration(
                removal.environmentId,
                "child",
              ),
            };
      const resource =
        removal.kind === "environment"
          ? "environments"
          : removal.kind === "route"
            ? "routes"
            : removal.kind === "entry"
              ? "entries"
              : "scripts";
      const intent = resourceRemovalIntents.current.get(key) ?? {
        removal: trackedRemoval,
        idempotencyKey: `groundplane:${newULID()}`,
      };
      resourceRemovalIntents.current.set(key, intent);
      persistPendingResourceRemovalIntents(resourceRemovalIntents.current);
      update((draft) => {
        draft.environmentDeletionRevision += 1;
      });
      const request = deleteResource(
        resource,
        trackedRemoval.resourceId,
        intent.idempotencyKey,
      )
        .then((accepted) => {
          if (!accepted.task_id)
            throw new Error("Controller response is missing task_id");
          pendingResourceRemovals.current.set(accepted.task_id, trackedRemoval);
          resourceRemovalTasks.current.set(key, accepted.task_id);
          persistPendingResourceRemovals(pendingResourceRemovals.current);
          update((draft) => {
            draft.environmentDeletionRevision += 1;
            if (trackedRemoval.kind === "environment") {
              const environment = findEnvironment(
                draft,
                trackedRemoval.resourceId,
              );
              if (environment) environment.deletionTaskId = accepted.task_id;
            }
          });
          monitorResourceRemoval(accepted.task_id);
          return accepted.task_id;
        })
        .catch((error) => {
          if (isDefinitiveRemovalRequestRejection(error)) {
            resourceRemovalIntents.current.delete(key);
            const taskId = resourceRemovalTasks.current.get(key);
            if (taskId && !pendingResourceRemovals.current.has(taskId))
              resourceRemovalTasks.current.delete(key);
            persistPendingResourceRemovalIntents(
              resourceRemovalIntents.current,
            );
            update((draft) => {
              draft.environmentDeletionRevision += 1;
              if (trackedRemoval.kind === "environment") {
                const environment = findEnvironment(
                  draft,
                  trackedRemoval.resourceId,
                );
                if (environment) environment.deletionTaskId = null;
              }
            });
          }
          throw error;
        })
        .finally(() => {
          if (resourceRemovalDispatches.current.get(key) === request)
            resourceRemovalDispatches.current.delete(key);
        });
      resourceRemovalDispatches.current.set(key, request);
      return request;
    },
    [
      deleteResource,
      isEnvironmentDeletionPending,
      monitorResourceRemoval,
      nextEnvironmentGeneration,
      update,
    ],
  );

  const retryResourceRemoval = useCallback(
    (taskId: string) => {
      let removal = pendingResourceRemovals.current.get(taskId);
      if (!removal) {
        const failure = [...environmentDeletionFailures.current.entries()].find(
          ([, candidate]) => candidate.taskId === taskId,
        );
        if (failure) {
          removal = {
            kind: "environment",
            projectId: failure[1].projectId,
            resourceId: failure[0],
            generation: environmentGenerations.current.get(failure[0]) ?? 0,
          };
          pendingResourceRemovals.current.set(taskId, removal);
          resourceRemovalTasks.current.set(resourceRemovalKey(removal), taskId);
          persistPendingResourceRemovals(pendingResourceRemovals.current);
        }
      }
      if (!removal) return undefined;
      if (removal.kind === "environment") {
        const currentGeneration =
          environmentGenerations.current.get(removal.resourceId) ??
          removal.generation;
        if (currentGeneration !== removal.generation) {
          removal = { ...removal, generation: currentGeneration };
          pendingResourceRemovals.current.set(taskId, removal);
          persistPendingResourceRemovals(pendingResourceRemovals.current);
        }
      }
      const previousFailure =
        removal.kind === "environment"
          ? environmentDeletionFailures.current.get(removal.resourceId)
          : undefined;
      const inFlight = resourceRemovalRetries.current.get(taskId);
      if (inFlight) return inFlight;
      const intent = resourceRemovalRetryIntents.current.get(taskId) ?? {
        removal,
        idempotencyKey: `groundplane:${newULID()}`,
      };
      resourceRemovalRetryIntents.current.set(taskId, intent);
      persistResourceRemovalRetryIntents(resourceRemovalRetryIntents.current);
      const request = retryResource(taskId, intent.idempotencyKey)
        .then((accepted) => {
          if (!accepted.task_id)
            throw new Error("Controller response is missing task_id");
          forgetResourceRemoval(taskId, removal);
          const currentGeneration =
            removal.kind === "environment"
              ? (environmentGenerations.current.get(removal.resourceId) ??
                removal.generation)
              : (environmentGenerations.current.get(removal.environmentId) ??
                removal.generation ??
                0);
          const acceptedGeneration = currentGeneration ?? 0;
          const acceptedRemoval: PendingResourceRemoval = {
            ...removal,
            generation: acceptedGeneration,
          };
          if (previousFailure)
            update((draft) => {
              if (draft.projectError === previousFailure.message)
                draft.projectError = null;
            });
          if (removal.kind === "environment") {
            environmentMutationOutcomes.current.set(removal.resourceId, {
              generation: acceptedGeneration,
              kind: "delete",
              outcome: "pending",
            });
          }
          pendingResourceRemovals.current.set(
            accepted.task_id,
            acceptedRemoval,
          );
          resourceRemovalTasks.current.set(
            resourceRemovalKey(acceptedRemoval),
            accepted.task_id,
          );
          persistPendingResourceRemovals(pendingResourceRemovals.current);
          update((draft) => {
            draft.environmentDeletionRevision += 1;
            if (acceptedRemoval.kind === "environment") {
              const environment = findEnvironment(
                draft,
                acceptedRemoval.resourceId,
              );
              if (environment) environment.deletionTaskId = accepted.task_id;
            }
          });
          resourceRemovalRetryIntents.current.delete(taskId);
          persistResourceRemovalRetryIntents(
            resourceRemovalRetryIntents.current,
          );
          monitorResourceRemoval(accepted.task_id);
          return accepted.task_id;
        })
        .finally(() => {
          if (resourceRemovalRetries.current.get(taskId) === request)
            resourceRemovalRetries.current.delete(taskId);
        });
      resourceRemovalRetries.current.set(taskId, request);
      return request;
    },
    [forgetResourceRemoval, monitorResourceRemoval, retryResource, update],
  );

  const waitForResourceRemoval = useCallback(
    async (taskId: string) => {
      let delayMs = 250;
      while (active.current) {
        const task = await requestResourceRemovalTask(taskId);
        const removal = pendingResourceRemovals.current.get(taskId);
        if (
          removal?.kind === "environment" &&
          task.target !== removal.resourceId
        ) {
          throw new Error(
            `Controller returned deletion Task ${taskId} for a different Environment`,
          );
        }
        if (terminalTaskStatuses.has(task.status)) {
          await reconcileResourceRemoval(taskId, task);
          if (task.status !== "completed")
            throw new Error(
              `Environment deletion Task ${taskId} ${task.status}`,
            );
          return task;
        }
        await new Promise((resolve) => setTimeout(resolve, delayMs));
        delayMs = Math.min(delayMs * 2, 2_000);
      }
      throw new Error("Environment deletion observation stopped");
    },
    [active, reconcileResourceRemoval, requestResourceRemovalTask],
  );

  const refreshEnvironmentDeletion = useCallback(
    async (environmentId: string) => {
      let taskId =
        environmentDeletionFailures.current.get(environmentId)?.taskId;
      let removal = taskId
        ? pendingResourceRemovals.current.get(taskId)
        : undefined;
      if (!removal) {
        const pending = [...pendingResourceRemovals.current.entries()].find(
          ([, candidate]) =>
            candidate.kind === "environment" &&
            candidate.resourceId === environmentId,
        );
        if (pending) {
          taskId = pending[0];
          removal = pending[1];
        }
      }
      if (!taskId || !removal || removal.kind !== "environment")
        throw new Error(
          `Environment deletion ${environmentId} has no observable Task`,
        );
      const task = await requestResourceRemovalTask(taskId);
      await reconcileResourceRemoval(taskId, task);
      if (task.status !== "completed")
        throw new Error(`Environment deletion Task ${taskId} ${task.status}`);
    },
    [reconcileResourceRemoval, requestResourceRemovalTask],
  );

  const observeEnvironmentDeletionTasks = useCallback(
    (projects: Project[]) => {
      for (const project of projects) {
        for (const environment of project.environments ?? []) {
          if (!environment.deletionTaskId) continue;
          if (successfulEnvironmentDeletions.current.has(environment.id))
            continue;
          for (const [taskId, removal] of pendingResourceRemovals.current) {
            if (
              removal.kind === "environment" &&
              removal.resourceId === environment.id &&
              taskId !== environment.deletionTaskId
            ) {
              pendingResourceRemovals.current.delete(taskId);
            }
          }
          const generation =
            environmentGenerations.current.get(environment.id) ?? 0;
          environmentMutationOutcomes.current.set(environment.id, {
            generation,
            kind: "delete",
            outcome: "pending",
          });
          pendingResourceRemovals.current.set(environment.deletionTaskId, {
            kind: "environment",
            projectId: project.id,
            resourceId: environment.id,
            generation,
          });
          resourceRemovalTasks.current.set(
            `environment:${environment.id}`,
            environment.deletionTaskId,
          );
          persistPendingResourceRemovals(pendingResourceRemovals.current);
          monitorResourceRemoval(environment.deletionTaskId);
        }
      }
    },
    [monitorResourceRemoval],
  );

  useEffect(() => {
    active.current = true;
    for (const removal of pendingEnvironmentDeletionReplays(
      resourceRemovalIntents.current,
      resourceRemovalTasks.current,
      pendingResourceRemovals.current,
    )) {
      void dispatchResourceRemoval(removal).catch(() => undefined);
    }
    for (const taskId of pendingResourceRemovals.current.keys())
      monitorResourceRemoval(taskId);
    return () => {
      active.current = false;
      for (const stop of resourceRemovalMonitors.current.values()) stop();
      for (const controller of resourceRemovalTaskControllers.current.values())
        controller.abort();
      for (const controller of resourceRemovalReconciliationControllers.current.values())
        controller.abort();
    };
  }, [active, dispatchResourceRemoval, monitorResourceRemoval]);

  useEffect(() => {
    const failure = [...environmentDeletionFailures.current.values()][0];
    if (failure)
      update((draft) => {
        if (!draft.projectError) draft.projectError = failure.message;
      });
  }, [update]);

  return {
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
  };
}
