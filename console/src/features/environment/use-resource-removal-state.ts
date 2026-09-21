import { useCallback, useRef } from "react";

import {
  findEnvironment,
  type EnvironmentMutationKind,
  type PendingResourceRemoval,
  type TaskResponse,
} from "@/features/environment/environment-removal-model";
import {
  loadEnvironmentDeletionFailures,
  loadPendingResourceRemovalIntents,
  loadPendingResourceRemovals,
  loadResourceRemovalRetryIntents,
  persistEnvironmentDeletionFailures,
  persistPendingResourceRemovalIntents,
  persistPendingResourceRemovals,
  resourceRemovalKey,
  type ResourceRemovalRetryIntent,
} from "@/features/environment/operation-storage";

import type {
  EnvironmentLifecycleDraft,
  EnvironmentLifecycleOptions,
} from "./lifecycle-types";
export function useResourceRemovalState<
  State extends EnvironmentLifecycleDraft,
>(update: EnvironmentLifecycleOptions<State>["update"]) {
  const pendingResourceRemovals = useRef(loadPendingResourceRemovals());
  const resourceRemovalTasks = useRef(
    new Map<string, string>(
      [...pendingResourceRemovals.current].map(([taskId, removal]) => [
        resourceRemovalKey(removal),
        taskId,
      ]),
    ),
  );
  const resourceRemovalDispatches = useRef(new Map<string, Promise<string>>());
  const resourceRemovalRetries = useRef(new Map<string, Promise<string>>());
  const resourceRemovalRetryIntents = useRef<
    Map<string, ResourceRemovalRetryIntent>
  >(loadResourceRemovalRetryIntents());
  const resourceRemovalTaskRequests = useRef(
    new Map<string, Promise<TaskResponse>>(),
  );
  const resourceRemovalTaskControllers = useRef(
    new Map<string, AbortController>(),
  );
  const resourceRemovalObservations = useRef(
    new Map<string, { task: TaskResponse; observedAt: number }>(),
  );
  const resourceRemovalReconciliations = useRef(
    new Map<string, Promise<void>>(),
  );
  const resourceRemovalReconciliationControllers = useRef(
    new Map<string, AbortController>(),
  );
  const resourceRemovalMonitors = useRef(new Map<string, () => void>());
  const resourceRemovalErrors = useRef(new Map<string, string>());
  const resourceRemovalIntents = useRef(loadPendingResourceRemovalIntents());
  const environmentDeletionFailures = useRef(loadEnvironmentDeletionFailures());
  const environmentGenerations = useRef(
    new Map<string, number>(
      [...pendingResourceRemovals.current].flatMap(([, removal]) =>
        removal.kind === "environment"
          ? [[removal.resourceId, removal.generation] as [string, number]]
          : [],
      ),
    ),
  );
  const environmentMutationOutcomes = useRef(
    new Map<
      string,
      {
        generation: number;
        kind: EnvironmentMutationKind;
        outcome: "pending" | "succeeded" | "failed";
      }
    >(
      [...pendingResourceRemovals.current].flatMap(([, removal]) =>
        removal.kind === "environment"
          ? [
              [
                removal.resourceId,
                {
                  generation: removal.generation,
                  kind: "delete" as const,
                  outcome: "pending" as const,
                },
              ],
            ]
          : [],
      ),
    ),
  );
  const successfulEnvironmentDeletions = useRef(new Map<string, string>());
  const nextEnvironmentGeneration = useCallback(
    (environmentId: string, kind: EnvironmentMutationKind = "child") => {
      const generation =
        (environmentGenerations.current.get(environmentId) ?? 0) + 1;
      environmentGenerations.current.set(environmentId, generation);
      environmentMutationOutcomes.current.set(environmentId, {
        generation,
        kind,
        outcome: "pending",
      });
      if (kind === "create" || kind === "delete")
        successfulEnvironmentDeletions.current.delete(environmentId);
      return generation;
    },
    [],
  );
  const settleEnvironmentMutation = useCallback(
    (
      environmentId: string,
      generation: number,
      outcome: "succeeded" | "failed",
    ) => {
      const current = environmentMutationOutcomes.current.get(environmentId);
      if (current?.generation === generation) current.outcome = outcome;
    },
    [],
  );

  const shouldPreserveEnvironmentOnLoad = useCallback(
    (environmentId: string, snapshotGeneration: number) => {
      if (successfulEnvironmentDeletions.current.has(environmentId))
        return false;
      const currentGeneration =
        environmentGenerations.current.get(environmentId) ?? 0;
      const mutation = environmentMutationOutcomes.current.get(environmentId);
      if (
        mutation?.generation === currentGeneration &&
        mutation.kind === "delete" &&
        mutation.outcome === "succeeded"
      )
        return false;
      return (
        currentGeneration === snapshotGeneration ||
        mutation?.generation === currentGeneration
      );
    },
    [],
  );

  const getEnvironmentDeletionFailure = useCallback((environmentId: string) => {
    return environmentDeletionFailures.current.get(environmentId) ?? null;
  }, []);

  const isEnvironmentDeletionPending = useCallback((environmentId: string) => {
    return (
      [...pendingResourceRemovals.current.values()].some(
        (removal) =>
          removal.kind === "environment" &&
          removal.resourceId === environmentId,
      ) ||
      [...resourceRemovalIntents.current.values()].some(
        ({ removal }) =>
          removal.kind === "environment" &&
          removal.resourceId === environmentId,
      )
    );
  }, []);

  const forgetResourceRemoval = useCallback(
    (taskId: string, removal: PendingResourceRemoval) => {
      pendingResourceRemovals.current.delete(taskId);
      resourceRemovalObservations.current.delete(taskId);
      resourceRemovalErrors.current.delete(taskId);
      if (
        removal.kind === "environment" &&
        environmentDeletionFailures.current.get(removal.resourceId)?.taskId ===
          taskId
      ) {
        environmentDeletionFailures.current.delete(removal.resourceId);
        persistEnvironmentDeletionFailures(environmentDeletionFailures.current);
      }
      const key = resourceRemovalKey(removal);
      if (resourceRemovalTasks.current.get(key) === taskId)
        resourceRemovalTasks.current.delete(key);
      if (
        resourceRemovalIntents.current.get(key)?.removal.resourceId ===
        removal.resourceId
      ) {
        resourceRemovalIntents.current.delete(key);
        persistPendingResourceRemovalIntents(resourceRemovalIntents.current);
      }
      persistPendingResourceRemovals(pendingResourceRemovals.current);
      update((draft) => {
        draft.environmentDeletionRevision += 1;
        if (removal.kind === "environment") {
          const environment = findEnvironment(draft, removal.resourceId);
          if (environment?.deletionTaskId === taskId)
            environment.deletionTaskId = null;
        }
      });
    },
    [update],
  );

  const recordResourceRemovalError = useCallback(
    (
      taskId: string,
      removal: PendingResourceRemoval,
      message: string,
      kind: "task" | "refresh" = "task",
    ) => {
      resourceRemovalErrors.current.set(taskId, message);
      if (removal.kind === "environment") {
        environmentDeletionFailures.current.set(removal.resourceId, {
          taskId,
          projectId: removal.projectId,
          message,
          kind,
        });
        persistEnvironmentDeletionFailures(environmentDeletionFailures.current);
        settleEnvironmentMutation(
          removal.resourceId,
          removal.generation,
          "failed",
        );
        update((draft) => {
          draft.projectError = message;
        });
      }
    },
    [settleEnvironmentMutation, update],
  );

  return {
    pendingResourceRemovals,
    resourceRemovalTasks,
    resourceRemovalDispatches,
    resourceRemovalRetries,
    resourceRemovalRetryIntents,
    resourceRemovalTaskRequests,
    resourceRemovalTaskControllers,
    resourceRemovalObservations,
    resourceRemovalReconciliations,
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
    recordResourceRemovalError,
  };
}
