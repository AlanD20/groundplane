import { useCallback, useEffect, useRef } from "react";
import type { operations } from "@/lib/api.generated";
import type {
  Connector,
  ConnectorCreateInput,
  ConnectorCredentialInput,
} from "@/lib/types";
import type { ConnectorMutationIntent } from "@/lib/connector-intent";
import { controllerRequest } from "@/lib/controller-json-request";
import { isTaskNotFoundError } from "@/lib/controller-request-errors";
import {
  loadPendingConnectorRemovals,
  persistPendingConnectorRemovals,
} from "./removal-storage";
import {
  connectorFromAPI,
  listEnvironmentConnectors,
  listAllConnectors,
  type ConnectorResponse,
  type ConnectorCreateRequest,
  type ConnectorTaskAccepted,
} from "./api";
type TaskResponse =
  operations["task.show"]["responses"][200]["content"]["application/json"];
export type ConnectorState = {
  connectors: Connector[];
  connectorsLoading: boolean;
  connectorError: string | null;
};
export type ConnectorActions = {
  addConnector: (
    c: ConnectorCreateInput,
    intent: ConnectorMutationIntent,
  ) => Promise<Connector>;
  removeConnector: (
    id: string,
    intent: ConnectorMutationIntent,
  ) => Promise<string>;
};
type UpdateConnectors = (change: (draft: ConnectorState) => void) => void;
export function useConnectorStore(
  state: ConnectorState & {
    projectsLoading: boolean;
    backingProjectsLoading: boolean;
  },
  update: UpdateConnectors,
  connectorEnvironmentIds: string,
) {
  const pendingConnectorRemovals = useRef(loadPendingConnectorRemovals());
  const connectorRemovalPolls = useRef(new Set<string>());
  const connectorRemovalRequests = useRef(new Map<string, Promise<string>>());
  const connectorFullLoadGeneration = useRef(0);
  const connectorEnvironmentGenerations = useRef(new Map<string, number>());
  const connectorRemovalPollBackoff = useRef(
    new Map<string, { failures: number; nextAttemptAt: number }>(),
  );

  const clearPendingConnectorRemoval = useCallback((taskId: string) => {
    pendingConnectorRemovals.current.delete(taskId);
    connectorRemovalPollBackoff.current.delete(taskId);
    persistPendingConnectorRemovals(pendingConnectorRemovals.current);
  }, []);

  const refreshConnectorEnvironment = useCallback(
    async (environmentId: string) => {
      const refreshGeneration =
        (connectorEnvironmentGenerations.current.get(environmentId) ?? 0) + 1;
      connectorEnvironmentGenerations.current.set(
        environmentId,
        refreshGeneration,
      );
      try {
        const refreshed = await listEnvironmentConnectors(
          environmentId,
          new AbortController().signal,
        );
        if (
          connectorEnvironmentGenerations.current.get(environmentId) !==
          refreshGeneration
        ) {
          return;
        }
        update((draft) => {
          draft.connectors = [
            ...draft.connectors.filter(
              (connector) => connector.scopeRef !== environmentId,
            ),
            ...refreshed,
          ];
          draft.connectorError = null;
        });
      } catch (error) {
        if (
          connectorEnvironmentGenerations.current.get(environmentId) !==
          refreshGeneration
        ) {
          return;
        }
        update((draft) => {
          draft.connectorError =
            error instanceof Error
              ? error.message
              : "Unable to refresh Connectors";
        });
      }
    },
    [update],
  );

  const reconcileConnectorRemoval = useCallback(
    async (taskId: string, task: TaskResponse) => {
      const pending = pendingConnectorRemovals.current.get(taskId);
      if (!pending) return;
      if (
        task.type !== "remove" ||
        task.target !== pending.connectorId ||
        task.environment_id !== pending.environmentId
      ) {
        clearPendingConnectorRemoval(taskId);
        await refreshConnectorEnvironment(pending.environmentId);
        return;
      }
      if (
        !["completed", "failed", "timed_out", "aborted"].includes(task.status)
      )
        return;
      clearPendingConnectorRemoval(taskId);
      if (task.status !== "completed") return;
      update((draft) => {
        draft.connectors = draft.connectors.filter(
          (candidate) => candidate.id !== pending.connectorId,
        );
        draft.connectorError = null;
      });
      await refreshConnectorEnvironment(pending.environmentId);
    },
    [clearPendingConnectorRemoval, refreshConnectorEnvironment, update],
  );

  useEffect(() => {
    const timer = setInterval(() => {
      const now = Date.now();
      for (const taskId of pendingConnectorRemovals.current.keys()) {
        const backoff = connectorRemovalPollBackoff.current.get(taskId);
        if (
          connectorRemovalPolls.current.has(taskId) ||
          (backoff && backoff.nextAttemptAt > now)
        ) {
          continue;
        }
        connectorRemovalPolls.current.add(taskId);
        void controllerRequest<TaskResponse>(
          `/tasks/${encodeURIComponent(taskId)}`,
          200,
        )
          .then(async (task) => {
            connectorRemovalPollBackoff.current.delete(taskId);
            update((draft) => {
              draft.connectorError = null;
            });
            await reconcileConnectorRemoval(taskId, task);
          })
          .catch(async (error: unknown) => {
            const pending = pendingConnectorRemovals.current.get(taskId);
            if (!pending) return;
            if (isTaskNotFoundError(error)) {
              clearPendingConnectorRemoval(taskId);
              await refreshConnectorEnvironment(pending.environmentId);
              return;
            }
            const failures =
              (connectorRemovalPollBackoff.current.get(taskId)?.failures ?? 0) +
              1;
            const delay = Math.min(
              30_000,
              1_000 * 2 ** Math.min(failures - 1, 5),
            );
            connectorRemovalPollBackoff.current.set(taskId, {
              failures,
              nextAttemptAt: Date.now() + delay,
            });
            update((draft) => {
              draft.connectorError =
                error instanceof Error
                  ? error.message
                  : "Unable to observe Connector removal Task";
            });
          })
          .finally(() => connectorRemovalPolls.current.delete(taskId));
      }
    }, 1000);
    return () => clearInterval(timer);
  }, [
    clearPendingConnectorRemoval,
    reconcileConnectorRemoval,
    refreshConnectorEnvironment,
    update,
  ]);

  useEffect(() => {
    if (state.projectsLoading || state.backingProjectsLoading) return;
    const controller = new AbortController();
    const environmentIds = connectorEnvironmentIds
      ? connectorEnvironmentIds.split(",")
      : [];
    const loadGeneration = connectorFullLoadGeneration.current + 1;
    connectorFullLoadGeneration.current = loadGeneration;
    const environmentGenerations = new Map(
      environmentIds.map((environmentId) => [
        environmentId,
        connectorEnvironmentGenerations.current.get(environmentId) ?? 0,
      ]),
    );
    void listAllConnectors(environmentIds, controller.signal).then(
      (connectors) => {
        if (connectorFullLoadGeneration.current !== loadGeneration) return;
        const unchangedEnvironments = new Set(
          environmentIds.filter(
            (environmentId) =>
              (connectorEnvironmentGenerations.current.get(environmentId) ??
                0) === environmentGenerations.get(environmentId),
          ),
        );
        const requestedEnvironments = new Set(environmentIds);
        update((draft) => {
          draft.connectors = [
            ...draft.connectors.filter(
              (connector) =>
                requestedEnvironments.has(connector.scopeRef) &&
                !unchangedEnvironments.has(connector.scopeRef),
            ),
            ...connectors.filter((connector) =>
              unchangedEnvironments.has(connector.scopeRef),
            ),
          ];
          draft.connectorsLoading = false;
          draft.connectorError = null;
        });
      },
      (error: unknown) => {
        if (
          controller.signal.aborted ||
          connectorFullLoadGeneration.current !== loadGeneration
        ) {
          return;
        }
        update((draft) => {
          draft.connectorsLoading = false;
          draft.connectorError =
            error instanceof Error
              ? error.message
              : "Unable to load Connectors";
        });
      },
    );
    return () => controller.abort();
  }, [
    connectorEnvironmentIds,
    state.backingProjectsLoading,
    state.projectsLoading,
    update,
  ]);

  const actions: ConnectorActions = {
    addConnector: async (connector, intent) => {
      const toAPI = (credential: ConnectorCredentialInput) =>
        credential.kind === "ref"
          ? { secret_ref: credential.name }
          : { value: credential.value };
      const body: ConnectorCreateRequest = {
        name: connector.name,
        kind: connector.kind,
        endpoint: connector.endpoint,
        bucket: connector.bucket,
        prefix: connector.prefix || undefined,
        region: connector.region,
        path_style: connector.pathStyle,
        credentials: {
          access_key: toAPI(connector.credentials.accessKey),
          secret_key: toAPI(connector.credentials.secretKey),
        },
      };
      const query = new URLSearchParams({ environment: connector.scopeRef });
      try {
        const created = connectorFromAPI(
          await controllerRequest<ConnectorResponse>(
            `/connectors?${query}`,
            201,
            {
              method: "POST",
              body,
              idempotencyKey: intent.idempotencyKey,
            },
          ),
        );
        connectorEnvironmentGenerations.current.set(
          connector.scopeRef,
          (connectorEnvironmentGenerations.current.get(connector.scopeRef) ??
            0) + 1,
        );
        update((draft) => {
          draft.connectors = draft.connectors.filter(
            (candidate) => candidate.id !== created.id,
          );
          draft.connectors.push(created);
          draft.connectorError = null;
        });
        return created;
      } catch (error) {
        update((draft) => {
          draft.connectorError =
            error instanceof Error
              ? error.message
              : "Unable to create Connector";
        });
        throw error;
      }
    },
    removeConnector: async (id, intent) => {
      const pendingTask = [...pendingConnectorRemovals.current.entries()].find(
        ([, pending]) => pending.connectorId === id,
      );
      if (pendingTask) return pendingTask[0];
      const inFlight = connectorRemovalRequests.current.get(id);
      if (inFlight) return inFlight;
      const request = (async () => {
        try {
          const connector = state.connectors.find(
            (candidate) => candidate.id === id,
          );
          if (!connector) throw new Error("Connector was not found");
          const accepted = await controllerRequest<ConnectorTaskAccepted>(
            `/connectors/${encodeURIComponent(id)}`,
            202,
            { method: "DELETE", idempotencyKey: intent.idempotencyKey },
          );
          if (!accepted.task_id)
            throw new Error("Controller response is missing task_id");
          pendingConnectorRemovals.current.set(accepted.task_id, {
            connectorId: id,
            environmentId: connector.scopeRef,
          });
          persistPendingConnectorRemovals(pendingConnectorRemovals.current);
          update((draft) => {
            draft.connectorError = null;
          });
          return accepted.task_id;
        } catch (error) {
          update((draft) => {
            draft.connectorError =
              error instanceof Error
                ? error.message
                : "Unable to remove Connector";
          });
          throw error;
        }
      })();
      connectorRemovalRequests.current.set(id, request);
      try {
        return await request;
      } finally {
        if (connectorRemovalRequests.current.get(id) === request) {
          connectorRemovalRequests.current.delete(id);
        }
      }
    },
  };
  return { actions, reconcileConnectorRemoval };
}
