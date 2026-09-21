import type { TaskResponse } from "./api";
import {
  abortTask as requestTaskAbort,
  requestTask,
  retryTask as requestTaskRetry,
} from "./api";
import { waitForRequest } from "@/lib/controller-json-request";

export type TaskActions = {
  getTask: (taskId: string, signal?: AbortSignal) => Promise<TaskResponse>;
  retryTask: (taskId: string) => Promise<string>;
  abortTask: (taskId: string) => Promise<void>;
};

type TaskActionOptions = {
  isResourceRemovalTask: (taskId: string) => boolean;
  requestResourceRemovalTask: (taskId: string) => Promise<TaskResponse>;
  monitorResourceRemoval: (taskId: string) => void;
  reconcileResourceRemoval: (
    taskId: string,
    task: TaskResponse,
  ) => Promise<void>;
  retryResourceRemoval: (taskId: string) => Promise<string> | undefined;
  reconcileZoneRemoval: (taskId: string, task: TaskResponse) => void;
  reconcileConnectorRemoval: (
    taskId: string,
    task: TaskResponse,
  ) => Promise<void>;
};

export function createTaskActions({
  isResourceRemovalTask,
  requestResourceRemovalTask,
  monitorResourceRemoval,
  reconcileResourceRemoval,
  retryResourceRemoval,
  reconcileZoneRemoval,
  reconcileConnectorRemoval,
}: TaskActionOptions): TaskActions {
  return {
    getTask: async (taskId, signal) => {
      const task = isResourceRemovalTask(taskId)
        ? await waitForRequest(requestResourceRemovalTask(taskId), signal)
        : await requestTask(taskId, signal);
      reconcileZoneRemoval(taskId, task);
      if (isResourceRemovalTask(taskId)) {
        monitorResourceRemoval(taskId);
        void reconcileResourceRemoval(taskId, task).catch(() => undefined);
      }
      await reconcileConnectorRemoval(taskId, task);
      return task;
    },
    retryTask: (taskId) =>
      retryResourceRemoval(taskId) ?? requestTaskRetry(taskId),
    abortTask: requestTaskAbort,
  };
}
