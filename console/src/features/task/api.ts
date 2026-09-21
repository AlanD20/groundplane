import type { operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";

export type TaskResponse =
  operations["task.show"]["responses"][200]["content"]["application/json"];

type TaskRetryResponse =
  operations["task.retry"]["responses"][202]["content"]["application/json"];
type TaskAbortResponse =
  operations["task.abort"]["responses"][202]["content"]["application/json"];

export function requestTask(
  taskId: string,
  signal?: AbortSignal,
): Promise<TaskResponse> {
  return controllerRequest<TaskResponse>(
    `/tasks/${encodeURIComponent(taskId)}`,
    200,
    { signal },
  );
}

export async function retryTask(
  taskId: string,
  idempotencyKey?: string,
): Promise<string> {
  const accepted = await controllerRequest<TaskRetryResponse>(
    `/tasks/${encodeURIComponent(taskId)}/retry`,
    202,
    { method: "POST", idempotencyKey },
  );
  if (!accepted.task_id)
    throw new Error("Controller response is missing task_id");
  return accepted.task_id;
}

export async function abortTask(taskId: string): Promise<void> {
  const accepted = await controllerRequest<TaskAbortResponse>(
    `/tasks/${encodeURIComponent(taskId)}/abort`,
    202,
    { method: "POST" },
  );
  if (accepted.task_id !== taskId)
    throw new Error("Task abort returned a different Task id");
}
