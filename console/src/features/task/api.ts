import type { operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { observeTerminalTask } from "./terminal-observation";
import { captureRetryKey, forgetRetryKey } from "./retry-request";

export type TaskResponse =
  operations["task.show"]["responses"][200]["content"]["application/json"];
export type TaskWork = TaskResponse["work"]["items"][number];
export type TaskAttemptEvents =
  operations["task.attempt-events"]["responses"][200]["content"]["application/json"];

export function requestTaskAttemptEvents(
  taskId: string,
  workId: string,
  attemptId: string,
  revision: number,
  after = 0,
  signal?: AbortSignal,
): Promise<TaskAttemptEvents> {
  const query = new URLSearchParams({
    revision: String(revision),
    after: String(after),
  });
  return controllerRequest<TaskAttemptEvents>(
    `/tasks/${encodeURIComponent(taskId)}/work/${encodeURIComponent(workId)}/attempts/${encodeURIComponent(attemptId)}/events?${query}`,
    200,
    { signal },
  );
}
export type TaskStages =
  operations["task.stages"]["responses"][200]["content"]["application/json"];

export function requestTaskStages(
  taskId: string,
  workId: string,
  attemptId: string,
  revision: number,
  signal?: AbortSignal,
): Promise<TaskStages> {
  return controllerRequest<TaskStages>(
    `/tasks/${encodeURIComponent(taskId)}/work/${encodeURIComponent(workId)}/attempts/${encodeURIComponent(attemptId)}/stages?revision=${revision}`,
    200,
    { signal },
  );
}
export type TaskAttempts =
  operations["task.attempts"]["responses"][200]["content"]["application/json"];
export type TaskImage =
  operations["task.image"]["responses"][200]["content"]["application/json"];

type TaskRetryResponse =
  operations["task.retry"]["responses"][202]["content"]["application/json"];
export type TaskRetryRequest =
  operations["task.retry"]["requestBody"]["content"]["application/json"];
type TaskAbortResponse =
  operations["task.abort"]["responses"][202]["content"]["application/json"];

export async function requestTask(
  taskId: string,
  signal?: AbortSignal,
  work?: { limit: number; after?: string; revision?: number },
): Promise<TaskResponse> {
  const query = new URLSearchParams();
  if (work) {
    query.set("limit", String(work.limit));
    if (work.after) query.set("after", work.after);
    if (work.revision) query.set("revision", String(work.revision));
  }
  const task = await controllerRequest<TaskResponse>(
    `/tasks/${encodeURIComponent(taskId)}${query.size ? `?${query}` : ""}`,
    200,
    { signal },
  );
  if (!signal?.aborted) observeTerminalTask(task);
  return task;
}

export function requestTaskAttempts(
  taskId: string,
  workId: string,
  revision: number,
  after = 0,
  signal?: AbortSignal,
): Promise<TaskAttempts> {
  const query = new URLSearchParams({
    revision: String(revision),
    after: String(after),
    limit: "25",
  });
  return controllerRequest<TaskAttempts>(
    `/tasks/${encodeURIComponent(taskId)}/work/${encodeURIComponent(workId)}/attempts?${query}`,
    200,
    { signal },
  );
}

export function requestTaskImage(
  taskId: string,
  workId: string,
  revision: number,
  signal?: AbortSignal,
): Promise<TaskImage> {
  return controllerRequest<TaskImage>(
    `/tasks/${encodeURIComponent(taskId)}/work/${encodeURIComponent(workId)}/image?revision=${revision}`,
    200,
    { signal },
  );
}

export async function retryTask(
  taskId: string,
  input: TaskRetryRequest,
  idempotencyKey?: string,
): Promise<string> {
  const key = idempotencyKey ?? captureRetryKey(taskId, input);
  const accepted = await controllerRequest<TaskRetryResponse>(
    `/tasks/${encodeURIComponent(taskId)}/retry`,
    202,
    { method: "POST", body: input, idempotencyKey: key },
  );
  if (!accepted.task_id)
    throw new Error("Controller response is missing task_id");
  if (accepted.task_id !== taskId)
    throw new Error("Retry returned a different Task identity");
  if (!idempotencyKey) forgetRetryKey(taskId, input, key);
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
