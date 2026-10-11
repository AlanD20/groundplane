import type { components, operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { requireTaskId } from "@/features/task/journal-model";
import { newULID } from "@/lib/utils";

export type RunnerCheckRequest = components["schemas"]["RunnerCheckRequest"];
export type RunnerCheckResult = components["schemas"]["RunnerCheckResult"];
type RunnerRead =
  operations["runner.show"]["responses"][200]["content"]["application/json"];
type Accepted =
  operations["runner.check"]["responses"][202]["content"]["application/json"];
type Submission = { key: string; body?: RunnerCheckRequest };

// Only unresolved operator submissions are retained across closing/reopening
// the inspector. No background retries or provider credentials live here.
const submissions = new Map<string, Submission>();
export const pendingRunnerCheck = (runnerId: string) =>
  submissions.get(runnerId);
export function runnerCheckSubmission(runnerId: string) {
  let pending = submissions.get(runnerId);
  if (!pending) {
    pending = { key: newULID() };
    submissions.set(runnerId, pending);
  }
  return pending;
}
export function releaseRunnerCheck(runnerId: string, pending: Submission) {
  if (submissions.get(runnerId) === pending) submissions.delete(runnerId);
}

export async function runnerCheckRequest(
  runnerId: string,
  signal: AbortSignal,
): Promise<RunnerCheckRequest> {
  const runner = await controllerRequest<RunnerRead>(
    `/runners/${encodeURIComponent(runnerId)}`,
    200,
    { signal },
  );
  if (
    runner.id !== runnerId ||
    runner.lifecycle !== "ready" ||
    runner.revision === undefined ||
    !Number.isSafeInteger(runner.revision) ||
    runner.revision <= 0
  )
    throw new Error(
      "Runner must be registered and ready. Refresh before testing.",
    );
  return { revision: runner.revision };
}

export async function startRunnerCheck(
  runnerId: string,
  body: RunnerCheckRequest,
  key: string,
  signal: AbortSignal,
) {
  const result = await controllerRequest<Accepted>(
    `/runners/${encodeURIComponent(runnerId)}/checks`,
    202,
    { method: "POST", body, idempotencyKey: key, signal },
  );
  return requireTaskId(result, "Runner check");
}

export async function getRunnerCheck(
  runnerId: string,
  taskId: string,
  signal: AbortSignal,
) {
  const query = taskId ? `?${new URLSearchParams({ task: taskId })}` : "";
  const result = await controllerRequest<RunnerCheckResult>(
    `/runners/${encodeURIComponent(runnerId)}/checks${query}`,
    200,
    { signal },
  );
  if (
    result.configuration.runner_id !== runnerId ||
    (taskId && result.task_id !== taskId)
  )
    throw new Error(
      "Controller returned a check for a different Runner or Task.",
    );
  return result;
}
