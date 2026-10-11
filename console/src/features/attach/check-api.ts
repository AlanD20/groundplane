import type { operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { requireTaskId } from "@/features/task/journal-model";
import type { AttachPageResponse } from "./api";

export type BackingCheckRequest =
  operations["attach.check"]["requestBody"]["content"]["application/json"];
export type BackingCheckResult =
  operations["attach.check.show"]["responses"][200]["content"]["application/json"];
type Accepted =
  operations["attach.check"]["responses"][202]["content"]["application/json"];

export async function backingCheckRequest(
  environmentId: string,
  attachId: string,
  grantId: string,
  signal: AbortSignal,
): Promise<BackingCheckRequest> {
  let cursor = "";
  do {
    const query = new URLSearchParams({
      environment: environmentId,
      limit: "200",
    });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<AttachPageResponse>(
      `/attaches?${query}`,
      200,
      { signal },
    );
    const attach = page.items?.find((item) => item.id === attachId);
    if (attach) {
      if (
        !Number.isSafeInteger(attach.revision) ||
        !attach.revision ||
        attach.revision <= 0
      )
        throw new Error(
          "The Attach snapshot has no valid revision. Refresh before testing.",
        );
      return {
        attach_id: attach.id,
        revision: attach.revision,
        grant_id: grantId || undefined,
      };
    }
    cursor = page.next_cursor ?? "";
  } while (cursor);
  throw new Error(
    "This Attach no longer exists in the Environment. Refresh the connections.",
  );
}

export async function startBackingCheck(
  environmentId: string,
  input: BackingCheckRequest,
  key: string,
  signal: AbortSignal,
) {
  const query = new URLSearchParams({ environment: environmentId });
  const result = await controllerRequest<Accepted>(
    `/attaches/checks?${query}`,
    202,
    { method: "POST", body: input, idempotencyKey: key, signal },
  );
  return requireTaskId(result, "Backing access check");
}

export async function getBackingCheck(
  environmentId: string,
  signal: AbortSignal,
  selection: { taskId: string } | { attachId: string; grantId: string },
) {
  const query = new URLSearchParams({ environment: environmentId });
  if ("taskId" in selection) query.set("task", selection.taskId);
  else {
    query.set("attach", selection.attachId);
    if (selection.grantId) query.set("grant", selection.grantId);
  }
  const result = await controllerRequest<BackingCheckResult>(
    `/attaches/checks?${query}`,
    200,
    { signal },
  );
  if (
    result.environment_id !== environmentId ||
    ("taskId" in selection
      ? result.task_id !== selection.taskId
      : result.configuration.attach_id !== selection.attachId ||
        (result.configuration.grant_id ?? "") !== selection.grantId)
  )
    throw new Error(
      "Controller returned an access check for a different selection.",
    );
  return result;
}
