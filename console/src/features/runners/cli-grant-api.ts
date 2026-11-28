import type { components } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { newULID } from "@/lib/utils";

export type GrantAction = components["schemas"]["RunnerCLIGrantAction"];
export type GrantScope = components["schemas"]["RunnerCLIGrantScope"];
export type GrantWrite = components["schemas"]["RunnerCLIGrantWrite"];
type GrantStatus = components["schemas"]["RunnerCLIGrantStatus"];
export type GrantObservation = { status: GrantStatus; revision: string };

type Intent = { key: string; revision: string; body?: GrantWrite };
const pending = new Map<string, Intent>();
export const pendingCLIGrant = (runnerId: string) => pending.get(runnerId);

export function captureCLIGrant(
  runnerId: string,
  revision: string,
  body?: GrantWrite,
) {
  const previous = pending.get(runnerId);
  if (previous) return previous;
  const intent = { key: newULID(), revision, body };
  pending.set(runnerId, intent);
  return intent;
}

export function releaseCLIGrant(runnerId: string, intent: Intent) {
  if (pending.get(runnerId) === intent) pending.delete(runnerId);
}

const path = (runnerId: string) =>
  `/runners/${encodeURIComponent(runnerId)}/cli-grant`;

export async function readCLIGrant(runnerId: string, signal: AbortSignal) {
  let revision = "";
  const status = await controllerRequest<GrantStatus>(path(runnerId), 200, {
    signal,
    onResponseHeaders: (headers) => {
      revision = headers.get("ETag") ?? "";
    },
  });
  if (status.runner_id !== runnerId || !/^"(0|[1-9]\d*)"$/.test(revision)) {
    throw new Error(
      "The Controller did not return a valid revision for this Runner grant.",
    );
  }
  return { status, revision };
}

export async function submitCLIGrant(runnerId: string, intent: Intent) {
  const options = { idempotencyKey: intent.key, ifMatch: intent.revision };
  if (!intent.body) {
    await controllerRequest<void>(path(runnerId), 204, {
      ...options,
      method: "DELETE",
    });
    return;
  }
  const status = await controllerRequest<GrantStatus>(path(runnerId), 200, {
    ...options,
    method: "PUT",
    body: intent.body,
  });
  if (status.runner_id !== runnerId || status.revision <= 0)
    throw new Error("The Controller did not confirm the Runner grant update.");
}
