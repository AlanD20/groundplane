import type { components, operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { requireTaskId } from "@/features/task/journal-model";

export type DNSCheckQuery = components["schemas"]["DNSCheckQuery"];
export type DNSCheckRequest = components["schemas"]["DNSCheckRequest"];
export type DNSCheckResult = components["schemas"]["DNSCheckResult"];
export type DNSCheckAnswer = components["schemas"]["DNSCheckAnswer"];
type Component =
  operations["component.show"]["responses"][200]["content"]["application/json"];
type Accepted =
  operations["component.dns-check"]["responses"][202]["content"]["application/json"];

export async function dnsCheckRequest(
  componentId: string,
  queries: DNSCheckQuery[],
  signal: AbortSignal,
): Promise<DNSCheckRequest> {
  const component = await controllerRequest<Component>(
    `/components/${encodeURIComponent(componentId)}`,
    200,
    { signal },
  );
  if (
    component.id !== componentId ||
    component.kind !== "coredns" ||
    component.owner !== "platform" ||
    !Number.isSafeInteger(component.revision) ||
    component.revision <= 0
  )
    throw new Error(
      "The CoreDNS snapshot is unavailable. Refresh before testing.",
    );
  return {
    revision: component.revision,
    queries: queries.map((query) => ({ ...query })),
  };
}

export async function startDNSCheck(
  componentId: string,
  body: DNSCheckRequest,
  key: string,
  signal: AbortSignal,
) {
  const result = await controllerRequest<Accepted>(
    `/components/${encodeURIComponent(componentId)}/dns-checks`,
    202,
    { method: "POST", body, idempotencyKey: key, signal },
  );
  return requireTaskId(result, "DNS check");
}

export async function getDNSCheck(
  componentId: string,
  taskId: string,
  signal: AbortSignal,
) {
  const query = taskId ? `?${new URLSearchParams({ task: taskId })}` : "";
  const result = await controllerRequest<DNSCheckResult>(
    `/components/${encodeURIComponent(componentId)}/dns-checks${query}`,
    200,
    { signal },
  );
  if (
    result.configuration.component_id !== componentId ||
    (taskId && result.task_id !== taskId)
  )
    throw new Error(
      "Controller returned a DNS check for a different selection.",
    );
  return result;
}
