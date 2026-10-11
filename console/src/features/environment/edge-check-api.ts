import type { operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { requireTaskId } from "@/features/task/journal-model";

export type EdgeCheckRequest =
  operations["route.check"]["requestBody"]["content"]["application/json"];
export type EdgeCheckResult =
  operations["route.check.show"]["responses"][200]["content"]["application/json"];
type RouteSnapshot =
  operations["route.show"]["responses"][200]["content"]["application/json"];
type Accepted =
  operations["route.check"]["responses"][202]["content"]["application/json"];
type ComponentSnapshot =
  operations["component.show"]["responses"][200]["content"]["application/json"];
export type TunnelCheckRequest =
  operations["component.tunnel-check"]["requestBody"]["content"]["application/json"];
export type TunnelCheckSelection = {
  originServiceId: string;
  port: number;
  hostname: string;
  path: string;
};
export type EdgeCheckSubmission =
  | { kind: "route"; body: EdgeCheckRequest }
  | { kind: "tunnel"; connectorId: string; body: TunnelCheckRequest };

export async function tunnelCheckRequest(
  environmentId: string,
  connectorId: string,
  selection: TunnelCheckSelection,
  signal: AbortSignal,
): Promise<TunnelCheckRequest> {
  const connector = await controllerRequest<ComponentSnapshot>(
    `/components/${encodeURIComponent(connectorId)}`,
    200,
    { signal },
  );
  if (
    connector.id !== connectorId ||
    connector.environment_id !== environmentId ||
    connector.kind !== "cloudflare-tunnel" ||
    !connector.enabled ||
    !Number.isSafeInteger(connector.revision) ||
    connector.revision <= 0
  )
    throw new Error(
      "The enabled Tunnel snapshot is unavailable. Refresh before testing access.",
    );
  return {
    revision: connector.revision,
    origin_service_id: selection.originServiceId,
    origin_port: selection.port,
    hostname: selection.hostname,
    path: selection.path,
  };
}

export async function edgeCheckRequest(
  environmentId: string,
  routeId: string,
  connectorId: string,
  path: string,
  signal: AbortSignal,
): Promise<EdgeCheckRequest> {
  const route = await controllerRequest<RouteSnapshot>(
    `/routes/${encodeURIComponent(routeId)}`,
    200,
    { signal },
  );
  if (
    route.id !== routeId ||
    route.environment_id !== environmentId ||
    !Number.isSafeInteger(route.revision) ||
    route.revision <= 0
  )
    throw new Error(
      "The Route snapshot is unavailable. Refresh before testing public access.",
    );
  return {
    route_id: routeId,
    revision: route.revision,
    connector_id: connectorId,
    path: path || undefined,
  };
}

export async function startEdgeCheck(
  environmentId: string,
  submission: EdgeCheckSubmission,
  key: string,
  signal: AbortSignal,
) {
  const query = new URLSearchParams({ environment: environmentId });
  const endpoint =
    submission.kind === "route"
      ? "/routes/checks"
      : `/components/${encodeURIComponent(submission.connectorId)}/tunnel-checks`;
  const result = await controllerRequest<Accepted>(
    `${endpoint}?${query}`,
    202,
    { method: "POST", body: submission.body, idempotencyKey: key, signal },
  );
  return requireTaskId(result, "Public access check");
}

export async function getEdgeCheck(
  environmentId: string,
  signal: AbortSignal,
  selection: { taskId: string } | { routeId: string } | { connectorId: string },
) {
  const query = new URLSearchParams({ environment: environmentId });
  if ("taskId" in selection) query.set("task", selection.taskId);
  else if ("routeId" in selection) query.set("route", selection.routeId);
  const endpoint =
    "connectorId" in selection
      ? `/components/${encodeURIComponent(selection.connectorId)}/tunnel-checks`
      : "/routes/checks";
  const result = await controllerRequest<EdgeCheckResult>(
    `${endpoint}?${query}`,
    200,
    { signal },
  );
  if (
    result.environment_id !== environmentId ||
    ("taskId" in selection
      ? result.task_id !== selection.taskId
      : "routeId" in selection
        ? result.configuration.route_id !== selection.routeId
        : result.configuration.connector_id !== selection.connectorId ||
          !!result.configuration.route_id)
  )
    throw new Error(
      "Controller returned a public access check for a different selection.",
    );
  return result;
}
