import type { operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { requireTaskId } from "@/features/task/journal-model";

export type EdgeCheckRequest =
  operations["route.check"]["requestBody"]["content"]["application/json"];
export type EdgeCheckResult =
  operations["route.check.show"]["responses"][200]["content"]["application/json"];
type Accepted =
  operations["route.check"]["responses"][202]["content"]["application/json"];
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

export function tunnelCheckRequest(
  selection: TunnelCheckSelection,
  after: string[],
): TunnelCheckRequest {
  return {
    revision: 0,
    origin_service_id: selection.originServiceId,
    origin_port: selection.port,
    hostname: selection.hostname,
    path: selection.path,
    after: [...after],
  };
}

export function edgeCheckRequest(
  routeId: string,
  connectorId: string,
  path: string,
  after: string[],
): EdgeCheckRequest {
  return {
    route_id: routeId,
    revision: 0,
    connector_id: connectorId,
    path: path || undefined,
    after: [...after],
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
