import { controllerRequest } from "@/lib/controller-json-request";
import type { operations } from "@/lib/api.generated";

type MonitoringOperation = operations["monitoring.show"];
export type MonitoringScopeKind =
  MonitoringOperation["parameters"]["path"]["scope"];
export type MonitoringRange = NonNullable<
  NonNullable<MonitoringOperation["parameters"]["query"]>["range"]
>;
export type MonitoringResponse =
  operations["monitoring.show"]["responses"][200]["content"]["application/json"];

export type MonitoringRequest = {
  scope: MonitoringScopeKind;
  id: string;
  range: MonitoringRange;
  points?: number;
  limit?: number;
  cursor?: string;
  consumerCursor?: string;
};

export function monitoringKey(request: MonitoringRequest): string {
  return [
    request.scope,
    request.id,
    request.range,
    request.points ?? "",
    request.limit ?? "",
    request.cursor ?? "",
    request.consumerCursor ?? "",
  ].join("/");
}

export function getMonitoring(
  request: MonitoringRequest,
  signal: AbortSignal,
): Promise<MonitoringResponse> {
  const query = new URLSearchParams({ range: request.range });
  if (request.points) query.set("points", String(request.points));
  if (request.limit) query.set("limit", String(request.limit));
  if (request.cursor) query.set("cursor", request.cursor);
  if (request.consumerCursor)
    query.set("consumer_cursor", request.consumerCursor);
  return controllerRequest<MonitoringResponse>(
    `/monitoring/${encodeURIComponent(request.scope)}/${encodeURIComponent(request.id)}?${query}`,
    200,
    { signal },
  );
}
