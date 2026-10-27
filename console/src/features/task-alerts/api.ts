import type { components } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";

export type AlertSettings = components["schemas"]["TaskAlertSettings"];
export type AlertWrite = components["schemas"]["TaskAlertSettingsWrite"];
const path = (scope: string, test = false) =>
  `/task-alerts${test ? "/test" : ""}?${scope === "platform" ? "platform=true" : `environment=${encodeURIComponent(scope)}`}`;

export function getTaskAlerts(scope: string, signal: AbortSignal) {
  return controllerRequest<AlertSettings>(path(scope), 200, { signal });
}
export function setTaskAlerts(scope: string, body: AlertWrite, key: string) {
  return controllerRequest<AlertSettings>(path(scope), 200, {
    method: "PUT",
    body,
    idempotencyKey: key,
  });
}
export function testTaskAlerts(scope: string, revision: string, key: string) {
  return controllerRequest<components["schemas"]["TaskAlertTestAccepted"]>(
    path(scope, true),
    200,
    {
      method: "POST",
      body: { expected_revision: revision },
      idempotencyKey: key,
    },
  );
}
