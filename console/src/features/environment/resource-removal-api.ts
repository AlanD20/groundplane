import type { operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";

export type RemovableEnvironmentResource =
  | "environments"
  | "services"
  | "routes"
  | "entries"
  | "scripts";

type ResourceRemovalResponse =
  operations["environment.delete"]["responses"][202]["content"]["application/json"];

export async function requestResourceRemoval(
  resource: RemovableEnvironmentResource,
  resourceId: string,
  idempotencyKey: string,
): Promise<string> {
  const accepted = await controllerRequest<ResourceRemovalResponse>(
    `/${resource}/${encodeURIComponent(resourceId)}`,
    202,
    { method: "DELETE", idempotencyKey },
  );
  if (!accepted.task_id)
    throw new Error("Controller response is missing task_id");
  return accepted.task_id;
}
