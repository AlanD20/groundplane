import { controllerRequest } from "@/lib/controller-json-request";
import type { operations } from "@/lib/api.generated";
import { requireTaskId } from "@/features/task/journal-model";
import type {
  BackingServiceCreateRequest,
  BackingServiceCreateAccepted,
} from "./api";
type BackingRuntimeTaskAccepted =
  operations["backing-service.start"]["responses"][202]["content"]["application/json"];

export type BackingServiceActions = {
  deleteBackingService: (id: string, after?: string[]) => Promise<string>;
  runBackingRuntimeAction: (
    id: string,
    action: "start" | "stop" | "destroy",
    after?: string[],
  ) => Promise<string>;
  addBackingProject: (
    input: BackingServiceCreateRequest,
  ) => Promise<BackingServiceCreateAccepted>;
};
export function createBackingServiceActions(): BackingServiceActions {
  return {
    deleteBackingService: async (id, after = []) => {
      const accepted = await controllerRequest<
        operations["backing-service.delete"]["responses"][202]["content"]["application/json"]
      >(`/backing-services/${encodeURIComponent(id)}`, 202, {
        method: "DELETE",
        body: { after },
      });
      return requireTaskId(accepted, "Backing Service deletion");
    },
    runBackingRuntimeAction: async (id, action, after = []) => {
      const accepted = await controllerRequest<BackingRuntimeTaskAccepted>(
        `/backing-services/${encodeURIComponent(id)}/${action}`,
        202,
        { method: "POST", body: { after } },
      );
      return requireTaskId(accepted, `Backing service ${action}`);
    },
    addBackingProject: async (input) => {
      const created = await controllerRequest<BackingServiceCreateAccepted>(
        "/backing-services",
        202,
        { method: "POST", body: input },
      );
      requireTaskId(created, "Backing service creation");
      return created;
    },
  };
}
