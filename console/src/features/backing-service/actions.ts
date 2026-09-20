import { controllerRequest } from "@/lib/controller-json-request";
import type { Project, Tenant, ServiceRuntimeIntent } from "@/lib/types";
import type { operations } from "@/lib/api.generated";
import { requireTaskId } from "@/features/task/journal-model";
import type {
  BackingServiceCreateRequest,
  BackingServiceCreatedResponse,
} from "./api";
import { listAllBackingProjects } from "./workspace-read";
type BackingRuntimeTaskAccepted =
  operations["backing-service.start"]["responses"][202]["content"]["application/json"];

type BackingWorkspace = {
  tenantProjects: Project[];
  tenants: Tenant[];
  backingProjects: Project[];
  backingProjectError: string | null;
};
export type BackingServiceActions = {
  runBackingRuntimeAction: (
    id: string,
    action: "start" | "stop" | "destroy",
  ) => Promise<string>;
  addBackingProject: (
    input: BackingServiceCreateRequest,
  ) => Promise<BackingServiceCreatedResponse>;
};
export function createBackingServiceActions(
  state: BackingWorkspace,
  update: (change: (draft: BackingWorkspace) => void) => void,
): BackingServiceActions {
  return {
    runBackingRuntimeAction: async (id, action) => {
      const accepted = await controllerRequest<BackingRuntimeTaskAccepted>(
        `/backing-services/${encodeURIComponent(id)}/${action}`,
        202,
        { method: "POST" },
      );
      const intent: ServiceRuntimeIntent =
        action === "start"
          ? "running"
          : action === "stop"
            ? "stopped"
            : "absent";
      update((d) => {
        const project = d.backingProjects.find(
          (candidate) => candidate.id === id,
        );
        project?.environments?.forEach((environment) => {
          environment.services.forEach((service) => {
            service.runtimeIntent = intent;
          });
        });
      });
      return requireTaskId(accepted, `Backing service ${action}`);
    },
    addBackingProject: async (input) => {
      const created = await controllerRequest<BackingServiceCreatedResponse>(
        "/backing-services",
        201,
        { method: "POST", body: input },
      );
      requireTaskId(created, "Backing service creation");
      const backingProjects = await listAllBackingProjects(
        state.tenantProjects,
        state.tenants,
        new AbortController().signal,
      );
      update((draft) => {
        draft.backingProjects = backingProjects;
        draft.backingProjectError = null;
      });
      return created;
    },
  };
}
