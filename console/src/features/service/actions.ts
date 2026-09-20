import type { Service, ServiceRuntimeIntent } from "@/lib/types";
import { controllerRequest } from "@/lib/controller-json-request";
import { requireTaskId } from "@/features/task/journal-model";
import {
  findEnvironment,
  type EnvironmentRemovalDraft,
  type PendingResourceRemoval,
} from "@/features/environment/environment-removal-model";
import {
  serviceFromAPI,
  serviceMutationBody,
  type ServiceMutationInput,
  type ServiceCreateRequest,
  type ServiceCreateResponse,
  type ServiceEditRequest,
  type ServiceEditResponse,
  type ServiceShowResponse,
  type ServiceRuntimeTaskAccepted,
} from "./api";
export type ServiceActions = {
  addService: (envId: string, input: ServiceMutationInput) => Promise<Service>;
  updateService: (
    envId: string,
    serviceId: string,
    input: ServiceMutationInput,
  ) => Promise<Service>;
  getService: (serviceId: string) => Promise<Service>;
  deleteService: (envId: string, serviceId: string) => Promise<string>;
  runServiceRuntimeAction: (
    envId: string,
    serviceId: string,
    action: "start" | "stop" | "destroy",
  ) => Promise<string>;
};
export function createServiceActions(
  update: (change: (draft: EnvironmentRemovalDraft) => void) => void,
  assertEnvironmentMutable: (environmentId: string, operation: string) => void,
  dispatchResourceRemoval: (removal: PendingResourceRemoval) => Promise<string>,
): ServiceActions {
  return {
    addService: async (envId, input) => {
      assertEnvironmentMutable(envId, "Service mutation");
      const body: ServiceCreateRequest = {
        environment_id: envId,
        name: input.name,
        ...serviceMutationBody(input),
      };
      const created = serviceFromAPI(
        await controllerRequest<ServiceCreateResponse>("/services", 201, {
          method: "POST",
          body,
        }),
      );
      update((draft) => {
        findEnvironment(draft, envId)?.services.push(created);
      });
      return created;
    },
    updateService: async (envId, serviceId, input) => {
      assertEnvironmentMutable(envId, "Service mutation");
      const body: ServiceEditRequest = serviceMutationBody(input);
      const edited = serviceFromAPI(
        await controllerRequest<ServiceEditResponse>(
          `/services/${encodeURIComponent(serviceId)}`,
          200,
          { method: "PATCH", body },
        ),
      );
      update((d) => {
        const e = findEnvironment(d, envId);
        if (!e) return;
        const i = e.services.findIndex((s) => s.id === serviceId);
        if (i >= 0) e.services[i] = edited;
      });
      return edited;
    },
    getService: async (serviceId) =>
      serviceFromAPI(
        await controllerRequest<ServiceShowResponse>(
          `/services/${encodeURIComponent(serviceId)}`,
          200,
          { method: "GET" },
        ),
      ),
    deleteService: (envId, serviceId) => (
      assertEnvironmentMutable(envId, "Service mutation"),
      dispatchResourceRemoval({
        kind: "service",
        environmentId: envId,
        resourceId: serviceId,
      })
    ),
    runServiceRuntimeAction: async (envId, serviceId, action) => {
      assertEnvironmentMutable(envId, "Service mutation");
      const accepted = await controllerRequest<ServiceRuntimeTaskAccepted>(
        `/services/${encodeURIComponent(serviceId)}/${action}`,
        202,
        { method: "POST" },
      );
      const taskId = requireTaskId(accepted, `Service ${action}`);
      const intent: ServiceRuntimeIntent =
        action === "start"
          ? "running"
          : action === "stop"
            ? "stopped"
            : "absent";
      update((d) => {
        const service = findEnvironment(d, envId)?.services.find(
          (candidate) => candidate.id === serviceId,
        );
        if (!service) return;
        service.runtimeIntent = intent;
      });
      return taskId;
    },
  };
}
