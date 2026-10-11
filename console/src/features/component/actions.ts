import type { operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
type ComponentTaskAccepted =
  operations["component.enable"]["responses"][202]["content"]["application/json"];
type ComponentConfigMutationResponse =
  operations["component-config.set"]["responses"][200]["content"]["application/json"];
type ComponentConfigInput =
  operations["component-config.set"]["requestBody"]["content"]["application/json"]["config"];

export type ComponentActions = {
  setComponentEnabled: (
    componentId: string,
    enabled: boolean,
    config?: ComponentConfigInput,
  ) => Promise<string>;
  reconcileEnvironmentComponent: (componentId: string) => Promise<string>;
  updateComponentConfig: (
    componentId: string,
    config: ComponentConfigInput,
  ) => Promise<string | null>;
};
export function createComponentActions(): ComponentActions {
  return {
    setComponentEnabled: async (componentId, enabled, config) => {
      const action = enabled ? "enable" : "disable";
      const accepted = await controllerRequest<ComponentTaskAccepted>(
        `/components/${encodeURIComponent(componentId)}/${action}`,
        202,
        {
          method: "POST",
          ...(enabled && config ? { body: { config } } : {}),
        },
      );
      if (!accepted.task_id)
        throw new Error(
          `Controller response is missing Component ${action} task_id`,
        );
      return accepted.task_id;
    },
    reconcileEnvironmentComponent: async (componentId) => {
      const accepted = await controllerRequest<ComponentTaskAccepted>(
        `/components/${encodeURIComponent(componentId)}/update`,
        202,
        { method: "POST" },
      );
      if (!accepted.task_id)
        throw new Error(
          "Controller response is missing Component update task_id",
        );
      return accepted.task_id;
    },
    updateComponentConfig: async (componentId, config) => {
      const result = await controllerRequest<ComponentConfigMutationResponse>(
        `/components/${encodeURIComponent(componentId)}/config`,
        200,
        { method: "PUT", body: { config } },
      );
      if (!result.reconcile_task_id)
        throw new Error(
          "Controller response is missing Component config reconcile_task_id",
        );
      return result.reconcile_task_id;
    },
  };
}
