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
    after?: string[],
  ) => Promise<string>;
  reconcileEnvironmentComponent: (
    componentId: string,
    after?: string[],
  ) => Promise<string>;
  updateComponentConfig: (
    componentId: string,
    config: ComponentConfigInput,
    after?: string[],
  ) => Promise<string | null>;
};
export function createComponentActions(): ComponentActions {
  return {
    setComponentEnabled: async (componentId, enabled, config, after = []) => {
      const action = enabled ? "enable" : "disable";
      const accepted = await controllerRequest<ComponentTaskAccepted>(
        `/components/${encodeURIComponent(componentId)}/${action}`,
        202,
        {
          method: "POST",
          body: enabled && config ? { config, after } : { after },
        },
      );
      if (!accepted.task_id)
        throw new Error(
          `Controller response is missing Component ${action} task_id`,
        );
      return accepted.task_id;
    },
    reconcileEnvironmentComponent: async (componentId, after = []) => {
      const accepted = await controllerRequest<ComponentTaskAccepted>(
        `/components/${encodeURIComponent(componentId)}/update`,
        202,
        { method: "POST", body: { after } },
      );
      if (!accepted.task_id)
        throw new Error(
          "Controller response is missing Component update task_id",
        );
      return accepted.task_id;
    },
    updateComponentConfig: async (componentId, config, after = []) => {
      const result = await controllerRequest<
        ComponentConfigMutationResponse | ComponentTaskAccepted
      >(`/components/${encodeURIComponent(componentId)}/config`, [200, 202], {
        method: "PUT",
        body: { config, after },
      });
      if ("task_id" in result) {
        if (!result.task_id)
          throw new Error(
            "Controller response is missing Component config task_id",
          );
        return result.task_id;
      }
      return null;
    },
  };
}
