import { useCallback } from "react";
import type { operations } from "@/lib/api.generated";
import type {
  PlatformInfra,
  ManagedConfigFile,
  EnvironmentComponent,
} from "@/lib/types";
import type { EnvironmentRemovalDraft } from "@/features/environment/environment-removal-model";
import { controllerRequest } from "@/lib/controller-json-request";
import { hydratePlatformComponents } from "./platform-projection";
import { listAllComponents, listPlatformComponents } from "./api";
type ComponentConfigResponse =
  operations["component-config.show"]["responses"][200]["content"]["application/json"];
export type ComponentState = {
  platformComponentsLoading: boolean;
  platformComponentError: string | null;
  managedConfigFiles: ManagedConfigFile[];
  managedConfigLoading: boolean;
  managedConfigError: string | null;
};
export type ComponentRefreshActions = {
  refreshPlatformComponents: (
    signal?: AbortSignal,
  ) => Promise<PlatformInfra["components"]>;
  refreshComponentConfig: (
    componentId: string,
    signal?: AbortSignal,
  ) => Promise<ManagedConfigFile[]>;
  refreshEnvironmentComponents: (
    environmentId: string,
    signal?: AbortSignal,
  ) => Promise<EnvironmentComponent[]>;
};
export function useComponentRefresh(
  update: (
    change: (
      draft: ComponentState &
        EnvironmentRemovalDraft & { platform: PlatformInfra },
    ) => void,
  ) => void,
): ComponentRefreshActions {
  const refreshPlatformComponents = useCallback(
    async (signal?: AbortSignal) => {
      const hydrated = hydratePlatformComponents(
        await listPlatformComponents(signal),
      );
      update((draft) => {
        draft.platform.components = hydrated.components;
        if (hydrated.dns) draft.platform.dns = hydrated.dns;
        draft.platformComponentsLoading = false;
        draft.platformComponentError = null;
      });
      return hydrated.components;
    },
    [update],
  );

  const refreshComponentConfig = useCallback(
    async (componentId: string, signal?: AbortSignal) => {
      update((draft) => {
        draft.managedConfigLoading = true;
        draft.managedConfigError = null;
      });
      try {
        const response = await controllerRequest<ComponentConfigResponse>(
          `/components/${encodeURIComponent(componentId)}/config`,
          200,
          { signal },
        );
        const files = response.managed_files.map((file) => ({
          path: file.path,
          template: file.template,
          rendered: file.rendered,
        }));
        update((draft) => {
          draft.managedConfigFiles = files;
          draft.managedConfigLoading = false;
          draft.managedConfigError = null;
        });
        return files;
      } catch (error) {
        update((draft) => {
          draft.managedConfigLoading = false;
          draft.managedConfigError =
            error instanceof Error
              ? error.message
              : "Unable to load managed Component config";
        });
        throw error;
      }
    },
    [update],
  );

  const refreshEnvironmentComponents = useCallback(
    async (environmentId: string, signal?: AbortSignal) => {
      const components = await listAllComponents(environmentId, signal);
      update((draft) => {
        for (const project of [
          ...draft.tenantProjects,
          ...draft.backingProjects,
        ]) {
          const environment = project.environments?.find(
            (candidate) => candidate.id === environmentId,
          );
          if (!environment) continue;
          environment.components = components;
          return;
        }
      });
      return components;
    },
    [update],
  );

  return {
    refreshPlatformComponents,
    refreshComponentConfig,
    refreshEnvironmentComponents,
  };
}
