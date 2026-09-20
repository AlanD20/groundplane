import { useCallback } from "react";
import type { Service } from "@/lib/types";
import type { operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import {
  findEnvironment,
  type EnvironmentRemovalDraft,
} from "@/features/environment/environment-removal-model";
import { listAllReleaseGroups } from "@/features/release-group/api";
import { refreshReleaseGroupTags } from "@/features/release-group/projection";
import { listAllReleases, projectReleaseSummary } from "./api";
export type ReleaseActions = {
  commitDeploy: (
    envId: string,
    service: string,
    tag: string,
    strategy: Service["strategy"],
  ) => Promise<string>;
  commitRollback: (
    envId: string,
    service: string,
    tag: string,
  ) => Promise<string>;
};
export function createReleaseActions(
  state: EnvironmentRemovalDraft,
  assertEnvironmentMutable: (environmentId: string, operation: string) => void,
): ReleaseActions {
  return {
    commitDeploy: async (envId, service, tag, strategy) => {
      assertEnvironmentMutable(envId, "deployment");
      const target = findEnvironment(state, envId)?.services.find(
        (candidate) => candidate.name === service,
      );
      if (!target) throw new Error(`Service ${service} no longer exists`);
      const body: operations["service.deploy"]["requestBody"]["content"]["application/json"] =
        {
          tag,
          strategy,
          on_failure: "switch_back",
        };
      const accepted = await controllerRequest<
        operations["service.deploy"]["responses"][202]["content"]["application/json"]
      >(`/services/${encodeURIComponent(target.id)}/deploy`, 202, {
        method: "POST",
        body,
      });
      if (!accepted.task_id)
        throw new Error("Controller response is missing deploy task_id");
      return accepted.task_id;
    },
    commitRollback: async (envId, service, tag) => {
      assertEnvironmentMutable(envId, "rollback");
      const target = findEnvironment(state, envId)?.services.find(
        (candidate) => candidate.name === service,
      );
      if (!target) throw new Error(`Service ${service} no longer exists`);
      const body: operations["service.rollback"]["requestBody"]["content"]["application/json"] =
        { tag };
      const accepted = await controllerRequest<
        operations["service.rollback"]["responses"][202]["content"]["application/json"]
      >(`/services/${encodeURIComponent(target.id)}/rollback`, 202, {
        method: "POST",
        body,
      });
      if (!accepted.task_id)
        throw new Error("Controller response is missing rollback task_id");
      return accepted.task_id;
    },
  };
}
export function useReleaseRefresh(
  state: EnvironmentRemovalDraft,
  update: (
    change: (
      draft: EnvironmentRemovalDraft & { projectError: string | null },
    ) => void,
  ) => void,
) {
  const refreshEnvironmentReleases = useCallback(
    async (environmentId: string, signal?: AbortSignal) => {
      const environment = findEnvironment(state, environmentId);
      if (!environment)
        throw new Error(`Environment ${environmentId} is not loaded`);
      try {
        const [deploys, releaseGroups] = await Promise.all([
          listAllReleases(environmentId, environment.services, signal),
          listAllReleaseGroups(environmentId, environment.services, signal),
        ]);
        update((draft) => {
          const current = findEnvironment(draft, environmentId);
          if (!current) return;
          Object.assign(current, projectReleaseSummary(deploys));
          current.deploys = deploys;
          current.releaseGroups = releaseGroups;
          refreshReleaseGroupTags(current);
        });
      } catch (error) {
        update((draft) => {
          draft.projectError =
            error instanceof Error
              ? error.message
              : "Unable to refresh release state";
        });
        throw error;
      }
    },
    [state, update],
  );

  return refreshEnvironmentReleases;
}
