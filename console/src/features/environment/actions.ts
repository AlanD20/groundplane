import type { operations } from "@/lib/api.generated";
import type { Environment } from "@/lib/types";
import type { EnvironmentLifecycle } from "@/features/environment/lifecycle-types";
import {
  findEnvironment,
  type EnvironmentRemovalDraft,
} from "./environment-removal-model";
import { controllerRequest } from "@/lib/controller-json-request";
import { environmentMutationKey } from "@/features/environment/operation-storage";
import { environmentFromAPI } from "@/features/environment/projection";
import { applyAuthoritativeEnvironmentScalars } from "@/features/environment/authoritative-state";
import { newULID } from "@/lib/utils";
import type { useEnvironmentMutationIntents } from "./use-mutation-intents";
type EnvironmentCreateRequest =
  operations["environment.create"]["requestBody"]["content"]["application/json"];
type EnvironmentTaskAccepted =
  operations["environment.create"]["responses"][202]["content"]["application/json"];
type EnvironmentEditRequest =
  operations["environment.edit"]["requestBody"]["content"]["application/json"];
type EnvironmentEditResponse =
  operations["environment.edit"]["responses"][200]["content"]["application/json"];
type EnvironmentRenameRequest =
  operations["environment.rename"]["requestBody"]["content"]["application/json"];
type EnvironmentRenameResponse =
  operations["environment.rename"]["responses"][200]["content"]["application/json"];

type Workspace = EnvironmentRemovalDraft & {
  projectError: string | null;
  environmentDeletionRevision: number;
};
export type EnvironmentActions = {
  addEnvironment: (
    projectId: string,
    name: string,
    networkPool: string,
  ) => Promise<EnvironmentTaskAccepted>;
  editEnvironment: (envId: string, networkPool: string) => Promise<Environment>;
  renameEnvironment: (envId: string, name: string) => Promise<Environment>;
  deleteEnvironment: (envId: string) => Promise<string>;
};
type Options = {
  state: Workspace;
  update: (change: (draft: Workspace) => void) => void;
  lifecycle: EnvironmentLifecycle<Workspace>;
  assertEnvironmentMutable: (environmentId: string, operation: string) => void;
  mutations: ReturnType<typeof useEnvironmentMutationIntents>;
};
export function createEnvironmentActions({
  state,
  update,
  lifecycle,
  assertEnvironmentMutable,
  mutations,
}: Options): EnvironmentActions {
  const {
    nextEnvironmentGeneration,
    settleEnvironmentMutation,
    environmentGenerations,
    isEnvironmentDeletionPending,
    dispatchResourceRemoval,
    waitForResourceRemoval,
  } = lifecycle;
  const {
    environmentMutationIntents,
    persistEnvironmentMutationIntent,
    clearEnvironmentMutationIntent,
  } = mutations;
  return {
    addEnvironment: async (projectId, name, networkPool) => {
      const body: EnvironmentCreateRequest = {
        project_id: projectId,
        name,
        network_pool: networkPool,
      };
      const key = environmentMutationKey("create", [
        projectId,
        name,
        networkPool,
      ]);
      const intent = environmentMutationIntents.current.get(key) ?? {
        key,
        kind: "create" as const,
        idempotencyKey: `groundplane:${newULID()}`,
        projectId,
        name,
        networkPool,
      };
      persistEnvironmentMutationIntent(intent);
      try {
        const accepted = await controllerRequest<EnvironmentTaskAccepted>(
          "/environments",
          202,
          { method: "POST", body, idempotencyKey: intent.idempotencyKey },
        );
        if (!accepted.task_id)
          throw new Error("Controller response is missing create task_id");
        clearEnvironmentMutationIntent(key);
        return accepted;
      } catch (error) {
        const message =
          error instanceof Error
            ? error.message
            : "Unable to create Environment";
        update((draft) => {
          draft.projectError = message;
        });
        throw error;
      }
    },
    editEnvironment: async (envId, networkPool) => {
      assertEnvironmentMutable(envId, "Environment edit");
      const body: EnvironmentEditRequest = { network_pool: networkPool };
      const project = [...state.tenantProjects, ...state.backingProjects].find(
        (candidate) =>
          candidate.environments?.some(
            (environment) => environment.id === envId,
          ),
      );
      if (!project) throw new Error(`Environment ${envId} is not loaded`);
      const key = environmentMutationKey("edit", [envId, networkPool]);
      const intent = environmentMutationIntents.current.get(key) ?? {
        key,
        kind: "edit" as const,
        idempotencyKey: `groundplane:${newULID()}`,
        projectId: project.id,
        environmentId: envId,
        networkPool,
      };
      persistEnvironmentMutationIntent(intent);
      const generation = nextEnvironmentGeneration(envId, "edit");
      try {
        const edited = environmentFromAPI(
          await controllerRequest<EnvironmentEditResponse>(
            `/environments/${encodeURIComponent(envId)}`,
            200,
            { method: "PATCH", body, idempotencyKey: intent.idempotencyKey },
          ),
        );
        if (edited.id !== envId || edited.projectId !== project.id)
          throw new Error(
            `Controller returned Environment ${edited.id} for a different resource`,
          );
        if ((environmentGenerations.current.get(envId) ?? 0) !== generation) {
          clearEnvironmentMutationIntent(key);
          throw new Error(`Environment edit ${envId} was superseded`);
        }
        if (!findEnvironment(state, envId)) {
          clearEnvironmentMutationIntent(key);
          throw new Error(`Environment edit ${envId} was not applied`);
        }
        update((draft) => {
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
            return;
          const environment = findEnvironment(draft, envId);
          if (!environment) return;
          applyAuthoritativeEnvironmentScalars(environment, edited);
        });
        settleEnvironmentMutation(envId, generation, "succeeded");
        clearEnvironmentMutationIntent(key);
        return edited;
      } catch (error) {
        settleEnvironmentMutation(envId, generation, "failed");
        throw error;
      }
    },
    renameEnvironment: async (envId, name) => {
      assertEnvironmentMutable(envId, "Environment rename");
      const body: EnvironmentRenameRequest = { name };
      const project = [...state.tenantProjects, ...state.backingProjects].find(
        (candidate) =>
          candidate.environments?.some(
            (environment) => environment.id === envId,
          ),
      );
      if (!project) throw new Error(`Environment ${envId} is not loaded`);
      const key = environmentMutationKey("rename", [envId, name]);
      const intent = environmentMutationIntents.current.get(key) ?? {
        key,
        kind: "rename" as const,
        idempotencyKey: `groundplane:${newULID()}`,
        projectId: project.id,
        environmentId: envId,
        name,
      };
      persistEnvironmentMutationIntent(intent);
      const generation = nextEnvironmentGeneration(envId, "rename");
      try {
        const renamed = environmentFromAPI(
          await controllerRequest<EnvironmentRenameResponse>(
            `/environments/${encodeURIComponent(envId)}/rename`,
            200,
            { method: "POST", body, idempotencyKey: intent.idempotencyKey },
          ),
        );
        if (renamed.id !== envId || renamed.projectId !== project.id)
          throw new Error(
            `Controller returned Environment ${renamed.id} for a different resource`,
          );
        if ((environmentGenerations.current.get(envId) ?? 0) !== generation) {
          clearEnvironmentMutationIntent(key);
          throw new Error(`Environment rename ${envId} was superseded`);
        }
        if (!findEnvironment(state, envId)) {
          clearEnvironmentMutationIntent(key);
          throw new Error(`Environment rename ${envId} was not applied`);
        }
        update((draft) => {
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
            return;
          const environment = findEnvironment(draft, envId);
          if (!environment) return;
          applyAuthoritativeEnvironmentScalars(environment, renamed);
        });
        settleEnvironmentMutation(envId, generation, "succeeded");
        clearEnvironmentMutationIntent(key);
        return renamed;
      } catch (error) {
        settleEnvironmentMutation(envId, generation, "failed");
        throw error;
      }
    },
    deleteEnvironment: async (envId) => {
      if (!isEnvironmentDeletionPending(envId))
        assertEnvironmentMutable(envId, "Environment deletion");
      const project = [...state.tenantProjects, ...state.backingProjects].find(
        (candidate) =>
          candidate.environments?.some(
            (environment) => environment.id === envId,
          ),
      );
      if (!project)
        return Promise.reject(new Error(`Environment ${envId} is not loaded`));
      const generation = nextEnvironmentGeneration(envId, "delete");
      const taskId = await dispatchResourceRemoval({
        kind: "environment",
        projectId: project.id,
        resourceId: envId,
        generation,
      });
      const task = await waitForResourceRemoval(taskId);
      if (task.task.status !== "succeeded") {
        throw new Error(
          `Environment deletion Task ${taskId} ${task.task.status}`,
        );
      }
      return taskId;
    },
  };
}
