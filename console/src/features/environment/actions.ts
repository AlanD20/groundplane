import type { RefObject } from "react";
import type { operations } from "@/lib/api.generated";
import type { Environment } from "@/lib/types";
import type { EnvironmentLifecycle } from "@/features/environment/lifecycle-types";
import {
  findEnvironment,
  type EnvironmentRemovalDraft,
  type TaskResponse,
} from "./environment-removal-model";
import { controllerRequest } from "@/lib/controller-json-request";
import { environmentMutationKey } from "@/features/environment/operation-storage";
import { environmentFromAPI } from "@/features/environment/projection";
import { applyAuthoritativeEnvironmentScalars } from "@/features/environment/authoritative-state";
import { observeEnvironmentTask } from "@/features/environment/task-observation";
import { newULID } from "@/lib/utils";
import { listAllEnvironments } from "./workspace-read";
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
  providerActive: RefObject<boolean>;
  requestEnvironmentTask: (
    taskId: string,
    signal?: AbortSignal,
  ) => Promise<TaskResponse>;
  assertEnvironmentMutable: (environmentId: string, operation: string) => void;
  mutations: ReturnType<typeof useEnvironmentMutationIntents>;
};
export function createEnvironmentActions({
  state,
  update,
  lifecycle,
  providerActive,
  requestEnvironmentTask,
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
    environmentTaskControllers,
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
      let intent = environmentMutationIntents.current.get(key) ?? {
        key,
        kind: "create" as const,
        idempotencyKey: `groundplane:${newULID()}`,
        projectId,
        name,
        networkPool,
      };
      persistEnvironmentMutationIntent(intent);
      let observedTaskStatus: TaskResponse["status"] | undefined;
      try {
        const accepted = intent.taskId
          ? { task_id: intent.taskId }
          : await controllerRequest<EnvironmentTaskAccepted>(
              "/environments",
              202,
              {
                method: "POST",
                body,
                idempotencyKey: intent.idempotencyKey,
              },
            );
        if (!accepted.task_id)
          throw new Error("Controller response is missing create task_id");
        if (!intent.taskId) {
          intent = { ...intent, taskId: accepted.task_id };
          persistEnvironmentMutationIntent(intent);
        }
        const observationController = new AbortController();
        environmentTaskControllers.current.add(observationController);
        let task: TaskResponse;
        try {
          task = await observeEnvironmentTask(
            requestEnvironmentTask,
            accepted.task_id,
            providerActive,
            observationController.signal,
          );
        } finally {
          environmentTaskControllers.current.delete(observationController);
        }
        observedTaskStatus = task.status;
        if (
          task.id !== accepted.task_id ||
          task.type !== "create" ||
          !task.target
        ) {
          clearEnvironmentMutationIntent(key);
          throw new Error(
            `Controller create Task ${accepted.task_id} did not identify an Environment target`,
          );
        }
        if (task.project_id !== projectId) {
          clearEnvironmentMutationIntent(key);
          throw new Error(
            `Controller create Task ${accepted.task_id} belongs to a different Project`,
          );
        }
        if (task.status !== "completed") {
          clearEnvironmentMutationIntent(key);
          throw new Error(
            `Controller create Task ${accepted.task_id} ${task.status}`,
          );
        }
        const environments = await listAllEnvironments(projectId);
        const created = environments.find(
          (environment) =>
            environment.id === task.target &&
            environment.projectId === projectId,
        );
        if (!created) {
          clearEnvironmentMutationIntent(key);
          throw new Error(
            `Controller did not publish Environment for task ${accepted.task_id}`,
          );
        }
        const generation = nextEnvironmentGeneration(created.id, "create");
        update((draft) => {
          if (
            (environmentGenerations.current.get(created.id) ?? 0) !== generation
          )
            return;
          const project = draft.tenantProjects.find(
            (candidate) => candidate.id === projectId,
          );
          if (!project) return;
          project.environments = project.environments ?? [];
          const index = project.environments.findIndex(
            (environment) => environment.id === created.id,
          );
          if (index >= 0) project.environments[index] = created;
          else project.environments.push(created);
        });
        if (created.provisioningState === "failed") {
          settleEnvironmentMutation(created.id, generation, "failed");
          clearEnvironmentMutationIntent(key);
          throw new Error(
            `Controller Environment ${created.id} provisioning failed`,
          );
        }
        settleEnvironmentMutation(created.id, generation, "succeeded");
        clearEnvironmentMutationIntent(key);
        return accepted;
      } catch (error) {
        if (observedTaskStatus && observedTaskStatus !== "completed")
          clearEnvironmentMutationIntent(key);
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
      if (task.status !== "completed") {
        throw new Error(`Environment deletion Task ${taskId} ${task.status}`);
      }
      return taskId;
    },
  };
}
