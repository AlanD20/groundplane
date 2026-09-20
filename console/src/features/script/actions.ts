import type { RefObject } from "react";
import type { Script, ScriptInput, ScriptPatch } from "@/features/script/types";
import type { operations } from "@/lib/api.generated";
import {
  scriptFromAPI,
  scriptCreateToAPI,
  scriptPatchToAPI,
  type ScriptCreateResponse,
  type ScriptEditResponse,
} from "@/features/script/request-model";
import { controllerRequest } from "@/lib/controller-json-request";
import { listAllServices } from "@/features/service/api";
import { requireTaskId } from "@/features/task/journal-model";
import {
  findEnvironment,
  type EnvironmentRemovalDraft,
  type PendingResourceRemoval,
} from "@/features/environment/environment-removal-model";
type ScriptRunResponse =
  operations["script.run"]["responses"][202]["content"]["application/json"];
export type ScriptActions = {
  addScript: (envId: string, script: ScriptInput) => Promise<Script>;
  updateScript: (
    envId: string,
    scriptId: string,
    patch: ScriptPatch,
  ) => Promise<Script>;
  runScript: (scriptId: string) => Promise<string>;
  removeScript: (envId: string, scriptId: string) => Promise<string>;
};
export function createScriptActions(
  update: (change: (draft: EnvironmentRemovalDraft) => void) => void,
  assertEnvironmentMutable: (environmentId: string, operation: string) => void,
  nextEnvironmentGeneration: (environmentId: string, intent: "child") => number,
  environmentGenerations: RefObject<Map<string, number>>,
  dispatchResourceRemoval: (removal: PendingResourceRemoval) => Promise<string>,
): ScriptActions {
  return {
    addScript: async (envId, script) => {
      assertEnvironmentMutable(envId, "Script mutation");
      const generation = nextEnvironmentGeneration(envId, "child");
      const targetService = (await listAllServices(envId)).find(
        (service) => service.name === script.service,
      );
      if (!targetService)
        throw new Error(
          `Service ${script.service} was not found in this Environment`,
        );
      const body = scriptCreateToAPI(envId, targetService.id, script);
      const created = scriptFromAPI(
        await controllerRequest<ScriptCreateResponse>("/scripts", 201, {
          method: "POST",
          body,
        }),
      );
      update((d) => {
        if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
          return;
        findEnvironment(d, envId)?.scripts.push(created);
      });
      return created;
    },
    updateScript: async (envId, scriptId, patch) => {
      assertEnvironmentMutable(envId, "Script mutation");
      const generation = nextEnvironmentGeneration(envId, "child");
      const body = scriptPatchToAPI(patch);
      const edited = scriptFromAPI(
        await controllerRequest<ScriptEditResponse>(
          `/scripts/${encodeURIComponent(scriptId)}`,
          200,
          { method: "PATCH", body },
        ),
      );
      update((d) => {
        if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
          return;
        const script = findEnvironment(d, envId)?.scripts.find(
          (candidate) => candidate.id === scriptId,
        );
        if (script) Object.assign(script, edited);
      });
      return edited;
    },
    runScript: async (scriptId) => {
      const accepted = await controllerRequest<ScriptRunResponse>(
        `/scripts/${encodeURIComponent(scriptId)}/run`,
        202,
        {
          method: "POST",
          body: undefined,
        },
      );
      return requireTaskId(accepted, "Script run");
    },
    removeScript: (envId, scriptId) => (
      assertEnvironmentMutable(envId, "Script mutation"),
      dispatchResourceRemoval({
        kind: "script",
        environmentId: envId,
        resourceId: scriptId,
      })
    ),
  };
}
