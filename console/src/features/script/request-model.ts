import type { operations } from "@/lib/api.generated";
import type { Script, ScriptInput, ScriptPatch } from "./types";
import {
  scriptExecutionFromAPI,
  scriptExecutionToAPI,
} from "./execution-wire.ts";

export type ScriptCreateRequest =
  operations["script.create"]["requestBody"]["content"]["application/json"];
export type ScriptCreateResponse =
  operations["script.create"]["responses"][201]["content"]["application/json"];
export type ScriptEditRequest =
  operations["script.edit"]["requestBody"]["content"]["application/json"];
export type ScriptEditResponse =
  operations["script.edit"]["responses"][200]["content"]["application/json"];

export function scriptCreateToAPI(
  environmentId: string,
  serviceId: string,
  input: ScriptInput,
): ScriptCreateRequest {
  return {
    environment_id: environmentId,
    service_id: serviceId,
    slug: input.slug,
    script: input.body,
    when: input.when,
    order: input.order,
    execution: scriptExecutionToAPI(input.execution),
  };
}

export function scriptPatchToAPI(patch: ScriptPatch): ScriptEditRequest {
  const body: ScriptEditRequest = {};
  if (patch.slug !== undefined) body.slug = patch.slug;
  if (patch.body !== undefined) body.script = patch.body;
  if (patch.when !== undefined) body.when = patch.when;
  if (patch.order !== undefined) body.order = patch.order;
  if (patch.execution !== undefined)
    body.execution = scriptExecutionToAPI(patch.execution);
  return body;
}

const scriptHooks: Script["when"][] = [
  "manual",
  "pre-deploy",
  "post-deploy",
  "pre-rollback",
  "post-rollback",
  "on-failure",
];

export function scriptFromAPI(
  script: ScriptCreateResponse | ScriptEditResponse,
): Script {
  if (!scriptHooks.includes(script.when as Script["when"])) {
    throw new Error(`Controller returned unknown Script hook ${script.when}`);
  }
  if (
    !Number.isInteger(script.order) ||
    script.order < 0 ||
    script.order > 65535
  ) {
    throw new Error("Controller returned an invalid Script order");
  }
  return {
    id: script.id,
    environmentId: script.environment_id,
    slug: script.slug,
    serviceId: script.service_id,
    service: script.service,
    body: script.script,
    order: script.order,
    execution: scriptExecutionFromAPI(script.execution),
    when: script.when as Script["when"],
    origin: script.origin,
    reconciliationKey: script.reconciliation_key,
    activeGeneration: script.active_generation,
  };
}
