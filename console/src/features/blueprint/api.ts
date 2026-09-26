import type { operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import {
  ControllerRequestError,
  ControllerTransportError,
  controllerResponseError,
} from "@/lib/controller-request-errors";
import {
  createBlueprintMultipartBody,
  type BlueprintApplyRequest,
} from "@/lib/blueprint-bundle";
import { newULID } from "@/lib/utils";
import { BlueprintApplyIntent } from "./apply-intent";

export type BlueprintDocumentResponse =
  operations["blueprint.show"]["responses"][200]["content"]["application/json"];
export type BlueprintValidationResponse =
  operations["blueprint.validate"]["responses"][200]["content"]["application/json"];
type BlueprintTaskAccepted =
  operations["blueprint.apply"]["responses"][202]["content"]["application/json"];

export type BlueprintActions = {
  getBlueprint: (envId: string) => Promise<BlueprintDocumentResponse>;
  validateBlueprint: (
    envId: string,
    request: BlueprintApplyRequest,
    expectedRevision: string,
  ) => Promise<BlueprintValidationResponse>;
  applyBlueprint: (
    envId: string,
    request: BlueprintApplyRequest,
    expectedRevision?: string,
    key?: string,
  ) => Promise<BlueprintTaskAccepted>;
};

let applyIntent: BlueprintApplyIntent | null = null;
function intent() {
  applyIntent ??= new BlueprintApplyIntent(sessionStorage);
  return applyIntent;
}

export function pendingBlueprintApply() { return intent().current; }
export function settleBlueprintApply(taskId: string) { intent().settle(taskId); }

async function sendBlueprintApply(
  envId: string,
  request: BlueprintApplyRequest,
  revision: string,
  key: string,
): Promise<BlueprintTaskAccepted> {
  const path = `/environments/${encodeURIComponent(envId)}/blueprint`;
  const multipart = await createBlueprintMultipartBody(request);
  let response: Response;
  try {
    response = await fetch(`/api/v1${path}`, {
      method: "PUT",
      headers: {
        Accept: "application/json",
        "Content-Type": multipart.contentType,
        "If-Match": `"${revision}"`,
        "Idempotency-Key": key,
      },
      body: multipart.body,
    });
  } catch (error) {
    throw new ControllerTransportError(`PUT ${path}: response unavailable`, error);
  }
  if (response.status !== 202)
    throw await controllerResponseError(response, "PUT", path);
  return (await response.json()) as BlueprintTaskAccepted;
}

export async function resolvePendingBlueprintApply() {
  const taskId = await intent().publish(
    sendBlueprintApply,
    (error) => error instanceof ControllerRequestError &&
      error.status >= 400 && error.status < 500 &&
      error.status !== 408 && error.status !== 429,
  );
  return { task_id: taskId };
}

const getBlueprint: BlueprintActions["getBlueprint"] = async (envId) =>
  controllerRequest<BlueprintDocumentResponse>(
    `/environments/${encodeURIComponent(envId)}/blueprint`,
    200,
  );

export function createBlueprintActions(
  assertEnvironmentMutable: (environmentId: string, operation: string) => void,
): BlueprintActions {
  return {
    getBlueprint,
    validateBlueprint: async (envId, request, expectedRevision) => {
      const path = `/environments/${encodeURIComponent(envId)}/blueprint/validate`;
      const multipart = await createBlueprintMultipartBody(request);
      const response = await fetch(`/api/v1${path}`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": multipart.contentType,
          "If-Match": `"${expectedRevision}"`,
        },
        body: multipart.body,
      });
      if (response.status !== 200)
        throw await controllerResponseError(response, "POST", path);
      return (await response.json()) as BlueprintValidationResponse;
    },
    applyBlueprint: async (envId, request, expectedRevision, key = newULID()) => {
      const pending = intent().current;
      if (pending) {
        if (pending.environmentId === envId && pending.key === key)
          return resolvePendingBlueprintApply();
        throw new Error("An earlier Blueprint Apply is unresolved. Resolve it before starting another Apply.");
      }
      assertEnvironmentMutable(envId, "desired-state apply");
      const path = `/environments/${encodeURIComponent(envId)}/blueprint`;
      const revision =
        expectedRevision ??
        (await controllerRequest<BlueprintDocumentResponse>(path, 200))
          .revision;
      await intent().begin(envId, request, revision, key);
      return resolvePendingBlueprintApply();
    },
  };
}
