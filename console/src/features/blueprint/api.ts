import type { operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { controllerResponseError } from "@/lib/controller-request-errors";
import {
  createBlueprintMultipartBody,
  type BlueprintApplyRequest,
} from "@/lib/blueprint-bundle";
import { newULID } from "@/lib/utils";

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
  ) => Promise<BlueprintTaskAccepted>;
};

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
    applyBlueprint: async (envId, request, expectedRevision) => {
      assertEnvironmentMutable(envId, "desired-state apply");
      const path = `/environments/${encodeURIComponent(envId)}/blueprint`;
      const revision =
        expectedRevision ??
        (await controllerRequest<BlueprintDocumentResponse>(path, 200))
          .revision;
      const multipart = await createBlueprintMultipartBody(request);
      const response = await fetch(`/api/v1${path}`, {
        method: "PUT",
        headers: {
          Accept: "application/json",
          "Content-Type": multipart.contentType,
          "If-Match": `"${revision}"`,
          "Idempotency-Key": newULID(),
        },
        body: multipart.body,
      });
      if (response.status !== 202)
        throw await controllerResponseError(response, "PUT", path);
      const accepted = (await response.json()) as BlueprintTaskAccepted;
      if (!accepted.task_id)
        throw new Error("Controller response is missing task_id");
      return accepted;
    },
  };
}
