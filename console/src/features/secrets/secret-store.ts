import { useCallback, useRef } from "react";
import type { operations } from "@/lib/api.generated";
import type { Project, ReusableSecret, SecretKind } from "@/lib/types";
import { controllerRequest } from "@/lib/controller-json-request";

type SecretPageResponse =
  operations["secret.list"]["responses"][200]["content"]["application/json"];
type SecretResponse =
  operations["secret.create"]["responses"][201]["content"]["application/json"];
type SecretCreateRequest =
  operations["secret.create"]["requestBody"]["content"]["application/json"];
type SecretTaskAccepted =
  operations["secret.remove"]["responses"][202]["content"]["application/json"];
type SecretValueResponse =
  operations["secret.reveal"]["responses"][200]["content"]["application/json"];
type ReusableSecretCreateInput = {
  key: string;
  kind: SecretKind;
  path?: string;
  value: string;
} & (
  | { projectId: string; platform?: never }
  | { projectId?: never; platform: true }
);
export type ReusableSecretState = {
  reusableSecrets: ReusableSecret[];
  reusableSecretsLoading: boolean;
  secretError: string | null;
};
type UpdateSecrets = (change: (draft: ReusableSecretState) => void) => void;
export type ReusableSecretActions = {
  createReusableSecret: (
    input: ReusableSecretCreateInput,
  ) => Promise<ReusableSecret>;
  removeReusableSecret: (id: string) => Promise<{ task_id: string }>;
  refreshReusableSecrets: (signal?: AbortSignal) => Promise<void>;
  revealReusableSecret: (id: string) => Promise<string>;
};

type ExpectedSecretScope = { projectId: string } | { platform: true };

function reusableSecretFromAPI(
  secret: SecretResponse,
  expectedScope: ExpectedSecretScope,
): ReusableSecret {
  const kind: SecretKind | null =
    secret.kind === "env_var" ? "env" : secret.kind === "file" ? "file" : null;
  if (!kind)
    throw new Error(
      "Controller returned a reusable Secret with an invalid kind",
    );

  const base = {
    id: secret.id,
    key: secret.key,
    kind,
    ref: secret.ref,
    updatedAt: secret.updated_at,
  };

  if ("projectId" in expectedScope) {
    if (
      secret.scope !== "project" ||
      secret.project_id !== expectedScope.projectId
    ) {
      throw new Error(
        "Controller returned a reusable Secret for the wrong project owner",
      );
    }
    return { ...base, scope: "project", projectId: expectedScope.projectId };
  }
  if (secret.scope !== "platform" || secret.project_id !== undefined) {
    throw new Error(
      "Controller returned a reusable Secret for the wrong platform owner",
    );
  }
  return { ...base, scope: "platform" };
}

async function listReusableSecretScope(
  expectedScope: ExpectedSecretScope,
  signal?: AbortSignal,
): Promise<ReusableSecret[]> {
  const secrets: ReusableSecret[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({ limit: "200" });
    if ("projectId" in expectedScope)
      query.set("project", expectedScope.projectId);
    else query.set("platform", "true");
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<SecretPageResponse>(
      "/secrets?" + query.toString(),
      200,
      { signal },
    );
    secrets.push(
      ...(page.items ?? []).map((secret) =>
        reusableSecretFromAPI(secret, expectedScope),
      ),
    );
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return secrets;
}

export function useSecretRefresh(
  reusableSecretProjectIds: string,
  update: UpdateSecrets,
) {
  const reusableSecretLoadGeneration = useRef(0);
  const refreshReusableSecrets = useCallback(
    async (signal?: AbortSignal) => {
      const loadGeneration = reusableSecretLoadGeneration.current + 1;
      reusableSecretLoadGeneration.current = loadGeneration;
      update((draft) => {
        draft.reusableSecretsLoading = true;
      });
      try {
        const projectIds = reusableSecretProjectIds
          ? reusableSecretProjectIds.split(",")
          : [];
        const scopes: ExpectedSecretScope[] = [
          { platform: true },
          ...projectIds.map((projectId) => ({ projectId })),
        ];
        const pages = await Promise.all(
          scopes.map((scope) => listReusableSecretScope(scope, signal)),
        );
        if (reusableSecretLoadGeneration.current !== loadGeneration) return;
        update((draft) => {
          draft.reusableSecrets = pages.flat();
          draft.reusableSecretsLoading = false;
          draft.secretError = null;
        });
      } catch (error) {
        if (
          signal?.aborted ||
          reusableSecretLoadGeneration.current !== loadGeneration
        )
          return;
        update((draft) => {
          draft.reusableSecretsLoading = false;
          draft.secretError =
            error instanceof Error
              ? error.message
              : "Unable to load reusable Secrets";
        });
        throw error;
      }
    },
    [reusableSecretProjectIds, update],
  );

  return refreshReusableSecrets;
}

export function createSecretActions(
  state: ReusableSecretState & { tenantProjects: Pick<Project, "id">[] },
  update: UpdateSecrets,
  refreshReusableSecrets: ReusableSecretActions["refreshReusableSecrets"],
): ReusableSecretActions {
  return {
    createReusableSecret: async (input) => {
      const body: SecretCreateRequest = {
        key: input.key,
        kind: input.kind === "env" ? "env_var" : "file",
        value: input.value,
        ...(input.kind === "file" ? { path: input.path } : {}),
        ...(input.projectId !== undefined
          ? { project_id: input.projectId }
          : { platform: true }),
      };
      const expectedScope: ExpectedSecretScope =
        input.projectId !== undefined
          ? { projectId: input.projectId }
          : { platform: true };
      const created = reusableSecretFromAPI(
        await controllerRequest<SecretResponse>("/secrets", 201, {
          method: "POST",
          body,
        }),
        expectedScope,
      );
      update((draft) => {
        draft.reusableSecrets = draft.reusableSecrets.filter(
          (secret) => secret.id !== created.id,
        );
        draft.reusableSecrets.push(created);
        draft.secretError = null;
      });
      return created;
    },
    removeReusableSecret: async (id) => {
      const secret = state.reusableSecrets.find(
        (candidate) => candidate.id === id,
      );
      if (!secret) throw new Error("Reusable Secret was not found");
      if (
        secret.scope === "project" &&
        !state.tenantProjects.some((project) => project.id === secret.projectId)
      ) {
        throw new Error("Reusable Secret project owner was not found");
      }
      try {
        const accepted = await controllerRequest<SecretTaskAccepted>(
          "/secrets/" + encodeURIComponent(id),
          202,
          { method: "DELETE" },
        );
        if (!accepted.task_id)
          throw new Error("Controller response is missing task_id");
        update((draft) => {
          draft.secretError = null;
        });
        return accepted;
      } catch (error) {
        update((draft) => {
          draft.secretError =
            error instanceof Error
              ? error.message
              : "Unable to remove reusable Secret";
        });
        throw error;
      }
    },
    refreshReusableSecrets,
    revealReusableSecret: async (id) => {
      const revealed = await controllerRequest<SecretValueResponse>(
        "/secrets/" + encodeURIComponent(id) + "/value",
        200,
      );
      return revealed.value;
    },
  };
}
