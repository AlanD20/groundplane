import { useCallback } from "react";
import type { operations } from "@/lib/api.generated";
import type { Runner } from "@/lib/types";
import { controllerRequest } from "@/lib/controller-json-request";
type RunnerPageResponse =
  operations["runner.list"]["responses"][200]["content"]["application/json"];
type RunnerCreateRequest =
  operations["runner.create"]["requestBody"]["content"]["application/json"];
type RunnerCreateResponse =
  operations["runner.create"]["responses"][202]["content"]["application/json"];
type RunnerEditRequest =
  operations["runner.edit"]["requestBody"]["content"]["application/json"];
type RunnerEditResponse =
  operations["runner.edit"]["responses"][200]["content"]["application/json"];
type RunnerRetryRequest =
  operations["runner.retry"]["requestBody"]["content"]["application/json"];
type RunnerRetryResponse =
  operations["runner.retry"]["responses"][202]["content"]["application/json"];
type RunnerRemoveResponse =
  operations["runner.remove"]["responses"][202]["content"]["application/json"];

export type RunnerState = {
  runners: Runner[];
  runnersLoading: boolean;
  runnerError: string | null;
};
export type RunnerActions = {
  refreshRunners: (tenantId: string, projectIds: string[]) => Promise<void>;
  createRunner: (input: {
    slug: string;
    tenantId?: string;
    projectId?: string;
    githubUrl: string;
    labels: string[];
    registrationToken: string;
  }) => Promise<string>;
  renameRunner: (runnerId: string, slug: string) => Promise<Runner>;
  retryRunner: (runnerId: string, registrationToken: string) => Promise<string>;
  removeRunner: (runnerId: string) => Promise<string>;
};
type UpdateRunners = (change: (draft: RunnerState) => void) => void;
export function useRunnerStore(update: UpdateRunners): RunnerActions {
  const refreshRunners = useCallback(
    async (tenantId: string, projectIds: string[]) => {
      update((draft) => {
        draft.runnersLoading = true;
        draft.runnerError = null;
      });
      try {
        const paths = [
          `/runners?tenant=${encodeURIComponent(tenantId)}`,
          ...projectIds.map(
            (projectId) => `/runners?project=${encodeURIComponent(projectId)}`,
          ),
        ];
        const pages = await Promise.all(
          paths.map((path) => controllerRequest<RunnerPageResponse>(path, 200)),
        );
        const runners = new Map<string, Runner>();
        for (const page of pages) {
          for (const runner of page.items ?? []) {
            runners.set(runner.id, {
              id: runner.id,
              slug: runner.slug,
              tenantId: runner.tenant_id,
              projectId: runner.project_id ?? null,
              githubUrl: runner.github_url,
              name: runner.name,
              labels: runner.labels ?? [],
              lifecycle: runner.lifecycle,
              createTaskId: runner.create_task_id,
              removeTaskId: runner.remove_task_id,
              online: runner.online,
              observedAt: runner.observed_at,
              createdAt: runner.created_at,
            });
          }
        }
        update((draft) => {
          draft.runners = [...runners.values()].sort((left, right) =>
            left.id.localeCompare(right.id),
          );
          draft.runnersLoading = false;
        });
      } catch (error) {
        update((draft) => {
          draft.runnersLoading = false;
          draft.runnerError =
            error instanceof Error ? error.message : "Unable to load Runners";
        });
      }
    },
    [update],
  );
  const createRunner = useCallback(
    async (input: {
      slug: string;
      tenantId?: string;
      projectId?: string;
      githubUrl: string;
      labels: string[];
      registrationToken: string;
    }) => {
      const body: RunnerCreateRequest = {
        slug: input.slug,
        github_url: input.githubUrl,
        labels: input.labels,
        registration_token: input.registrationToken,
        ...(input.projectId
          ? { project_id: input.projectId }
          : { tenant_id: input.tenantId }),
      };
      const response = await controllerRequest<RunnerCreateResponse>(
        "/runners",
        202,
        { method: "POST", body },
      );
      body.registration_token = "";
      if (!response.task_id)
        throw new Error(
          "Controller response is missing Runner creation task_id",
        );
      return response.task_id;
    },
    [],
  );
  const renameRunner = useCallback(
    async (runnerId: string, slug: string) => {
      const body: RunnerEditRequest = { slug };
      const response = await controllerRequest<RunnerEditResponse>(
        `/runners/${encodeURIComponent(runnerId)}`,
        200,
        { method: "PATCH", body },
      );
      const renamed: Runner = {
        id: response.id,
        slug: response.slug,
        tenantId: response.tenant_id,
        projectId: response.project_id ?? null,
        githubUrl: response.github_url,
        name: response.name,
        labels: response.labels ?? [],
        lifecycle: response.lifecycle,
        createTaskId: response.create_task_id,
        removeTaskId: response.remove_task_id,
        online: response.online,
        observedAt: response.observed_at,
        createdAt: response.created_at,
      };
      update((draft) => {
        const index = draft.runners.findIndex(
          (runner) => runner.id === renamed.id,
        );
        if (index >= 0) draft.runners[index] = renamed;
      });
      return renamed;
    },
    [update],
  );
  const retryRunner = useCallback(
    async (runnerId: string, registrationToken: string) => {
      const body: RunnerRetryRequest = {
        registration_token: registrationToken,
      };
      const response = await controllerRequest<RunnerRetryResponse>(
        `/runners/${encodeURIComponent(runnerId)}/retry`,
        202,
        { method: "POST", body },
      );
      body.registration_token = "";
      if (!response.task_id)
        throw new Error("Controller response is missing Runner retry task_id");
      return response.task_id;
    },
    [],
  );
  const removeRunner = useCallback(async (runnerId: string) => {
    const response = await controllerRequest<RunnerRemoveResponse>(
      `/runners/${encodeURIComponent(runnerId)}`,
      202,
      { method: "DELETE" },
    );
    if (!response.task_id)
      throw new Error("Controller response is missing Runner removal task_id");
    return response.task_id;
  }, []);

  return {
    refreshRunners,
    createRunner,
    renameRunner,
    retryRunner,
    removeRunner,
  };
}
