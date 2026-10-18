import type { Project } from "@/lib/types";
import type { TaskResponse } from "@/features/task/api";

export type PendingResourceRemoval =
  | {
      kind: "environment";
      projectId: string;
      resourceId: string;
      generation: number;
    }
  | {
      kind: "service" | "route" | "entry" | "script";
      environmentId: string;
      resourceId: string;
      generation?: number;
    };

export type EnvironmentMutationKind =
  "create" | "edit" | "rename" | "delete" | "child";

export type EnvironmentDeletionFailure = {
  taskId: string;
  projectId: string;
  message: string;
  kind: "task" | "refresh";
};

export type EnvironmentRemovalDraft = {
  tenantProjects: Project[];
  backingProjects: Project[];
};

export function findEnvironment(
  draft: EnvironmentRemovalDraft,
  environmentId: string,
) {
  for (const project of [...draft.tenantProjects, ...draft.backingProjects]) {
    const environment = project.environments?.find(
      (candidate) => candidate.id === environmentId,
    );
    if (environment) return environment;
  }
  return undefined;
}

export function environmentDeletionTaskError(
  taskId: string,
  detail: TaskResponse,
  removal: PendingResourceRemoval,
) {
  const task = detail.task;
  if (task.id !== taskId)
    return `Controller returned Task ${task.id} while observing ${taskId}`;
  if (task.action !== "remove")
    return `Controller returned ${task.action} Task ${taskId} for resource deletion`;
  if (task.target_id !== removal.resourceId)
    return `Controller returned deletion Task ${taskId} for a different resource`;
  if (
    removal.kind === "environment" &&
    task.scope.project_id !== removal.projectId
  ) {
    return `Controller returned deletion Task ${taskId} for a different Project`;
  }
  const environmentId =
    removal.kind === "environment" ? removal.resourceId : removal.environmentId;
  if (
    task.scope.environment_id &&
    task.scope.environment_id !== environmentId
  ) {
    return `Controller returned deletion Task ${taskId} for a different Environment`;
  }
  return undefined;
}
