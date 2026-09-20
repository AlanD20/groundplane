import { controllerRequest } from "@/lib/controller-json-request";
import type { Project } from "@/lib/types";
import {
  projectFromAPI,
  type ProjectCreateRequest,
  type ProjectCreateResponse,
  type ProjectEditRequest,
  type ProjectEditResponse,
  type ProjectRenameRequest,
  type ProjectRenameResponse,
} from "./api";
type HierarchyTaskAccepted = { task_id: string };
export type ProjectActions = {
  addProject: (p: {
    tenantId: string;
    slug: string;
    name: string;
    description: string;
  }) => Promise<Project>;
  editProject: (projectId: string, name: string) => Promise<Project>;
  renameProject: (projectId: string, slug: string) => Promise<Project>;
  deleteProject: (projectId: string) => Promise<string>;
};
export function createProjectActions(
  update: (change: (draft: { tenantProjects: Project[] }) => void) => void,
): ProjectActions {
  return {
    addProject: async (project) => {
      const body: ProjectCreateRequest = {
        tenant_id: project.tenantId,
        slug: project.slug,
        name: project.name,
        description: project.description,
      };
      const created = projectFromAPI(
        await controllerRequest<ProjectCreateResponse>("/projects", 201, {
          method: "POST",
          body,
        }),
      );
      update((draft) => {
        draft.tenantProjects.push(created);
      });
      return created;
    },
    editProject: async (projectId, name) => {
      const body: ProjectEditRequest = { name };
      const updated = projectFromAPI(
        await controllerRequest<ProjectEditResponse>(
          `/projects/${encodeURIComponent(projectId)}`,
          200,
          { method: "PATCH", body },
        ),
      );
      update((draft) => {
        const index = draft.tenantProjects.findIndex(
          (project) => project.id === updated.id,
        );
        if (index >= 0) draft.tenantProjects[index] = updated;
      });
      return updated;
    },
    renameProject: async (projectId, slug) => {
      const body: ProjectRenameRequest = { slug };
      const renamed = projectFromAPI(
        await controllerRequest<ProjectRenameResponse>(
          `/projects/${encodeURIComponent(projectId)}/rename`,
          200,
          { method: "POST", body },
        ),
      );
      update((draft) => {
        const index = draft.tenantProjects.findIndex(
          (project) => project.id === renamed.id,
        );
        if (index >= 0) draft.tenantProjects[index] = renamed;
      });
      return renamed;
    },
    deleteProject: async (projectId) => {
      const accepted = await controllerRequest<HierarchyTaskAccepted>(
        `/projects/${encodeURIComponent(projectId)}`,
        202,
        { method: "DELETE" },
      );
      if (!accepted.task_id)
        throw new Error("Controller response is missing task_id");
      return accepted.task_id;
    },
  };
}
