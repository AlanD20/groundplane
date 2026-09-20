import type { operations } from "@/lib/api.generated";
import type { Project } from "@/lib/types";
import { listAllEnvironments } from "@/features/environment/workspace-read";
import { controllerRequest } from "@/lib/controller-json-request";
export type ProjectPageResponse =
  operations["project.list"]["responses"][200]["content"]["application/json"];
export type ProjectCreateRequest =
  operations["project.create"]["requestBody"]["content"]["application/json"];
export type ProjectCreateResponse =
  operations["project.create"]["responses"][201]["content"]["application/json"];
export type ProjectShowResponse =
  operations["project.show"]["responses"][200]["content"]["application/json"];
export type ProjectEditRequest =
  operations["project.edit"]["requestBody"]["content"]["application/json"];
export type ProjectEditResponse =
  operations["project.edit"]["responses"][200]["content"]["application/json"];
export type ProjectRenameRequest =
  operations["project.rename"]["requestBody"]["content"]["application/json"];
export type ProjectRenameResponse =
  operations["project.rename"]["responses"][200]["content"]["application/json"];
export function projectFromAPI(
  project: ProjectCreateResponse | ProjectShowResponse,
): Project {
  if (project.kind !== "tenant" && project.kind !== "backing") {
    throw new Error(`Controller returned unknown project kind ${project.kind}`);
  }
  return {
    id: project.id,
    tenantId: project.tenant_id ?? null,
    slug: project.slug,
    name: project.name,
    description: project.description,
    kind: project.kind,
    deletionTaskId: project.deletion_task_id,
  };
}

export async function listAllTenantProjects(
  signal: AbortSignal,
): Promise<Project[]> {
  const projects: Project[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({ kind: "tenant", limit: "200" });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<ProjectPageResponse>(
      `/projects?${query}`,
      200,
      { signal },
    );
    projects.push(...(page.items ?? []).map(projectFromAPI));
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return Promise.all(
    projects.map(async (project) => ({
      ...project,
      environments: await listAllEnvironments(project.id, signal),
    })),
  );
}
