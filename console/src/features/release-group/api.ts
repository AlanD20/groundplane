import type { operations } from "@/lib/api.generated";
import type { Service, ReleaseGroup } from "@/lib/types";
import { controllerRequest } from "@/lib/controller-json-request";
export type ReleaseGroupMutationAccepted =
  operations["release-group.remove"]["responses"][202]["content"]["application/json"];
export type ReleaseGroupTaskAccepted =
  operations["release-group.deploy"]["responses"][202]["content"]["application/json"];
export type ReleaseGroupPageResponse =
  operations["release-group.list"]["responses"][200]["content"]["application/json"];
export type ReleaseGroupResponse =
  operations["release-group.show"]["responses"][200]["content"]["application/json"];
export type ReleaseGroupRollbackPreviewResponse =
  operations["release-group.rollback-preview"]["responses"][200]["content"]["application/json"];
export async function listAllReleaseGroups(
  environmentId: string,
  services: Service[],
  signal?: AbortSignal,
): Promise<ReleaseGroup[]> {
  const groups: ReleaseGroup[] = [];
  const names = new Map(services.map((service) => [service.id, service.name]));
  let cursor = "";
  do {
    const query = new URLSearchParams({
      environment_id: environmentId,
      limit: "200",
    });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<ReleaseGroupPageResponse>(
      `/release-groups?${query}`,
      200,
      { signal },
    );
    groups.push(
      ...(page.items ?? []).map((group) => ({
        id: group.id,
        name: group.name,
        services: (group.service_ids ?? []).map((id) => names.get(id) ?? id),
        order: (group.order ?? []).map((id) => names.get(id) ?? id),
        tag: group.tag,
        onFailure:
          group.on_failure === "leave_active"
            ? ("leave_active" as const)
            : ("switch_back" as const),
      })),
    );
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return groups;
}
