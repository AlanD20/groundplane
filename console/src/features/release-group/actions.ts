import type { ReleaseGroup } from "@/lib/types";
import { controllerRequest } from "@/lib/controller-json-request";
import { requireTaskId } from "@/features/task/journal-model";
import {
  findEnvironment,
  type EnvironmentRemovalDraft,
} from "@/features/environment/environment-removal-model";
import type {
  ReleaseGroupResponse,
  ReleaseGroupMutationAccepted,
  ReleaseGroupTaskAccepted,
  ReleaseGroupRollbackPreviewResponse,
} from "./api";
export type ReleaseGroupActions = {
  addReleaseGroup: (
    envId: string,
    group: ReleaseGroup,
  ) => Promise<ReleaseGroup>;
  updateReleaseGroup: (
    envId: string,
    groupId: string,
    patch: Pick<ReleaseGroup, "name" | "services" | "order" | "onFailure">,
  ) => Promise<ReleaseGroup>;
  removeReleaseGroup: (envId: string, groupId: string) => Promise<string>;
  deployReleaseGroup: (
    envId: string,
    groupId: string,
    tag?: string,
  ) => Promise<string>;
  previewReleaseGroupRollback: (
    envId: string,
    groupId: string,
    tag?: string,
  ) => Promise<ReleaseGroupRollbackPreviewResponse>;
  rollbackReleaseGroup: (
    envId: string,
    groupId: string,
    tag: string | undefined,
    previewRevision: string,
  ) => Promise<string>;
};
export function createReleaseGroupActions(
  state: EnvironmentRemovalDraft,
  update: (change: (draft: EnvironmentRemovalDraft) => void) => void,
  assertEnvironmentMutable: (environmentId: string, operation: string) => void,
): ReleaseGroupActions {
  return {
    addReleaseGroup: async (envId, group) => {
      assertEnvironmentMutable(envId, "Release group mutation");
      const environment = findEnvironment(state, envId);
      if (!environment) throw new Error(`Environment ${envId} was not found`);
      const serviceIDs = new Map(
        environment.services.map((service) => [service.name, service.id]),
      );
      const response = await controllerRequest<ReleaseGroupResponse>(
        "/release-groups",
        201,
        {
          method: "POST",
          body: {
            environment_id: envId,
            name: group.name,
            service_ids: group.services.map(
              (name) => serviceIDs.get(name) ?? name,
            ),
            order: group.order.map((name) => serviceIDs.get(name) ?? name),
            on_failure: group.onFailure,
          },
        },
      );
      const names = new Map(
        environment.services.map((service) => [service.id, service.name]),
      );
      const created: ReleaseGroup = {
        id: response.id,
        name: response.name,
        services: (response.service_ids ?? []).map((id) => names.get(id) ?? id),
        order: (response.order ?? []).map((id) => names.get(id) ?? id),
        tag: response.tag,
        onFailure:
          response.on_failure === "leave_active"
            ? "leave_active"
            : "switch_back",
      };
      update((draft) => {
        findEnvironment(draft, envId)?.releaseGroups.push(created);
      });
      return created;
    },
    updateReleaseGroup: async (envId, groupId, patch) => {
      assertEnvironmentMutable(envId, "Release group mutation");
      const environment = findEnvironment(state, envId);
      if (!environment) throw new Error(`Environment ${envId} was not found`);
      const serviceIDs = new Map(
        environment.services.map((service) => [service.name, service.id]),
      );
      const response = await controllerRequest<ReleaseGroupResponse>(
        `/release-groups/${encodeURIComponent(groupId)}`,
        200,
        {
          method: "PATCH",
          body: {
            name: patch.name,
            service_ids: patch.services.map(
              (name) => serviceIDs.get(name) ?? name,
            ),
            order: patch.order.map((name) => serviceIDs.get(name) ?? name),
            on_failure: patch.onFailure,
          },
        },
      );
      const names = new Map(
        environment.services.map((service) => [service.id, service.name]),
      );
      const edited: ReleaseGroup = {
        id: response.id,
        name: response.name,
        services: (response.service_ids ?? []).map((id) => names.get(id) ?? id),
        order: (response.order ?? []).map((id) => names.get(id) ?? id),
        tag: response.tag,
        onFailure:
          response.on_failure === "leave_active"
            ? "leave_active"
            : "switch_back",
      };
      update((draft) => {
        const groups = findEnvironment(draft, envId)?.releaseGroups;
        const index =
          groups?.findIndex((candidate) => candidate.id === groupId) ?? -1;
        if (groups && index >= 0) groups[index] = edited;
      });
      return edited;
    },
    removeReleaseGroup: async (_envId, groupId) => {
      assertEnvironmentMutable(_envId, "Release group mutation");
      const response = await controllerRequest<ReleaseGroupMutationAccepted>(
        `/release-groups/${encodeURIComponent(groupId)}`,
        202,
        { method: "DELETE" },
      );
      return requireTaskId(response, "Release group removal");
    },
    deployReleaseGroup: async (_envId, groupId, tag) => {
      assertEnvironmentMutable(_envId, "Release group mutation");
      const body = tag ? { tag } : {};
      const response = await controllerRequest<ReleaseGroupTaskAccepted>(
        `/release-groups/${encodeURIComponent(groupId)}/deploy`,
        202,
        { method: "POST", body },
      );
      return requireTaskId(response, "Release group deploy");
    },
    previewReleaseGroupRollback: async (_envId, groupId, tag) =>
      controllerRequest<ReleaseGroupRollbackPreviewResponse>(
        `/release-groups/${encodeURIComponent(groupId)}/rollback-preview${tag === undefined ? "" : `?tag=${encodeURIComponent(tag)}`}`,
        200,
      ),
    rollbackReleaseGroup: async (_envId, groupId, tag, previewRevision) => {
      assertEnvironmentMutable(_envId, "Release group mutation");
      const response = await controllerRequest<ReleaseGroupTaskAccepted>(
        `/release-groups/${encodeURIComponent(groupId)}/rollback`,
        202,
        {
          method: "POST",
          body: {
            ...(tag === undefined ? {} : { tag }),
            preview_revision: previewRevision,
          },
        },
      );
      return requireTaskId(response, "Release group rollback");
    },
  };
}
