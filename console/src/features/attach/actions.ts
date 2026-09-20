import { controllerRequest } from "@/lib/controller-json-request";

import { requireTaskId } from "@/features/task/journal-model";
import {
  findEnvironment,
  type EnvironmentRemovalDraft,
} from "@/features/environment/environment-removal-model";
import {
  revealAttachFactValue,
  type AttachCreateRequest,
  type AttachTaskAccepted,
  type AttachRenameRequest,
  type AttachRenameResponse,
} from "./api";
type AttachCreateInput = {
  serviceId: string;
  backingServiceId: string;
  name?: string;
  credential: { mode: "new" } | { mode: "existing"; attachId: string };
  grantAttachIds?: string[];
};

export type AttachActions = {
  addAttach: (envId: string, input: AttachCreateInput) => Promise<string>;
  renameAttach: (
    envId: string,
    attachId: string,
    name: string,
  ) => Promise<void>;
  removeAttach: (envId: string, attachId: string) => Promise<string>;
  revealAttachFact: (
    attachId: string,
    key: string,
    grantAttachId?: string,
  ) => Promise<string>;
};
export function createAttachActions(
  update: (change: (draft: EnvironmentRemovalDraft) => void) => void,
  assertEnvironmentMutable: (environmentId: string, operation: string) => void,
): AttachActions {
  return {
    addAttach: async (_envId, input) => {
      assertEnvironmentMutable(_envId, "Attach mutation");
      const body: AttachCreateRequest = {
        service_id: input.serviceId,
        backing_service_id: input.backingServiceId,
        name: input.name,
        credential:
          input.credential.mode === "new"
            ? { mode: "new" }
            : { mode: "existing", attach_id: input.credential.attachId },
        grant_attach_ids:
          input.credential.mode === "new" ? input.grantAttachIds : undefined,
      };
      const response = await controllerRequest<AttachTaskAccepted>(
        "/attaches",
        202,
        { method: "POST", body },
      );
      return requireTaskId(response, "Attach creation");
    },
    renameAttach: async (envId, attachId, name) => {
      assertEnvironmentMutable(envId, "Attach mutation");
      const body: AttachRenameRequest = { name };
      const response = await controllerRequest<AttachRenameResponse>(
        `/attaches/${encodeURIComponent(attachId)}/rename`,
        200,
        { method: "POST", body },
      );
      update((draft) => {
        const attach = findEnvironment(draft, envId)?.attaches.find(
          (candidate) => candidate.id === attachId,
        );
        if (attach) attach.name = response.name;
      });
    },
    removeAttach: async (_envId, attachId) => {
      assertEnvironmentMutable(_envId, "Attach mutation");
      const response = await controllerRequest<AttachTaskAccepted>(
        `/attaches/${encodeURIComponent(attachId)}`,
        202,
        {
          method: "DELETE",
        },
      );
      return requireTaskId(response, "Attach removal");
    },
    revealAttachFact: (attachId, key, grantAttachId) =>
      revealAttachFactValue(attachId, key, grantAttachId),
  };
}
