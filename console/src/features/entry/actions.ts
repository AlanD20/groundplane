import { requireTaskId } from "@/features/task/journal-model";
import { controllerRequest } from "@/lib/controller-json-request";

import { type PendingResourceRemoval } from "@/features/environment/environment-removal-model";
import type {
  EntryCreateRequest,
  EntryEditRequest,
  EntryMutationAccepted,
  EntryBulkUpsertRequest,
  EntryBulkUpsertResponse,
  EntryValueResponse,
} from "./api";
export type EntryActions = {
  addEntry: (
    envId: string,
    input: Omit<EntryCreateRequest, "environment_id">,
  ) => Promise<string>;
  bulkUpsertEntries: (
    envId: string,
    input: Omit<EntryBulkUpsertRequest, "environment_id">,
  ) => Promise<EntryBulkUpsertResponse>;
  updateEntry: (
    envId: string,
    entryId: string,
    input: EntryEditRequest,
  ) => Promise<string>;
  removeEntry: (
    envId: string,
    entryId: string,
    after?: string[],
  ) => Promise<string>;
  revealEntry: (entryId: string) => Promise<string>;
};
export function createEntryActions(
  assertEnvironmentMutable: (environmentId: string, operation: string) => void,
  dispatchResourceRemoval: (removal: PendingResourceRemoval) => Promise<string>,
): EntryActions {
  return {
    addEntry: async (envId, input) => {
      assertEnvironmentMutable(envId, "Entry mutation");
      const response = await controllerRequest<EntryMutationAccepted>(
        "/entries",
        202,
        {
          method: "POST",
          body: { ...input, environment_id: envId },
        },
      );
      return requireTaskId(response, "Variable or file creation");
    },
    bulkUpsertEntries: async (envId, input) => {
      assertEnvironmentMutable(envId, "Entry mutation");
      return controllerRequest<EntryBulkUpsertResponse>("/entries/bulk", 202, {
        method: "POST",
        body: { ...input, environment_id: envId },
      });
    },
    updateEntry: async (envId, entryId, input) => {
      assertEnvironmentMutable(envId, "Entry mutation");
      const response = await controllerRequest<EntryMutationAccepted>(
        `/entries/${encodeURIComponent(entryId)}`,
        202,
        { method: "PATCH", body: input },
      );
      return requireTaskId(response, "Variable or file edit");
    },
    removeEntry: (envId, entryId, after = []) => (
      assertEnvironmentMutable(envId, "Entry mutation"),
      dispatchResourceRemoval({
        kind: "entry",
        environmentId: envId,
        resourceId: entryId,
        after,
      })
    ),
    revealEntry: async (entryId) => {
      const value = await controllerRequest<EntryValueResponse>(
        `/entries/${encodeURIComponent(entryId)}/value`,
        200,
      );
      return value.value;
    },
  };
}
