import type { RefObject } from "react";
import type { EnvironmentEntry } from "@/lib/types";
import { entryFromAPI } from "@/features/entry/projection";
import { controllerRequest } from "@/lib/controller-json-request";

import {
  findEnvironment,
  type EnvironmentRemovalDraft,
  type PendingResourceRemoval,
} from "@/features/environment/environment-removal-model";
import type {
  EntryCreateRequest,
  EntryEditRequest,
  EntryResponse,
  EntryBulkUpsertRequest,
  EntryBulkUpsertResponse,
  EntryValueResponse,
} from "./api";
export type EntryActions = {
  addEntry: (
    envId: string,
    input: Omit<EntryCreateRequest, "environment_id">,
  ) => Promise<EnvironmentEntry>;
  bulkUpsertEntries: (
    envId: string,
    input: Omit<EntryBulkUpsertRequest, "environment_id">,
  ) => Promise<EntryBulkUpsertResponse>;
  updateEntry: (
    envId: string,
    entryId: string,
    input: EntryEditRequest,
  ) => Promise<EnvironmentEntry>;
  removeEntry: (envId: string, entryId: string) => Promise<string>;
  revealEntry: (entryId: string) => Promise<string>;
};
export function createEntryActions(
  update: (change: (draft: EnvironmentRemovalDraft) => void) => void,
  assertEnvironmentMutable: (environmentId: string, operation: string) => void,
  nextEnvironmentGeneration: (environmentId: string, intent: "child") => number,
  environmentGenerations: RefObject<Map<string, number>>,
  dispatchResourceRemoval: (removal: PendingResourceRemoval) => Promise<string>,
): EntryActions {
  return {
    addEntry: async (envId, input) => {
      assertEnvironmentMutable(envId, "Entry mutation");
      const generation = nextEnvironmentGeneration(envId, "child");
      const response = await controllerRequest<EntryResponse>("/entries", 201, {
        method: "POST",
        body: { ...input, environment_id: envId },
      });
      const entry = entryFromAPI(response);
      update((draft) => {
        if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
          return;
        findEnvironment(draft, envId)?.entries.push(entry);
      });
      return entry;
    },
    bulkUpsertEntries: async (envId, input) => {
      assertEnvironmentMutable(envId, "Entry mutation");
      const generation = nextEnvironmentGeneration(envId, "child");
      const response = await controllerRequest<EntryBulkUpsertResponse>(
        "/entries/bulk",
        202,
        {
          method: "POST",
          body: { ...input, environment_id: envId },
        },
      );
      const entries = (response.entries ?? []).map(entryFromAPI);
      update((draft) => {
        if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
          return;
        const environment = findEnvironment(draft, envId);
        if (!environment) return;
        const ids = new Set(entries.map((entry) => entry.id));
        environment.entries = environment.entries.filter(
          (entry) => !ids.has(entry.id),
        );
        environment.entries.push(...entries);
      });
      return response;
    },
    updateEntry: async (envId, entryId, input) => {
      assertEnvironmentMutable(envId, "Entry mutation");
      const generation = nextEnvironmentGeneration(envId, "child");
      const response = await controllerRequest<EntryResponse>(
        `/entries/${encodeURIComponent(entryId)}`,
        200,
        {
          method: "PATCH",
          body: input,
        },
      );
      const entry = entryFromAPI(response);
      update((draft) => {
        if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
          return;
        const environment = findEnvironment(draft, envId);
        if (!environment) return;
        const index = environment.entries.findIndex(
          (candidate) => candidate.id === entryId,
        );
        if (index >= 0) environment.entries[index] = entry;
      });
      return entry;
    },
    removeEntry: (envId, entryId) => (
      assertEnvironmentMutable(envId, "Entry mutation"),
      dispatchResourceRemoval({
        kind: "entry",
        environmentId: envId,
        resourceId: entryId,
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
