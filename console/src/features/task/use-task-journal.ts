import { useCallback, useEffect, useRef } from "react";
import type {
  ActivityEntry,
  TaskJournalScope,
  TaskJournalState,
  TaskJournalSurface,
} from "@/lib/types";
import type { TaskResponse } from "@/features/environment/environment-removal-model";
import { controllerRequest } from "@/lib/controller-json-request";
import {
  emptyTaskJournal,
  taskJournalKey,
  taskJournalQuery,
  taskFromAPI,
  type TaskPageResponse,
} from "./journal-model";
export type TaskJournalStoreState = {
  activity: ActivityEntry[];
  taskJournals: Record<string, TaskJournalState>;
};
export type TaskJournalActions = {
  getTaskJournal: (scope: TaskJournalScope) => TaskJournalState;
  loadTaskJournal: (
    surface: TaskJournalSurface,
    scope: TaskJournalScope,
    cursor?: string,
  ) => Promise<void>;
  getTaskJournalDetail: (
    taskId: string,
    signal?: AbortSignal,
  ) => Promise<ActivityEntry>;
};
export function useTaskJournal(
  state: TaskJournalStoreState,
  update: (change: (draft: TaskJournalStoreState) => void) => void,
): TaskJournalActions {
  const taskJournalEpochs = useRef(new Map<string, number>());
  const loadTaskJournal = useCallback<TaskJournalActions["loadTaskJournal"]>(
    async (surface, scope, cursor) => {
      const key = taskJournalKey(scope);
      const epoch = (taskJournalEpochs.current.get(key) ?? 0) + 1;
      taskJournalEpochs.current.set(key, epoch);
      update((draft) => {
        const journal = draft.taskJournals[key] ?? emptyTaskJournal();
        journal.loadError = null;
        journal.failedCursor = null;
        journal.loading = !cursor;
        journal.loadingMore = !!cursor;
        draft.taskJournals[key] = journal;
      });
      try {
        const page = await controllerRequest<TaskPageResponse>(
          `/${surface}?${taskJournalQuery(scope, cursor)}`,
          200,
        );
        const entries = (page.items ?? []).map(taskFromAPI);
        if (taskJournalEpochs.current.get(key) !== epoch) return;
        update((draft) => {
          const journal = draft.taskJournals[key] ?? emptyTaskJournal();
          journal.entries = cursor ? [...journal.entries, ...entries] : entries;
          journal.nextCursor = page.next_cursor ?? null;
          journal.loaded = true;
          journal.loading = false;
          journal.loadingMore = false;
          journal.loadError = null;
          journal.failedCursor = null;
          draft.taskJournals[key] = journal;
        });
      } catch (error) {
        if (taskJournalEpochs.current.get(key) !== epoch) return;
        update((draft) => {
          const journal = draft.taskJournals[key] ?? emptyTaskJournal();
          journal.loaded = true;
          journal.loading = false;
          journal.loadingMore = false;
          journal.loadError =
            error instanceof Error ? error.message : "Unable to load Tasks";
          journal.failedCursor = cursor ?? null;
          draft.taskJournals[key] = journal;
        });
        throw error;
      }
    },
    [update],
  );

  useEffect(() => {
    void loadTaskJournal("tasks", { kind: "all" }).catch(() => undefined);
  }, [loadTaskJournal]);

  return {
    getTaskJournal: (scope) =>
      state.taskJournals[taskJournalKey(scope)] ?? emptyTaskJournal(),
    loadTaskJournal,
    getTaskJournalDetail: async (taskId, signal) =>
      taskFromAPI(
        await controllerRequest<TaskResponse>(
          `/tasks/${encodeURIComponent(taskId)}`,
          200,
          { signal },
        ),
      ),
  };
}
