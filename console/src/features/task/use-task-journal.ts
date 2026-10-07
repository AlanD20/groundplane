import { useCallback, useEffect, useRef } from "react";
import type {
  ActivityEntry,
  TaskJournalScope,
  TaskJournalState,
  TaskJournalSurface,
  TaskPageSize,
} from "@/lib/types";
import type { TablePageSize } from "@/lib/console-preferences";
import { controllerRequest } from "@/lib/controller-json-request";
import { requestTask } from "./api";
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
  defaultTablePageSize: TablePageSize;
};
export type TaskJournalActions = {
  getTaskJournal: (scope: TaskJournalScope) => TaskJournalState;
  loadTaskJournal: (
    surface: TaskJournalSurface,
    scope: TaskJournalScope,
    cursor?: string,
    pageSize?: TaskPageSize,
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
  const pageSizes = useRef(new Map<string, TaskPageSize>());
  const selectedPageSizes = useRef(new Map<string, TaskPageSize>());
  const defaultPageSize = state.defaultTablePageSize;
  const loadTaskJournal = useCallback<TaskJournalActions["loadTaskJournal"]>(
    async (surface, scope, cursor, requestedPageSize) => {
      const key = taskJournalKey(scope);
      const pageSize =
        requestedPageSize ??
        selectedPageSizes.current.get(key) ??
        defaultPageSize;
      if (pageSize !== (pageSizes.current.get(key) ?? defaultPageSize))
        cursor = undefined;
      if (requestedPageSize !== undefined)
        selectedPageSizes.current.set(key, requestedPageSize);
      pageSizes.current.set(key, pageSize);
      const epoch = (taskJournalEpochs.current.get(key) ?? 0) + 1;
      taskJournalEpochs.current.set(key, epoch);
      update((draft) => {
        const journal = draft.taskJournals[key] ?? emptyTaskJournal(pageSize);
        if (journal.pageSize !== pageSize) {
          journal.entries = [];
          journal.nextCursor = null;
          journal.pageIndex = 0;
          journal.pageCursors = [undefined];
        }
        journal.pageSize = pageSize;
        journal.loadError = null;
        journal.failedCursor = null;
        journal.loading = !cursor;
        journal.loadingMore = !!cursor;
        draft.taskJournals[key] = journal;
      });
      try {
        const page = await controllerRequest<TaskPageResponse>(
          `/${surface}?${taskJournalQuery(scope, cursor, pageSize)}`,
          200,
        );
        const entries = (page.items ?? []).map(taskFromAPI);
        if (taskJournalEpochs.current.get(key) !== epoch) return;
        update((draft) => {
          const journal = draft.taskJournals[key] ?? emptyTaskJournal(pageSize);
          const knownPage = cursor ? journal.pageCursors.indexOf(cursor) : 0;
          const pageIndex = knownPage < 0 ? journal.pageIndex + 1 : knownPage;
          journal.entries = entries;
          journal.pageIndex = pageIndex;
          journal.pageCursors = [
            ...journal.pageCursors.slice(0, pageIndex),
            cursor,
          ];
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
          const journal = draft.taskJournals[key] ?? emptyTaskJournal(pageSize);
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
    [update, defaultPageSize],
  );

  useEffect(() => {
    void loadTaskJournal("tasks", { kind: "all" }).catch(() => undefined);
  }, [loadTaskJournal]);

  return {
    getTaskJournal: (scope) =>
      state.taskJournals[taskJournalKey(scope)] ??
      emptyTaskJournal(defaultPageSize),
    loadTaskJournal,
    getTaskJournalDetail: async (taskId, signal) =>
      taskFromAPI(await requestTask(taskId, signal)),
  };
}
