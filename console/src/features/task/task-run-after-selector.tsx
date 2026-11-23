import { useVisibleRefresh } from "@/lib/use-visible-refresh";
import { useEffect, useId, useState } from "react";
import { SearchableChecklist } from "@/components/common/searchable-checklist";
import { TaskLink } from "@/components/common/task-link";
import { StatusBadge } from "@/components/common/status-badge";
import { Check } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useStore } from "@/lib/store";
import type { ActivityEntry } from "@/lib/types";
import { TaskPagination } from "./task-pagination";
import { requestTask } from "./api";
import { taskFromAPI } from "./journal-model";
import { taskPresentation } from "./task-overview";

const scope = { kind: "all", status: "active" } as const;

/** Whole-Task prerequisites; the Controller owns ordering and failure propagation. */
export function TaskRunAfterSelector({
  value,
  onChange,
  onBusyChange,
}: {
  value: string[];
  onChange: (ids: string[]) => void;
  onBusyChange: (busy: boolean) => void;
}) {
  const store = useStore();
  const { getTaskJournal, loadTaskJournal, getTaskJournalDetail } = store;
  const journal = getTaskJournal(scope);
  const [known, setKnown] = useState<Record<string, ActivityEntry>>({});
  const [manual, setManual] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [waitForTasks, setWaitForTasks] = useState(value.length > 0);
  const choosing = waitForTasks || value.length > 0;
  const selectedIds = value.join(",");
  const [refreshError, setRefreshError] = useState("");
  const inputId = useId();
  useEffect(() => {
    onBusyChange(
      loading || manual.trim().length > 0 || (choosing && value.length === 0),
    );
  }, [loading, manual, onBusyChange, choosing, value.length]);
  useVisibleRefresh(
    async (signal) => {
      if (journal.loading || journal.loadingMore) return;
      const [, results] = await Promise.all([
        loadTaskJournal(
          "tasks",
          scope,
          journal.pageCursors[journal.pageIndex],
          journal.pageSize,
        ).catch(() => undefined),
        Promise.allSettled(
          (selectedIds ? selectedIds.split(",") : []).map(async (id) =>
            taskFromAPI((await requestTask(id, signal, { limit: 1 })).task),
          ),
        ),
      ]);
      if (signal.aborted) return;
      const loaded = results.flatMap((result) =>
        result.status === "fulfilled" ? [result.value] : [],
      );
      setKnown((current) => ({
        ...current,
        ...Object.fromEntries(loaded.map((task) => [task.id, task])),
      }));
      setRefreshError(
        results.some((result) => result.status === "rejected")
          ? "Some prerequisite statuses could not be refreshed."
          : "",
      );
    },
    {
      enabled: choosing,
      interval: 5000,
      key: `task-prerequisites/${journal.pageIndex}/${journal.pageSize}/${selectedIds}`,
    },
  );
  const tasks = new Map(
    [...journal.entries, ...Object.values(known)].map((task) => [
      task.id,
      task,
    ]),
  );
  function select(ids: string[]) {
    setKnown((current) => ({
      ...current,
      ...Object.fromEntries(
        ids.flatMap((id) => (tasks.has(id) ? [[id, tasks.get(id)!]] : [])),
      ),
    }));
    onChange(ids);
  }
  async function add() {
    const id = manual.trim();
    if (!/^task_[0-9A-HJKMNP-TV-Z]{26}$/.test(id)) {
      setError("Enter a valid Task ID.");
      return;
    }
    if (value.includes(id)) {
      setManual("");
      return;
    }
    if (value.length >= 16) {
      setError("Choose at most 16 Tasks.");
      return;
    }
    setLoading(true);
    setError("");
    try {
      const task = await getTaskJournalDetail(id);
      if (task.finishedAt) {
        setError("This Task has already finished. Choose an unfinished Task.");
        return;
      }
      setKnown((current) => ({ ...current, [id]: task }));
      onChange([...value, id]);
      setManual("");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Unable to load Task");
    } finally {
      setLoading(false);
    }
  }
  return (
    <section
      className="min-w-0 space-y-3 border-t border-border pt-4"
      aria-label="Execution timing"
    >
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h3 className="text-sm font-medium">Execution timing</h3>
        <div
          role="group"
          aria-label="When to start"
          className="flex flex-wrap gap-1 rounded-lg border border-border p-1"
        >
          <Button
            type="button"
            size="sm"
            variant={!choosing ? "default" : "ghost"}
            aria-pressed={!choosing}
            disabled={loading}
            onClick={() => {
              setWaitForTasks(false);
              setManual("");
              setError("");
              onChange([]);
            }}
          >
            <Check
              aria-hidden
              className={choosing ? "invisible size-3" : "size-3"}
            />
            Start immediately
          </Button>
          <Button
            type="button"
            size="sm"
            variant={choosing ? "default" : "ghost"}
            aria-pressed={choosing}
            onClick={() => {
              setWaitForTasks(true);
            }}
          >
            <Check
              aria-hidden
              className={choosing ? "size-3" : "invisible size-3"}
            />
            Wait for Tasks{value.length > 0 ? ` · ${value.length}` : ""}
          </Button>
        </div>
      </div>
      {choosing && (
        <>
          <fieldset
            disabled={loading}
            data-form-shortcuts="ignore"
            onKeyDown={(event) => {
              if (
                event.key === "Enter" &&
                event.target instanceof HTMLInputElement &&
                event.target.type !== "checkbox"
              ) {
                event.preventDefault();
                event.stopPropagation();
              }
            }}
          >
            <SearchableChecklist
              label="Run after these Tasks"
              maximum={16}
              selected={value}
              onChange={select}
              searchPlaceholder="Search Tasks on this page…"
              emptyMessage="No unfinished Tasks on this page."
              options={journal.entries
                .map((task) => tasks.get(task.id) ?? task)
                .filter((task) => !task.finishedAt)
                .map((task) => {
                  const presentation = taskPresentation(task, store);
                  return {
                    id: task.id,
                    label: presentation.title,
                    detail: `${task.status.replaceAll("_", " ")} · ${presentation.scope}`,
                    searchText: task.id,
                  };
                })}
            />
            <TaskPagination scope={scope} surface="tasks" />
            <Button
              type="button"
              variant="link"
              size="sm"
              disabled={journal.loading || journal.loadingMore}
              onClick={() =>
                void loadTaskJournal("tasks", scope).catch(() => undefined)
              }
            >
              Refresh available Tasks
            </Button>
          </fieldset>
          <p className="text-xs text-muted-foreground">
            Optional. Wait for selected Tasks to finish successfully before
            starting. Failed or cancelled prerequisites prevent this action from
            running.
          </p>
          <div
            data-form-shortcuts="ignore"
            className="space-y-2"
            onKeyDown={(event) => {
              if (
                event.key === "Enter" &&
                event.target instanceof HTMLInputElement
              ) {
                event.preventDefault();
                event.stopPropagation();
                if (!loading) void add();
              }
            }}
          >
            <Label htmlFor={inputId}>Add another Task by ID</Label>
            <div className="flex gap-2">
              <Input
                id={inputId}
                value={manual}
                onChange={(event) => setManual(event.target.value)}
                placeholder="task_…"
                disabled={loading}
              />
              <Button
                type="button"
                variant="outline"
                disabled={loading || !manual.trim() || value.length >= 16}
                onClick={() => void add()}
              >
                {loading ? "Loading…" : "Add Task"}
              </Button>
            </div>
          </div>
          {journal.loading && (
            <p role="status" className="text-xs text-muted-foreground">
              Loading unfinished Tasks…
            </p>
          )}
          {journal.loadError && (
            <p role="alert" className="text-xs text-destructive">
              {journal.loadError}
            </p>
          )}
          {error && (
            <p role="alert" className="text-xs text-destructive">
              {error}
            </p>
          )}
          {value.length > 0 && (
            <ul className="space-y-2">
              {value.map((id) => (
                <li key={id} className="flex flex-wrap items-center gap-2">
                  <TaskLink taskId={id}>{tasks.get(id)?.title ?? id}</TaskLink>
                  {tasks.has(id) && (
                    <StatusBadge status={tasks.get(id)!.status} />
                  )}
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    aria-label={`Remove prerequisite ${id}`}
                    onClick={() =>
                      onChange(value.filter((selected) => selected !== id))
                    }
                  >
                    Remove
                  </Button>
                </li>
              ))}
            </ul>
          )}
        </>
      )}
      {refreshError && (
        <p role="status" className="text-xs text-muted-foreground">
          {refreshError}
        </p>
      )}
    </section>
  );
}
