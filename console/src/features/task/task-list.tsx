import { HelpHint, ResourcePanel } from "@/components/common/resource-panel";
import { TaskJournalItem } from "@/components/common/task-journal-item";
import { Button } from "@/components/ui/button";
import { Select } from "@/components/ui/select";
import { useStore } from "@/lib/store";
import type {
  ActivityEntry,
  TaskJournalScope,
  TaskJournalSurface,
} from "@/lib/types";
import { RefreshCw } from "lucide-react";
import { useEffect, useState, type ReactNode } from "react";
import { TaskPagination } from "./task-pagination";

const filters = [
  { value: "all", label: "All statuses", match: () => true },
  {
    value: "running",
    label: "Running",
    match: (task: ActivityEntry) => task.status === "running",
  },
  {
    value: "pending",
    label: "Queued",
    match: (task: ActivityEntry) => task.status === "pending",
  },
  {
    value: "completed",
    label: "Completed",
    match: (task: ActivityEntry) => task.status === "completed",
  },
  {
    value: "failed",
    label: "Failed",
    match: (task: ActivityEntry) => task.status === "failed",
  },
  {
    value: "timed_out",
    label: "Timed out",
    match: (task: ActivityEntry) => task.status === "timed_out",
  },
  {
    value: "aborted",
    label: "Aborted",
    match: (task: ActivityEntry) => task.status === "aborted",
  },
];

export function TaskList({
  scope,
  title,
  surface = "tasks",
  filters: ownerFilters,
}: {
  scope: TaskJournalScope;
  title: string;
  surface?: TaskJournalSurface;
  filters?: ReactNode;
}) {
  const store = useStore();
  const [filter, setFilter] = useState("all");
  const journal = store.getTaskJournal(scope);
  const visible = journal.entries.filter(
    (task) =>
      filters.find((item) => item.value === filter)?.match(task) ?? true,
  );
  const busy = journal.loading || journal.loadingMore;
  useEffect(() => {
    void store.loadTaskJournal(surface, scope).catch(() => undefined);
  }, [scope, surface, store.loadTaskJournal]);
  return (
    <ResourcePanel
      title={
        <span className="flex items-center gap-2">
          {title}
          <HelpHint label="About Task filtering">
            Newest first. Status filters apply to the current page, not the
            entire history.
          </HelpHint>
        </span>
      }
      actions={
        <div className="flex flex-wrap items-center gap-2">
          {ownerFilters}
          <Select
            aria-label={`${title} status`}
            className="w-40"
            value={filter}
            options={filters}
            onValueChange={setFilter}
          />
          <Button
            variant="outline"
            size="icon-sm"
            aria-label={`Refresh ${title}`}
            disabled={busy}
            onClick={() =>
              void store.loadTaskJournal(surface, scope).catch(() => undefined)
            }
          >
            <RefreshCw className="size-3.5" />
          </Button>
        </div>
      }
    >
      {journal.loading && (
        <p role="status" className="text-xs text-muted-foreground">
          Loading Tasks…
        </p>
      )}
      {journal.loadError && (
        <div
          role="alert"
          className="flex items-center justify-between gap-3 text-xs text-destructive"
        >
          <span>{journal.loadError}</span>
          <Button
            variant="outline"
            size="sm"
            disabled={busy}
            onClick={() =>
              void store
                .loadTaskJournal(
                  surface,
                  scope,
                  journal.failedCursor ?? undefined,
                )
                .catch(() => undefined)
            }
          >
            Retry
          </Button>
        </div>
      )}
      {!journal.loading && visible.length === 0 && (
        <p className="py-4 text-center text-xs text-muted-foreground">
          No Tasks on this page match this filter.
        </p>
      )}
      <div className="divide-y divide-border">
        {visible.map((task) => (
          <TaskJournalItem
            key={task.id}
            entry={task}
            scope={scope}
            surface={surface}
          />
        ))}
      </div>
      <TaskPagination scope={scope} surface={surface} />
    </ResourcePanel>
  );
}
