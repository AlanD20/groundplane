"use client";

import { useEffect, useMemo, useState } from "react";
import { Activity, RefreshCw } from "lucide-react";
import { useStore } from "@/lib/store";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { TaskJournalItem } from "@/components/common/task-journal-item";
import { TaskPagination } from "./task-pagination";
import type { ActivityEntry, Environment, TaskJournalScope } from "@/lib/types";

// Paginated Environment task history.
// Compact list — click a task to open the detail drawer with the full
// procedure and controls (abort, retry).
export function TasksCard({ env }: { env: Environment }) {
  const store = useStore();
  const [filter, setFilter] = useState("all");
  const scope = useMemo<TaskJournalScope>(
    () => ({ kind: "environment", environmentId: env.id }),
    [env.id],
  );
  const journal = store.getTaskJournal(scope);
  const tasks = journal.entries;
  const running = tasks.filter(
    (t) => t.status === "running" || t.status === "pending",
  ).length;
  useEffect(() => {
    void store.loadTaskJournal("tasks", scope).catch(() => undefined);
  }, [scope, store.loadTaskJournal]);

  const filters: {
    key: string;
    label: string;
    match: (t: ActivityEntry) => boolean;
  }[] = [
    { key: "all", label: "All", match: () => true },
    {
      key: "inflight",
      label: "In-flight",
      match: (t) => t.status === "running",
    },
    { key: "queued", label: "Queued", match: (t) => t.status === "pending" },
    {
      key: "completed",
      label: "Completed",
      match: (t) => t.status === "completed",
    },
    {
      key: "failed",
      label: "Failed",
      match: (t) =>
        t.status === "failed" ||
        t.status === "timed_out" ||
        t.status === "aborted",
    },
  ];
  const visible = tasks.filter(
    (t) => filters.find((f) => f.key === filter)?.match(t) ?? true,
  );

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between gap-3">
        <CardTitle className="flex items-center gap-2">
          <Activity className="size-4 text-muted-foreground" /> Tasks · this
          environment
        </CardTitle>
        <div className="flex items-center gap-2">
          <Badge variant={running > 0 ? "success" : "muted"}>
            {running > 0 ? (
              <>{running} in-flight on this page</>
            ) : (
              "No in-flight tasks on this page"
            )}
          </Badge>
          <Button
            variant="outline"
            size="sm"
            disabled={journal.loading || journal.loadingMore}
            onClick={() =>
              void store.loadTaskJournal("tasks", scope).catch(() => undefined)
            }
          >
            <RefreshCw className="size-4" /> Refresh
          </Button>
        </div>
      </CardHeader>
      <CardContent className="flex flex-col gap-2">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <p className="text-xs text-muted-foreground">
            Task history for this Environment. Filters apply to this page. Click
            a task to open its details and controls.
          </p>
          <div className="flex shrink-0 items-center gap-0.5 rounded-lg border border-border bg-surface p-0.5">
            {filters.map((f) => (
              <Button
                variant="ghost"
                size="content"
                key={f.key}
                type="button"
                onClick={() => setFilter(f.key)}
                className={
                  filter === f.key
                    ? "rounded-md bg-primary px-2 py-1 text-[11px] font-medium text-primary-foreground"
                    : "rounded-md px-2 py-1 text-[11px] font-medium text-muted-foreground transition-colors hover:text-foreground"
                }
              >
                {f.label}
              </Button>
            ))}
          </div>
        </div>
        {journal.loading && (
          <div role="status" className="text-xs text-muted-foreground">
            loading environment tasks…
          </div>
        )}
        {journal.loadError && (
          <div
            role="alert"
            className="flex items-center justify-between gap-2 text-xs text-destructive"
          >
            <span>{journal.loadError}</span>
            <Button
              variant="outline"
              size="sm"
              disabled={journal.loading || journal.loadingMore}
              onClick={() =>
                void store
                  .loadTaskJournal(
                    "tasks",
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
        {!journal.loading && visible.length === 0 ? (
          <div className="text-xs text-muted-foreground">
            No tasks on this page match this filter.
          </div>
        ) : (
          <div className="flex flex-col gap-1.5">
            {visible.map((task) => (
              <TaskJournalItem
                key={task.id}
                entry={task}
                scope={scope}
                surface="tasks"
              />
            ))}
          </div>
        )}
        <TaskPagination scope={scope} surface="tasks" />
      </CardContent>
    </Card>
  );
}
