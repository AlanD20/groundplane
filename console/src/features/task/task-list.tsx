import { HelpHint, ResourcePanel } from "@/components/common/resource-panel";
import { TaskJournalItem } from "@/components/common/task-journal-item";
import { ResourceTable } from "@/components/common/resource-table";
import {
  Table,
  TableBody,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Select } from "@/components/ui/select";
import { useStore } from "@/lib/store";
import type {
  TaskJournalScope,
  TaskJournalSurface,
  TaskStatus,
  TaskType,
} from "@/lib/types";
import { RefreshCw } from "lucide-react";
import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useSearchParams } from "react-router-dom";
import { TaskPagination } from "./task-pagination";

const filters = [
  { value: "all", label: "All statuses" },
  { value: "running", label: "Running" },
  { value: "pending", label: "Queued" },
  { value: "completed", label: "Completed" },
  { value: "failed", label: "Failed" },
  { value: "timed_out", label: "Timed out" },
  { value: "aborted", label: "Aborted" },
];

export function TaskList({
  scope: ownerScope,
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
  const [search] = useSearchParams();
  const [filter, setFilter] = useState(search.get("status") ?? "all");
  const [resource, setResource] = useState(
    search.get("resource_kind") ?? "all",
  );
  const [operation, setOperation] = useState(search.get("type") ?? "all");
  const scope = useMemo(
    () => ({
      ...ownerScope,
      status: filter === "all" ? undefined : (filter as TaskStatus),
      resourceKind: resource === "all" ? undefined : resource,
      taskType: operation === "all" ? undefined : (operation as TaskType),
    }),
    [ownerScope, filter, resource, operation],
  );
  const journal = store.getTaskJournal(scope);
  const visible = journal.entries;
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
            Newest first. Filters search the whole recorded history.
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
          <Select
            aria-label="Task resource"
            className="w-40"
            value={resource}
            onValueChange={setResource}
            options={[
              { value: "all", label: "All resources" },
              ...[
                "component",
                "service",
                "image",
                "agent",
                "controller",
                "etcd",
                "runner",
                "route",
                "entry",
                "secret",
                "volume",
                "backing_zone",
                "connector",
                "script",
                "release_group",
                "hierarchy_deletion",
              ].map((value) => ({
                value,
                label: value
                  .replaceAll("_", " ")
                  .replace(/^./, (letter) => letter.toUpperCase()),
              })),
            ]}
          />
          <Select
            aria-label="Task operation"
            className="w-40"
            value={operation}
            onValueChange={setOperation}
            options={[
              { value: "all", label: "All operations" },
              ...[
                "deploy",
                "rollback",
                "backup",
                "backup_prune",
                "restore",
                "attach",
                "detach",
                "run",
                "script",
                "provision",
                "create",
                "update",
                "remove",
                "start",
                "stop",
                "destroy",
                "rotate",
                "fetch",
              ].map((value) => ({
                value,
                label:
                  value === "destroy"
                    ? "Remove containers"
                    : value
                        .replaceAll("_", " ")
                        .replace(/^./, (letter) => letter.toUpperCase()),
              })),
            ]}
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
          No recorded Tasks match these filters.
        </p>
      )}
      <ResourceTable>
        <Table aria-label={title}>
          <TableHeader>
            <TableRow>
              <TableHead>Task</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Resource</TableHead>
              <TableHead>Scope</TableHead>
              <TableHead>When · newest first</TableHead>
              <TableHead>
                <span className="sr-only">Actions</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {visible.map((task) => (
              <TaskJournalItem
                key={task.id}
                entry={task}
                scope={scope}
                surface={surface}
                tableRow
              />
            ))}
          </TableBody>
        </Table>
      </ResourceTable>
      <TaskPagination scope={scope} surface={surface} />
    </ResourcePanel>
  );
}
