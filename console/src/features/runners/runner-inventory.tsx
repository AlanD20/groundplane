import { Fragment, type ReactNode } from "react";
import { TableExpansion } from "@/components/common/table-expansion";
import { DetailRow } from "@/components/common/detail-row";
import {
  ResourcePanel,
  SummaryItem,
  SummaryStrip,
} from "@/components/common/resource-panel";
import { ResourceRow, ResourceTable } from "@/components/common/resource-table";
import { StatusBadge } from "@/components/common/status-badge";
import {
  CollectionToolbar,
  TablePagination,
  TableSortHead,
  useTableView,
} from "@/components/common/table-controls";
import { TaskLink } from "@/components/common/task-link";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { formatTimestamp } from "@/lib/format-timestamp";
import type { Runner } from "@/lib/types";
import {
  ChevronRight,
  GitBranch,
  Pencil,
  RotateCcw,
  Trash2,
} from "lucide-react";
import { useState } from "react";
import { ContainerSoftwareIdentity } from "@/features/image-delivery/software-identity";
import { RunnerCheckPanel } from "./runner-check-panel";

function runnerStatus(runner: Runner) {
  if (runner.lifecycle !== "ready") return runner.lifecycle;
  return runner.online ? "Online" : "Offline";
}

export function RunnerInventory({
  expandedId,
  editor,
  runners,
  tenantCount,
  loading,
  error,
  owner,
  onRename,
  onRetry,
  onRemove,
}: {
  expandedId?: string;
  editor?: ReactNode;
  runners: Runner[];
  tenantCount: number;
  loading: boolean;
  error: string | null;
  owner: (runner: Runner) => { scope: string; name: string };
  onRename: (runner: Runner) => void;
  onRetry: (runner: Runner) => void;
  onRemove: (runner: Runner) => void;
}) {
  const [query, setQuery] = useState("");
  const [inspectedId, setInspectedId] = useState<string | null>(null);
  const toggleDetails = (id: string) => {
    if (expandedId === id) return;
    setInspectedId((current) => (current === id ? null : id));
  };
  const table = useTableView(
    runners.filter((runner) => {
      const ownership = owner(runner);
      return `${runner.slug} ${runner.githubUrl} ${ownership.scope} ${ownership.name} ${runner.labels.join(" ")}`
        .toLowerCase()
        .includes(query.trim().toLowerCase());
    }),
    {
      name: (runner) => runner.slug,
      status: runnerStatus,
      scope: (runner) => owner(runner).scope,
      owner: (runner) => owner(runner).name,
    },
    "name",
    "asc",
    query,
  );
  const unavailable = loading || !!error;
  const actions = (runner: Runner) => (
    <>
      <Button
        variant="outline"
        size="sm"
        disabled={
          !!expandedId ||
          runner.lifecycle === "provisioning" ||
          runner.lifecycle === "deleting"
        }
        onClick={() => {
          setInspectedId(null);
          onRename(runner);
        }}
        aria-label={`Rename ${runner.slug}`}
      >
        <Pencil className="size-3.5" /> Rename
      </Button>
      {runner.lifecycle === "failed" && (
        <Button
          variant="outline"
          size="sm"
          onClick={() => {
            setInspectedId(null);
            onRetry(runner);
          }}
          disabled={!!expandedId}
          aria-label={`Retry ${runner.slug} registration`}
        >
          <RotateCcw className="size-3.5" /> Retry registration
        </Button>
      )}
      <Button
        variant="outline"
        size="sm"
        disabled={runner.lifecycle === "provisioning"}
        onClick={() => {
          setInspectedId(null);
          onRemove(runner);
        }}
        aria-label={`Remove ${runner.slug}`}
        className="text-destructive"
      >
        <Trash2 className="size-3.5" /> Remove
      </Button>
    </>
  );
  const status = (runner: Runner) => (
    <StatusBadge
      status={
        runner.lifecycle === "ready"
          ? runner.online
            ? "online"
            : "offline"
          : runner.lifecycle === "failed"
            ? "failed"
            : "pending"
      }
      label={runnerStatus(runner).replace(/^./, (letter) =>
        letter.toUpperCase(),
      )}
    />
  );
  return (
    <>
      <SummaryStrip>
        <SummaryItem label="Runners in this view">
          {unavailable ? "Not available" : runners.length}
        </SummaryItem>
        <SummaryItem label="Online">
          {unavailable
            ? "Not available"
            : runners.filter(
                (runner) => runner.lifecycle === "ready" && runner.online,
              ).length}
        </SummaryItem>
        <SummaryItem label="Failed">
          {unavailable
            ? "Not available"
            : runners.filter((runner) => runner.lifecycle === "failed").length}
        </SummaryItem>
        <SummaryItem label="Tenant capacity">
          {unavailable ? "Not available" : `${tenantCount} / 5`}
        </SummaryItem>
      </SummaryStrip>
      <ResourcePanel title="Runners">
        {error ? (
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
        ) : loading ? (
          <p role="status" className="text-sm text-muted-foreground">
            Loading Runners…
          </p>
        ) : (
          <>
            <CollectionToolbar
              label="Runners"
              query={query}
              onQueryChange={setQuery}
            />
            <div className="@container">
              <ResourceTable>
                <Table aria-label="GitHub Runners">
                  <TableHeader>
                    <TableRow>
                      <TableSortHead sort={table} field="name">
                        Runner
                      </TableSortHead>
                      <TableSortHead sort={table} field="status">
                        Status
                      </TableSortHead>
                      <TableSortHead sort={table} field="scope">
                        Scope
                      </TableSortHead>
                      <TableSortHead sort={table} field="owner">
                        Owner
                      </TableSortHead>
                      <TableHead>Installed software</TableHead>
                      <TableHead>Current job</TableHead>
                      <TableHead>Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {[
                      ...runners.filter(
                        (r) =>
                          r.id === expandedId &&
                          !table.rows.some((row) => row.id === r.id),
                      ),
                      ...table.rows,
                    ].map((runner) => (
                      <Fragment key={runner.id}>
                        <ResourceRow
                          key={runner.id}
                          onOpen={() => toggleDetails(runner.id)}
                        >
                          <TableCell>
                            <Button
                              variant="ghost"
                              size="content"
                              className="max-w-64 justify-start gap-2 p-0 text-xs"
                              onClick={() => toggleDetails(runner.id)}
                              disabled={expandedId === runner.id}
                              aria-expanded={inspectedId === runner.id}
                              aria-controls={`runner-details-${runner.id}`}
                            >
                              <ChevronRight
                                aria-hidden
                                className={`size-3.5 shrink-0 transition-transform motion-reduce:transition-none ${inspectedId === runner.id ? "rotate-90" : ""}`}
                              />
                              <GitBranch className="size-3.5 shrink-0 text-primary" />
                              <span className="truncate">{runner.slug}</span>
                            </Button>
                          </TableCell>
                          <TableCell>{status(runner)}</TableCell>
                          <TableCell>{owner(runner).scope}</TableCell>
                          <TableCell className="max-w-64 break-words">
                            {owner(runner).name}
                          </TableCell>
                          <TableCell>
                            <ContainerSoftwareIdentity
                              software={runner.software}
                              showReferences={false}
                              unavailable="Identity not reported"
                            />
                          </TableCell>
                          <TableCell className="text-muted-foreground">
                            Not reported
                          </TableCell>
                          <TableCell>
                            <div className="flex flex-wrap gap-2">
                              {actions(runner)}
                            </div>
                          </TableCell>
                        </ResourceRow>
                        <TableExpansion columns={7}>
                          {expandedId === runner.id ? (
                            editor
                          ) : inspectedId === runner.id ? (
                            <div
                              id={`runner-details-${runner.id}`}
                              className="w-[calc(100cqw-2rem)] min-w-0 space-y-4 border-t border-border pt-4"
                            >
                              <div className="grid gap-x-8 gap-y-2 md:grid-cols-2">
                                <DetailRow
                                  label="Status"
                                  value={status(runner)}
                                />
                                <DetailRow
                                  label="Owner"
                                  value={owner(runner).name}
                                />
                                <DetailRow
                                  label="GitHub name"
                                  value={runner.name}
                                />
                                <DetailRow
                                  label="Labels"
                                  value={runner.labels.join(", ") || "None"}
                                />
                                <DetailRow
                                  label="Current job"
                                  value="Not reported by the Controller"
                                />
                                <DetailRow
                                  label="Installed software"
                                  value={
                                    <ContainerSoftwareIdentity
                                      software={runner.software}
                                      unavailable="Identity not reported by the Controller"
                                    />
                                  }
                                />
                                <DetailRow
                                  label="Last report"
                                  value={formatTimestamp(
                                    runner.observedAt ?? "",
                                    "Not reported",
                                  )}
                                />
                                <DetailRow
                                  label="Created"
                                  value={formatTimestamp(
                                    runner.createdAt,
                                    "Not reported",
                                  )}
                                />
                                <DetailRow
                                  label="Registration Task"
                                  value={
                                    <TaskLink taskId={runner.createTaskId}>
                                      Inspect Task
                                    </TaskLink>
                                  }
                                />
                                {runner.removeTaskId && (
                                  <DetailRow
                                    label="Removal Task"
                                    value={
                                      <TaskLink taskId={runner.removeTaskId}>
                                        Inspect Task
                                      </TaskLink>
                                    }
                                  />
                                )}
                                <DetailRow
                                  label="Runner ID"
                                  value={runner.id}
                                />
                              </div>
                              <DetailRow
                                label="GitHub URL"
                                value={runner.githubUrl}
                              />
                              <RunnerCheckPanel
                                runnerId={runner.id}
                                open
                                disabled={runner.lifecycle !== "ready"}
                              />
                              <p className="text-xs text-muted-foreground">
                                Trusted workflows use normal GP CLI/API access.
                                Builds use a dedicated rootless Docker daemon,
                                not the host socket.
                              </p>
                            </div>
                          ) : null}
                        </TableExpansion>
                      </Fragment>
                    ))}
                  </TableBody>
                </Table>
              </ResourceTable>
            </div>
            {!table.total && (
              <p
                role="status"
                className="py-4 text-center text-xs text-muted-foreground"
              >
                {runners.length
                  ? "No Runners match your search."
                  : "No Runners yet."}
              </p>
            )}
            <TablePagination table={table} label="Runners" />
          </>
        )}
      </ResourcePanel>
    </>
  );
}
