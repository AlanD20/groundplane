import { Fragment, type ReactNode } from "react";
import { TableExpansion } from "@/components/common/table-expansion";
import { CompactReference } from "@/components/common/compact-reference";
import { Badge } from "@/components/ui/badge";
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
                      <TableSortHead sort={table} field="owner">
                        Owner
                      </TableSortHead>
                      <TableHead>Installed software</TableHead>
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
                          <TableCell className="max-w-64 break-words">
                            <div>{owner(runner).name}</div>
                            <div className="mt-1 text-xs text-muted-foreground">
                              {owner(runner).scope}
                            </div>
                          </TableCell>
                          <TableCell>
                            <ContainerSoftwareIdentity
                              software={runner.software}
                              showReferences={false}
                              unavailable="Identity not reported"
                            />
                          </TableCell>
                          <TableCell>
                            <div className="flex flex-wrap gap-2">
                              {actions(runner)}
                            </div>
                          </TableCell>
                        </ResourceRow>
                        <TableExpansion columns={5}>
                          {expandedId === runner.id ? (
                            editor
                          ) : inspectedId === runner.id ? (
                            <div
                              id={`runner-details-${runner.id}`}
                              className="w-[calc(100cqw-2rem)] min-w-0 space-y-4 border-t border-border pt-4"
                            >
                              <div className="flex flex-wrap items-center justify-between gap-3">
                                <CompactReference
                                  value={runner.id}
                                  label="Runner ID"
                                />
                                <div className="flex flex-wrap gap-3 text-xs">
                                  <TaskLink taskId={runner.createTaskId}>
                                    Registration task
                                  </TaskLink>
                                  {runner.removeTaskId && (
                                    <TaskLink taskId={runner.removeTaskId}>
                                      Removal task
                                    </TaskLink>
                                  )}
                                </div>
                              </div>
                              <div className="grid items-start gap-4 @3xl:grid-cols-2">
                                <ResourcePanel title="GitHub registration">
                                  <div className="min-w-0 space-y-1">
                                    <span className="text-xs text-muted-foreground">
                                      Repository or organization
                                    </span>
                                    <p className="break-all text-sm">
                                      {runner.githubUrl}
                                    </p>
                                  </div>
                                  <DetailRow
                                    label="Runner name"
                                    value={runner.name || "Not reported"}
                                  />
                                  <div className="space-y-2">
                                    <span className="text-xs text-muted-foreground">
                                      Workflow labels
                                    </span>
                                    <div className="flex flex-wrap gap-2">
                                      {runner.labels.length ? (
                                        runner.labels.map((label) => (
                                          <Badge
                                            key={label}
                                            variant="secondary"
                                            className="max-w-full whitespace-normal break-all font-mono"
                                          >
                                            {label}
                                          </Badge>
                                        ))
                                      ) : (
                                        <span className="text-xs text-muted-foreground">
                                          None
                                        </span>
                                      )}
                                    </div>
                                  </div>
                                </ResourcePanel>
                                <ResourcePanel title="Runtime">
                                  <ContainerSoftwareIdentity
                                    software={runner.software}
                                    unavailable="Software identity not reported"
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
                                </ResourcePanel>
                              </div>
                              <RunnerCheckPanel
                                runnerId={runner.id}
                                open
                                disabled={runner.lifecycle !== "ready"}
                              />
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
