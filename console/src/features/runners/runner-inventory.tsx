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
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Drawer, DrawerContent } from "@/components/ui/drawer";
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
import { ArrowRight, GitBranch, Pencil, RotateCcw, Trash2 } from "lucide-react";
import { useState } from "react";

function runnerStatus(runner: Runner) {
  if (runner.lifecycle !== "ready") return runner.lifecycle;
  return runner.online ? "Online" : "Offline";
}

export function RunnerInventory({
  runners,
  loading,
  error,
  owner,
  onRename,
  onRetry,
  onRemove,
}: {
  runners: Runner[];
  loading: boolean;
  error: string | null;
  owner: (runner: Runner) => { scope: string; name: string };
  onRename: (runner: Runner) => void;
  onRetry: (runner: Runner) => void;
  onRemove: (runner: Runner) => void;
}) {
  const [query, setQuery] = useState("");
  const [inspectedId, setInspectedId] = useState<string | null>(null);
  const inspected = runners.find((runner) => runner.id === inspectedId);
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
          runner.lifecycle === "provisioning" || runner.lifecycle === "deleting"
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
        <SummaryItem label="Runners">
          {unavailable ? "Not available" : `${runners.length} / 5`}
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
        <SummaryItem label="Build pipeline">
          <span className="text-sm">Build → push → fetch → deploy</span>
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
                    <TableHead>Current job</TableHead>
                    <TableHead>Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {table.rows.map((runner) => (
                    <ResourceRow
                      key={runner.id}
                      onOpen={() => setInspectedId(runner.id)}
                    >
                      <TableCell>
                        <Button
                          variant="ghost"
                          size="content"
                          className="max-w-64 justify-start gap-2 p-0 text-[11px]"
                          onClick={() => setInspectedId(runner.id)}
                        >
                          <GitBranch className="size-3.5 shrink-0 text-primary" />
                          <span className="truncate">{runner.slug}</span>
                        </Button>
                      </TableCell>
                      <TableCell>{status(runner)}</TableCell>
                      <TableCell>{owner(runner).scope}</TableCell>
                      <TableCell className="max-w-64 break-words">
                        {owner(runner).name}
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
                  ))}
                </TableBody>
              </Table>
            </ResourceTable>
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
      <ResourcePanel title="Build and delivery">
        <div className="flex flex-wrap items-center gap-2">
          {[
            "Rootless Docker build",
            "GP TLS registry push",
            "Agent image fetch",
            "Service Deploy",
          ].map((step, index) => (
            <span key={step} className="inline-flex items-center gap-2">
              {index > 0 && (
                <ArrowRight className="size-3.5 text-muted-foreground" />
              )}
              <Badge variant="muted">{step}</Badge>
            </span>
          ))}
        </div>
      </ResourcePanel>
      <Drawer
        open={!!inspected}
        onOpenChange={(open) => {
          if (!open) setInspectedId(null);
        }}
      >
        <DrawerContent>
          {inspected && (
            <>
              <DialogHeader>
                <DialogTitle>{inspected.slug}</DialogTitle>
                <DialogDescription>
                  GitHub Runner · {owner(inspected).scope}
                </DialogDescription>
              </DialogHeader>
              <div className="space-y-4">
                <DetailRow label="Status" value={status(inspected)} />
                <DetailRow label="GitHub URL" value={inspected.githubUrl} />
                <DetailRow label="Owner" value={owner(inspected).name} />
                <DetailRow label="GitHub name" value={inspected.name} />
                <DetailRow
                  label="Labels"
                  value={inspected.labels.join(", ") || "None"}
                />
                <DetailRow
                  label="Current job"
                  value="Not reported by the Controller"
                />
                <DetailRow
                  label="Last report"
                  value={formatTimestamp(
                    inspected.observedAt ?? "",
                    "Not reported",
                  )}
                />
                <DetailRow
                  label="Created"
                  value={formatTimestamp(inspected.createdAt, "Not reported")}
                />
                <DetailRow
                  label="Registration Task"
                  value={
                    <TaskLink taskId={inspected.createTaskId}>
                      Inspect Task
                    </TaskLink>
                  }
                />
                {inspected.removeTaskId && (
                  <DetailRow
                    label="Removal Task"
                    value={
                      <TaskLink taskId={inspected.removeTaskId}>
                        Inspect Task
                      </TaskLink>
                    }
                  />
                )}
                <DetailRow label="Runner ID" value={inspected.id} />
                <p className="text-xs text-muted-foreground">
                  Trusted workflows use normal GP CLI/API access. Builds use a
                  dedicated rootless Docker daemon, not the host socket.
                </p>
                <div className="flex flex-wrap gap-2">{actions(inspected)}</div>
              </div>
            </>
          )}
        </DrawerContent>
      </Drawer>
    </>
  );
}
