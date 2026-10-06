import { Fragment, useState } from "react";
import {
  ChevronDown,
  ChevronRight,
  Database,
  FileText,
  HardDrive,
  Trash2,
} from "lucide-react";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  TablePagination,
  TableSortHead,
  useTableView,
} from "@/components/common/table-controls";
import { TaskLink } from "@/components/common/task-link";
import { AdvancedDetails } from "@/components/common/resource-panel";
import { useStore } from "@/lib/store";
import type { Environment } from "@/lib/types";
import type { RecoveryPoint, RecoveryPointState } from "./types";
import { backupSourceLabel } from "./environment-backup-projection";
import {
  groupRecoveryPoints,
  type RecoveryPointGroup,
} from "./recovery-point-groups";
import { RecoveryGroupDeleteDialog } from "./recovery-group-delete-dialog";

function archiveSize(bytes: number) {
  const unit =
    bytes >= 1024 ** 3 ? 3 : bytes >= 1024 ** 2 ? 2 : bytes >= 1024 ? 1 : 0;
  return `${(bytes / 1024 ** unit).toLocaleString(undefined, { maximumFractionDigits: 1 })} ${["B", "KiB", "MiB", "GiB"][unit]}`;
}

export function RecoveryGroupsTable({
  env,
  points,
  onRestore,
  onDelete,
}: {
  env: Environment;
  points: RecoveryPointState;
  onRestore: (point: RecoveryPoint) => void;
  onDelete: (point: RecoveryPoint) => void;
}) {
  const store = useStore();
  const [expanded, setExpanded] = useState(new Set<string>());
  const [deleting, setDeleting] = useState<RecoveryPointGroup | null>(null);
  const table = useTableView(
    groupRecoveryPoints(points.items, points.nextCursor),
    {
      created: (group) => Date.parse(group.createdAt),
      size: (group) => group.sizeBytes,
      sources: (group) => group.points.length,
    },
    "created",
    "desc",
    env.id,
  );
  const toggle = (id: string) =>
    setExpanded((prior) => {
      const next = new Set(prior);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  return (
    <>
      <div className="overflow-x-auto rounded-lg border border-border">
        <Table className="min-w-[600px] text-sm">
          <TableHeader>
            <TableRow>
              <TableSortHead sort={table} field="created">
                Backup
              </TableSortHead>
              <TableHead>Destination</TableHead>
              <TableSortHead sort={table} field="sources">
                Sources
              </TableSortHead>
              <TableSortHead sort={table} field="size">
                Size
              </TableSortHead>
              <TableHead>
                <span className="sr-only">Actions</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {table.rows.map((group) => {
              const open = expanded.has(`${env.id}/${group.id}`);
              const first = group.points[0];
              const label = group.capture
                ? new Date(group.createdAt).toLocaleString()
                : "Earlier recovery points";
              return (
                <Fragment key={group.id}>
                  <TableRow
                    className="cursor-pointer"
                    onClick={() => toggle(`${env.id}/${group.id}`)}
                  >
                    <TableCell>
                      <Button
                        variant="ghost"
                        size="sm"
                        aria-expanded={open}
                        aria-controls={`backup-${group.id}`}
                        onClick={(event) => {
                          event.stopPropagation();
                          toggle(`${env.id}/${group.id}`);
                        }}
                      >
                        {open ? (
                          <ChevronDown className="size-4" />
                        ) : (
                          <ChevronRight className="size-4" />
                        )}
                        {label}
                      </Button>
                    </TableCell>
                    <TableCell
                      className="max-w-48 truncate"
                      title={first.connectorBucket}
                    >
                      {group.capture
                        ? (store.connectors.find(
                            (connector) => connector.id === first.connectorId,
                          )?.name ?? first.connectorBucket)
                        : "Various"}
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline">
                        {group.points.length}
                        {group.capture
                          ? ` / ${group.capture.sourceCount}`
                          : ""}{" "}
                        available
                      </Badge>
                      {!group.fullyLoaded && (
                        <span className="ml-2 text-xs text-muted-foreground">
                          More to load
                        </span>
                      )}
                    </TableCell>
                    <TableCell className="whitespace-nowrap">
                      {archiveSize(group.sizeBytes)}
                    </TableCell>
                    <TableCell className="text-right">
                      {group.capture && (
                        <Button
                          variant="ghost"
                          size="sm"
                          disabled={
                            !group.fullyLoaded ||
                            points.loading ||
                            points.loadingMore ||
                            !!points.loadError
                          }
                          onClick={(event) => {
                            event.stopPropagation();
                            setDeleting(group);
                          }}
                        >
                          <Trash2 className="size-3.5" />
                          Delete backup
                        </Button>
                      )}
                    </TableCell>
                  </TableRow>
                  {open && (
                    <TableRow id={`backup-${group.id}`}>
                      <TableCell colSpan={5} className="bg-surface/60 p-4">
                        <div className="mb-3 flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
                          {group.capture ? (
                            <TaskLink taskId={group.capture.taskId}>
                              View Backup Task
                            </TaskLink>
                          ) : (
                            <span>
                              These Points have no recorded run identity.
                            </span>
                          )}
                          <span>
                            Retention applies separately to each source.
                          </span>
                        </div>
                        <div className="divide-y divide-border rounded-lg border border-border bg-background">
                          {group.points.map((point) => {
                            const Icon =
                              point.sourceKind === "attach"
                                ? Database
                                : point.sourceKind === "volume"
                                  ? HardDrive
                                  : FileText;
                            const label = backupSourceLabel(store, env, {
                              id: point.sourceId,
                              kind: point.sourceKind,
                              targetId: point.targetId,
                            });
                            return (
                              <div key={point.id} className="p-3">
                                <div className="flex flex-wrap items-center justify-between gap-3">
                                  <div className="flex min-w-0 items-center gap-3">
                                    <Icon className="size-4 shrink-0 text-primary" />
                                    <div className="min-w-0">
                                      <p className="break-words font-medium">
                                        {label}
                                      </p>
                                      <p className="text-xs text-muted-foreground">
                                        {point.sourceKind === "attach"
                                          ? "PostgreSQL database"
                                          : point.sourceKind === "volume"
                                            ? "Persistent Volume"
                                            : "Environment configuration"}{" "}
                                        · {archiveSize(point.sizeBytes)} ·{" "}
                                        {point.encrypted
                                          ? `Encrypted · era ${point.keyEra}`
                                          : "Not encrypted"}
                                      </p>
                                    </div>
                                  </div>
                                  <div className="flex items-center gap-2">
                                    <Button
                                      variant="outline"
                                      size="sm"
                                      onClick={() => onRestore(point)}
                                    >
                                      Restore
                                    </Button>
                                    <Button
                                      variant="ghost"
                                      size="sm"
                                      onClick={() => onDelete(point)}
                                    >
                                      <Trash2 className="size-3.5" />
                                      Delete
                                    </Button>
                                  </div>
                                </div>
                                <AdvancedDetails title="Archive details">
                                  <dl className="grid gap-2 text-xs">
                                    <div>
                                      <dt className="text-muted-foreground">
                                        Recovery Point
                                      </dt>
                                      <dd className="break-all font-mono">
                                        {point.id}
                                      </dd>
                                    </div>
                                    <div>
                                      <dt className="text-muted-foreground">
                                        Captured
                                      </dt>
                                      <dd>
                                        {new Date(
                                          point.createdAt,
                                        ).toLocaleString()}
                                      </dd>
                                    </div>
                                    <div>
                                      <dt className="text-muted-foreground">
                                        Storage
                                      </dt>
                                      <dd className="break-all">
                                        s3://{point.connectorBucket}/
                                        {point.connectorPrefix}
                                      </dd>
                                    </div>
                                  </dl>
                                </AdvancedDetails>
                              </div>
                            );
                          })}
                        </div>
                        {group.capture &&
                          group.points.length < group.capture.sourceCount &&
                          group.fullyLoaded && (
                            <p className="mt-3 text-xs text-muted-foreground">
                              Some sources are unavailable: their Points were
                              not created, expired, or were deleted. Remaining
                              Points can still be restored individually.
                            </p>
                          )}
                        {!group.fullyLoaded && (
                          <p className="mt-3 text-xs text-muted-foreground">
                            Load more below to include the rest of this group
                            before deleting it.
                          </p>
                        )}
                      </TableCell>
                    </TableRow>
                  )}
                </Fragment>
              );
            })}
          </TableBody>
        </Table>
      </div>
      <div className="mt-3">
        <TablePagination table={table} label="Backups" />
      </div>
      {deleting && (
        <RecoveryGroupDeleteDialog
          key={`${env.id}/${deleting.id}`}
          env={env}
          group={deleting}
          onClose={() => setDeleting(null)}
        />
      )}
    </>
  );
}
