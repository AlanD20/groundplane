import { ImageReference } from "@/components/common/image-reference";
import { ResourcePanel } from "@/components/common/resource-panel";
import {
  TablePagination,
  useTableView,
} from "@/components/common/table-controls";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Boxes, ExternalLink, Terminal } from "lucide-react";
import type { ServiceObservation } from "./service-observation";

export function ServiceContainers({
  observation,
  onOpenLogs,
}: {
  observation: ServiceObservation;
  onOpenLogs?: () => void;
}) {
  const table = useTableView(
    observation.state === "unavailable" ? [] : observation.containers,
    {
      name: (item) => item.name,
      state: (item) => item.state,
      health: (item) => item.health,
      replica: (item) => item.replica,
    },
    "replica",
    "asc",
  );
  return (
    <ResourcePanel
      title="Containers"
      actions={
        onOpenLogs ? (
          <Button variant="link" size="sm" onClick={onOpenLogs}>
            Open logs <ExternalLink className="size-3.5" />
          </Button>
        ) : undefined
      }
    >
      {observation.state === "unavailable" ? (
        <p role="status" className="text-sm text-muted-foreground">
          Container status is unavailable. Waiting for a current Agent report.
        </p>
      ) : observation.containers.length === 0 ? (
        <p role="status" className="text-sm text-muted-foreground">
          No workload containers are present.
        </p>
      ) : (
        <div className="space-y-3">
          <div className="divide-y divide-border rounded-xl border border-border bg-card">
            {table.rows.map((item) => {
              const content = (
                <>
                  <span className="flex size-9 shrink-0 items-center justify-center rounded-lg bg-accent text-primary">
                    <Boxes className="size-4" />
                  </span>
                  <span className="min-w-0 flex-1 text-left">
                    <span
                      className="block truncate text-sm font-medium"
                      title={item.name}
                    >
                      Replica {item.replica}
                    </span>
                    <span className="mt-1 block min-w-0 text-xs text-muted-foreground">
                      <ImageReference value={item.image} />
                    </span>
                    <span
                      className="mt-1 block truncate text-xs text-muted-foreground"
                      title={`${item.name} · ${item.id}`}
                    >
                      {item.name}
                    </span>
                  </span>
                  <span className="flex flex-wrap items-center justify-end gap-2">
                    <Badge
                      variant={
                        item.state === "running"
                          ? "primary"
                          : item.state === "dead"
                            ? "danger"
                            : "muted"
                      }
                    >
                      {item.state}
                    </Badge>
                    <Badge
                      variant={
                        item.health === "healthy"
                          ? "success"
                          : item.health === "unhealthy"
                            ? "danger"
                            : item.health === "starting"
                              ? "warning"
                              : "muted"
                      }
                    >
                      {item.health === "none" ? "No healthcheck" : item.health}
                    </Badge>
                    {onOpenLogs && (
                      <Terminal className="size-4 text-muted-foreground" />
                    )}
                  </span>
                </>
              );
              return onOpenLogs ? (
                <Button
                  key={item.id}
                  variant="ghost"
                  size="content"
                  className="grid w-full grid-cols-[auto_minmax(0,1fr)_auto] items-center gap-3 rounded-none p-4 first:rounded-t-xl last:rounded-b-xl"
                  aria-label={`Open logs for ${item.name}`}
                  onClick={onOpenLogs}
                >
                  {content}
                </Button>
              ) : (
                <div
                  key={item.id}
                  className="grid grid-cols-[auto_minmax(0,1fr)_auto] items-center gap-3 p-4"
                >
                  {content}
                </div>
              );
            })}
          </div>
          <TablePagination table={table} />
        </div>
      )}
    </ResourcePanel>
  );
}
