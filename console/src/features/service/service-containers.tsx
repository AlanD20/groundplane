import { ImageReference } from "@/components/common/image-reference";
import { ResourcePanel } from "@/components/common/resource-panel";
import { ResourceTable } from "@/components/common/resource-table";
import {
  TablePagination,
  TableSortHead,
  useTableView,
} from "@/components/common/table-controls";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableHead,
  TableRow,
} from "@/components/ui/table";
import type { ServiceObservation } from "./service-observation";

export function ServiceContainers({
  observation,
}: {
  observation: ServiceObservation;
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
    <ResourcePanel title="Containers">
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
          <ResourceTable>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableSortHead sort={table} field="name">
                    Container
                  </TableSortHead>
                  <TableSortHead sort={table} field="state">
                    State
                  </TableSortHead>
                  <TableSortHead sort={table} field="health">
                    Health
                  </TableSortHead>
                  <TableHead>Image</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {table.rows.map((item) => (
                  <TableRow key={item.id}>
                    <TableCell className="max-w-80">
                      <p className="truncate font-medium" title={item.name}>
                        {item.name}
                      </p>
                      <p
                        className="text-xs text-muted-foreground"
                        title={item.id}
                      >
                        {item.id.slice(0, 12)} · replica {item.replica}
                      </p>
                    </TableCell>
                    <TableCell>
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
                    </TableCell>
                    <TableCell>
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
                        {item.health === "none"
                          ? "No healthcheck"
                          : item.health}
                      </Badge>
                    </TableCell>
                    <TableCell className="max-w-72">
                      <ImageReference value={item.image} />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </ResourceTable>
          <TablePagination table={table} />
        </div>
      )}
    </ResourcePanel>
  );
}
