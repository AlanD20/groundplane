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
  TableRow,
} from "@/components/ui/table";
import { ArrowUpRight } from "lucide-react";
import { Link } from "react-router-dom";
import type { HostImage } from "./api";

export function ImageUsage({
  image,
  onClose,
}: {
  image: HostImage;
  onClose: () => void;
}) {
  const table = useTableView(
    image.container_uses,
    {
      service: (use) => use.owner?.service_name ?? "",
      environment: (use) => use.owner?.environment_name ?? "",
      name: (use) => use.name,
      state: (use) => use.state,
    },
    "service",
  );
  return (
    <section className="space-y-3">
      <div>
        <h3 className="text-sm font-medium">Containers using this image</h3>
        <p className="mt-1 text-xs text-muted-foreground">
          Includes stopped containers. Each one keeps this image in use.
        </p>
      </div>
      {table.total ? (
        <>
          <div className="overflow-x-auto rounded-lg border border-border">
            <Table className="min-w-[540px] text-xs">
              <TableHeader>
                <TableRow>
                  <TableSortHead sort={table} field="service">
                    Service
                  </TableSortHead>
                  <TableSortHead sort={table} field="environment">
                    Environment
                  </TableSortHead>
                  <TableSortHead sort={table} field="name">
                    Container
                  </TableSortHead>
                  <TableSortHead sort={table} field="state">
                    State
                  </TableSortHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {table.rows.map((use) => {
                  const owner = use.owner;
                  const environmentPath = owner
                    ? owner.backing
                      ? `/platform/backing-services/${encodeURIComponent(owner.project_id)}`
                      : `/t/${encodeURIComponent(owner.tenant_slug)}/${encodeURIComponent(owner.project_slug)}/${encodeURIComponent(owner.environment_name)}`
                    : null;
                  const servicePath =
                    environmentPath && owner?.service_id
                      ? owner.backing
                        ? environmentPath
                        : `${environmentPath}?view=services&service=${encodeURIComponent(owner.service_id)}`
                      : null;
                  return (
                    <TableRow key={use.id}>
                      <TableCell className="max-w-40 break-words">
                        {servicePath ? (
                          <Link
                            className="text-primary hover:underline"
                            onClick={onClose}
                            to={servicePath}
                          >
                            {owner?.service_name}
                            <ArrowUpRight className="ml-1 inline size-3" />
                          </Link>
                        ) : (
                          <span className="text-muted-foreground">
                            {use.managed
                              ? "GP-managed container"
                              : "Not managed by GP"}
                          </span>
                        )}
                      </TableCell>
                      <TableCell>
                        {environmentPath ? (
                          <Link
                            className="text-primary hover:underline"
                            onClick={onClose}
                            to={environmentPath}
                          >
                            {owner?.environment_name}
                            <ArrowUpRight className="ml-1 inline size-3" />
                            <span className="mt-1 block text-[11px] text-muted-foreground">
                              {owner?.backing
                                ? "Backing service"
                                : `${owner?.tenant_slug} / ${owner?.project_slug}`}
                            </span>
                          </Link>
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </TableCell>
                      <TableCell className="max-w-52 break-all" title={use.id}>
                        {use.name}
                      </TableCell>
                      <TableCell>
                        <Badge
                          variant={
                            use.state === "running" ? "success" : "muted"
                          }
                        >
                          {use.state === "exited" ? "Stopped" : use.state}
                        </Badge>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>
          <TablePagination table={table} label="Containers" />
        </>
      ) : (
        <p className="text-sm text-muted-foreground">
          No containers currently use this image.
        </p>
      )}
      {image.protection_reason && (
        <div className="space-y-1 rounded-lg border border-border bg-surface p-3">
          <h3 className="text-sm font-medium">Other removal protection</h3>
          <p className="break-words text-sm text-muted-foreground">
            {image.protection_reason}
          </p>
          <p className="text-xs text-muted-foreground">
            Removing containers alone does not release this protection.
          </p>
        </div>
      )}
    </section>
  );
}
