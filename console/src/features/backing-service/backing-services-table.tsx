import { StatusBadge } from "@/components/common/status-badge";
import {
  TablePagination,
  TableSortHead,
  useTableView,
} from "@/components/common/table-controls";
import { Input } from "@/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { serviceObservationState } from "@/features/service/service-observation";
import { useStore } from "@/lib/store";
import { valkeyAuthenticationDetails } from "@/lib/valkey-authentication";
import { useState } from "react";
import { Link } from "react-router-dom";

export function BackingServicesTable({ now }: { now: number }) {
  const store = useStore();
  const [query, setQuery] = useState("");
  const rows = store.backingProjects.filter((project) =>
    `${project.name} ${project.slug} ${project.environments?.[0]?.services[0]?.adapter}`
      .toLowerCase()
      .includes(query.trim().toLowerCase()),
  );
  const table = useTableView(
    rows,
    {
      name: (project) => project.name,
      consumers: (project) => project.consumers?.length ?? 0,
      adapter: (project) =>
        project.environments?.[0]?.services[0]?.adapter ?? "",
    },
    "name",
    "asc",
    query,
  );
  return (
    <section className="space-y-4" aria-label="Backing services">
      <Input
        type="search"
        aria-label="Search backing services"
        placeholder="Search by name or adapter…"
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        className="max-w-sm"
      />
      <div className="overflow-x-auto rounded-lg border border-border bg-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableSortHead sort={table} field="name">
                Backing Service
              </TableSortHead>
              <TableHead>Runtime</TableHead>
              <TableSortHead sort={table} field="adapter">
                Adapter & endpoint
              </TableSortHead>
              <TableSortHead sort={table} field="consumers">
                Usage
              </TableSortHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {table.rows.map((project) => {
              const env = project.environments?.[0],
                service = env?.services[0];
              const adapter = store.adapters.find(
                (adapter) => adapter.key === service?.adapter,
              );
              const port =
                adapter?.urlScheme === "redis"
                  ? 6379
                  : adapter?.urlScheme === "pgsql"
                    ? 5432
                    : undefined;
              const backups = store.tenantProjects.reduce(
                (count, owner) =>
                  count +
                  (owner.environments ?? []).reduce(
                    (total, environment) =>
                      total +
                      (environment.backup?.sources ?? []).filter(
                        (source) =>
                          source.kind === "attach" &&
                          environment.attaches.some(
                            (attach) =>
                              attach.id === source.ref &&
                              attach.projectId === project.id,
                          ),
                      ).length,
                    0,
                  ),
                0,
              );
              return (
                <TableRow key={project.id}>
                  <TableCell className="min-w-44">
                    <Link
                      to={`/platform/backing-services/${project.id}`}
                      className="text-sm font-medium text-foreground hover:text-primary"
                    >
                      {project.name}
                    </Link>
                    <p className="mt-1 max-w-72 truncate text-xs text-muted-foreground">
                      {project.description}
                    </p>
                    {env && (
                      <p className="mt-1 text-[11px] text-muted-foreground">
                        Provisioning {env.provisioningState}
                      </p>
                    )}
                  </TableCell>
                  <TableCell>
                    <StatusBadge
                      status={
                        service
                          ? serviceObservationState(service.observation, now)
                          : "unavailable"
                      }
                    />
                    {service && (
                      <p className="mt-1 text-[11px] text-muted-foreground">
                        {service.resources.mem} memory limit
                      </p>
                    )}
                  </TableCell>
                  <TableCell>
                    <p>{service?.adapter ?? "Unavailable"}</p>
                    <p className="mt-1 text-xs text-muted-foreground">
                      {service?.serviceName}
                      {port ? `:${port}` : ""}
                    </p>
                    {valkeyAuthenticationDetails(service?.authentication) && (
                      <p className="mt-1 text-[11px] text-muted-foreground">
                        {
                          valkeyAuthenticationDetails(service?.authentication)
                            ?.label
                        }
                      </p>
                    )}
                  </TableCell>
                  <TableCell>
                    <p>{project.consumers?.length ?? 0} consumers</p>
                    <p className="mt-1 text-[11px] text-muted-foreground">
                      {backups} backup sources
                    </p>
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
        {!rows.length && (
          <p className="p-8 text-center text-xs text-muted-foreground">
            No backing services match your search.
          </p>
        )}
      </div>
      <TablePagination table={table} label="Backing services" />
    </section>
  );
}
