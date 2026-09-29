import { ImageReference } from "@/components/common/image-reference";
import { ResourceActionMenu } from "@/components/common/resource-action-menu";
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
import { environmentRuntimeState } from "@/features/service/service-observation";
import { useVisibleServiceObservations } from "@/features/service/use-service-observation-refresh";
import { formatTimestamp } from "@/lib/format-timestamp";
import { useStore } from "@/lib/store";
import type { Environment, Project, Tenant } from "@/lib/types";
import { ExternalLink, Settings, Trash2 } from "lucide-react";
import { useState } from "react";
import { Link } from "react-router-dom";

export function EnvironmentsTable({
  tenant,
  project,
  onRemove,
}: {
  tenant: Tenant;
  project: Project;
  onRemove: (environment: Environment) => void;
}) {
  const store = useStore();
  const [query, setQuery] = useState("");
  const environments = project.environments ?? [];
  const observation = useVisibleServiceObservations({
    environmentIds: environments.map((env) => env.id),
    observations: environments.flatMap((env) =>
      env.services.map((service) => service.observation),
    ),
    refreshEnvironment: store.refreshEnvironmentServices,
  });
  const rows = environments.filter((env) =>
    env.name.toLowerCase().includes(query.trim().toLowerCase()),
  );
  const table = useTableView(
    rows,
    {
      name: (env) => env.name,
      services: (env) => env.services.length,
      deployed: (env) =>
        env.lastDeployAt ? Date.parse(env.lastDeployAt) || 0 : 0,
    },
    "name",
    "asc",
    `${project.id}/${query}`,
  );
  return (
    <section className="space-y-4" aria-label="Environments">
      <Input
        type="search"
        aria-label="Search Environments"
        placeholder="Search Environments…"
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        className="max-w-sm"
      />
      {observation.refreshError && (
        <p role="alert" className="text-xs text-destructive">
          Runtime refresh failed: {observation.refreshError}
        </p>
      )}
      <div className="overflow-x-auto rounded-lg border border-border bg-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableSortHead sort={table} field="name">
                Environment
              </TableSortHead>
              <TableHead>Runtime</TableHead>
              <TableSortHead sort={table} field="services">
                Resources
              </TableSortHead>
              <TableSortHead sort={table} field="deployed">
                Last deploy
              </TableSortHead>
              <TableHead>
                <span className="sr-only">Actions</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {table.rows.map((env) => {
              const path = `/t/${tenant.slug}/${project.slug}/${encodeURIComponent(env.name)}`;
              return (
                <TableRow key={env.id}>
                  <TableCell>
                    <Link
                      to={path}
                      className="text-sm font-medium hover:text-primary"
                    >
                      {env.name}
                    </Link>
                    <p className="mt-1 text-[11px] text-muted-foreground">
                      Provisioning {env.provisioningState}
                    </p>
                  </TableCell>
                  <TableCell>
                    <StatusBadge
                      status={environmentRuntimeState(
                        env.services,
                        observation.now,
                      )}
                    />
                  </TableCell>
                  <TableCell>
                    <p>{env.services.length} Services</p>
                    <p className="mt-1 text-[11px] text-muted-foreground">
                      {env.routes.length} Routes, {env.zones.length} Zones,{" "}
                      {env.attaches.length} connections
                    </p>
                  </TableCell>
                  <TableCell className="max-w-60">
                    <ImageReference value={env.release} />
                    <p className="mt-1 text-[11px] text-muted-foreground">
                      {formatTimestamp(env.lastDeployAt, "Never deployed")}
                    </p>
                  </TableCell>
                  <TableCell className="w-12">
                    <ResourceActionMenu
                      label={`Actions for ${env.name}`}
                      actions={[
                        {
                          label: "Open Environment",
                          icon: <ExternalLink />,
                          href: path,
                        },
                        {
                          label: "Environment settings",
                          icon: <Settings />,
                          href: `${path}?view=settings`,
                        },
                        {
                          label: "Delete Environment",
                          icon: <Trash2 />,
                          destructive: true,
                          disabled: env.deletionTaskId !== null,
                          onSelect: () => onRemove(env),
                        },
                      ]}
                    />
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
        {!rows.length && (
          <p className="p-8 text-center text-xs text-muted-foreground">
            No Environments match your search.
          </p>
        )}
      </div>
      <TablePagination table={table} label="Environments" />
    </section>
  );
}
