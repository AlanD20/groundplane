import { ImageReference } from "@/components/common/image-reference";
import { ResourceActionMenu } from "@/components/common/resource-action-menu";
import { ResourceRow, ResourceTable } from "@/components/common/resource-table";
import { StatusBadge } from "@/components/common/status-badge";
import {
  CollectionToolbar,
  TablePagination,
  TableSortHead,
  useTableView,
} from "@/components/common/table-controls";
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
      <CollectionToolbar
        query={query}
        onQueryChange={setQuery}
        label="Environments"
      />
      {observation.refreshError && (
        <p role="alert" className="text-xs text-destructive">
          Runtime refresh failed: {observation.refreshError}
        </p>
      )}
      <ResourceTable>
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
                <ResourceRow key={env.id} href={path}>
                  <TableCell>
                    <Link
                      to={path}
                      className="text-sm font-medium hover:text-primary"
                    >
                      {env.name}
                    </Link>
                    <p className="mt-1 text-xs text-muted-foreground">
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
                    <p className="mt-1 text-xs text-muted-foreground">
                      {env.routes.length} Routes, {env.zones.length} Zones,{" "}
                      {env.attaches.length} connections
                    </p>
                  </TableCell>
                  <TableCell className="max-w-60">
                    <ImageReference value={env.release} />
                    <p className="mt-1 text-xs text-muted-foreground">
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
                </ResourceRow>
              );
            })}
          </TableBody>
        </Table>
        {!rows.length && (
          <p className="p-8 text-center text-xs text-muted-foreground">
            No Environments match your search.
          </p>
        )}
      </ResourceTable>
      <TablePagination table={table} label="Environments" />
    </section>
  );
}
