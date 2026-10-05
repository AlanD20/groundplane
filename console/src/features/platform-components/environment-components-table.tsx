import { ResourceRow, ResourceTable } from "@/components/common/resource-table";
import {
  CollectionToolbar,
  TablePagination,
  TableSortHead,
  useTableView,
} from "@/components/common/table-controls";
import { StatusBadge } from "@/components/common/status-badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useStore } from "@/lib/store";
import { useState } from "react";
import { Link } from "react-router-dom";

export function EnvironmentComponentsTable() {
  const store = useStore();
  const [query, setQuery] = useState("");
  const rows = store.tenantProjects
    .flatMap((p) => {
      const tenant = store.tenants.find((t) => t.id === p.tenantId);
      if (!tenant) return [];
      return (p.environments || []).flatMap((env) =>
        env.components.map((c) => ({
          ...c,
          name: c.kind === "caddy" ? "Caddy" : "Cloudflare Tunnel",
          environment: `${p.name}/${env.name}`,
          href: `/t/${tenant.slug}/${p.slug}/${encodeURIComponent(env.name)}?view=network&panel=router`,
        })),
      );
    })
    .filter((c) =>
      `${c.name} ${c.environment}`.toLowerCase().includes(query.toLowerCase()),
    );
  const table = useTableView(
    rows,
    {
      name: (c) => c.name,
      environment: (c) => c.environment,
      status: (c) => c.status,
    },
    "environment",
    "asc",
    query,
  );
  return (
    <div className="space-y-4">
      <CollectionToolbar
        query={query}
        onQueryChange={setQuery}
        label="Environment Components"
      />
      {store.projectsLoading && (
        <p role="status" className="text-xs text-muted-foreground">
          Loading Environment Components…
        </p>
      )}
      {store.projectError && (
        <p role="alert" className="text-xs text-destructive">
          {store.projectError}
        </p>
      )}
      <ResourceTable>
        <Table aria-label="Environment Components">
          <TableHeader>
            <TableRow>
              <TableSortHead sort={table} field="name">
                Component
              </TableSortHead>
              <TableSortHead sort={table} field="environment">
                Environment
              </TableSortHead>
              <TableSortHead sort={table} field="status">
                Status
              </TableSortHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {table.rows.map((c) => (
              <ResourceRow key={c.id} href={c.href}>
                <TableCell>
                  <Link to={c.href} className="font-medium hover:text-primary">
                    {c.name}
                  </Link>
                </TableCell>
                <TableCell>{c.environment}</TableCell>
                <TableCell>
                  <StatusBadge
                    status={c.status}
                    label={!c.enabled ? "Disabled" : undefined}
                  />
                </TableCell>
              </ResourceRow>
            ))}
          </TableBody>
        </Table>
      </ResourceTable>
      {!store.projectsLoading && !store.projectError && !rows.length && (
        <p className="py-4 text-xs text-muted-foreground">
          {query
            ? "No matching Components."
            : "No Environment Components loaded."}
        </p>
      )}
      <TablePagination table={table} label="Components" />
    </div>
  );
}
