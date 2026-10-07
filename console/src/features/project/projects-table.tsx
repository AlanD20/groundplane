import { ResourceActionMenu } from "@/components/common/resource-action-menu";
import { ResourceRow, ResourceTable } from "@/components/common/resource-table";
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
import { useStore } from "@/lib/store";
import type { Project, Tenant } from "@/lib/types";
import { ExternalLink, Settings, Trash2 } from "lucide-react";
import { useState } from "react";
import { Link } from "react-router-dom";

export function ProjectsTable({
  tenant,
  projects,
  onRemove,
}: {
  tenant: Tenant;
  projects: Project[];
  onRemove: (project: Project) => void;
}) {
  const store = useStore();
  const [query, setQuery] = useState("");
  const rows = projects.filter((project) =>
    `${project.name} ${project.description}`
      .toLowerCase()
      .includes(query.trim().toLowerCase()),
  );
  const table = useTableView(
    rows,
    {
      name: (project) => project.name,
      environments: (project) => project.environments?.length ?? 0,
      services: (project) =>
        project.environments?.reduce(
          (count, env) => count + env.services.length,
          0,
        ) ?? 0,
    },
    "name",
    "asc",
    `${tenant.id}/${query}`,
  );
  return (
    <section className="space-y-4" aria-label="Projects">
      <CollectionToolbar
        query={query}
        onQueryChange={setQuery}
        label="Projects"
      />
      <ResourceTable>
        <Table>
          <TableHeader>
            <TableRow>
              <TableSortHead sort={table} field="name">
                Project
              </TableSortHead>
              <TableSortHead sort={table} field="environments">
                Environments
              </TableSortHead>
              <TableSortHead sort={table} field="services">
                Services
              </TableSortHead>
              <TableHead>Runners</TableHead>
              <TableHead>
                <span className="sr-only">Actions</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {table.rows.map((project) => {
              const path = `/t/${tenant.slug}/${project.slug}`;
              return (
                <ResourceRow key={project.id} href={path}>
                  <TableCell className="min-w-44">
                    <Link
                      to={path}
                      className="text-sm font-medium hover:text-primary"
                    >
                      {project.name}
                    </Link>
                    <p className="mt-1 max-w-80 truncate text-xs text-muted-foreground">
                      {project.description}
                    </p>
                    {project.createdAt && (
                      <p className="mt-1 text-xs text-muted-foreground">
                        Created {project.createdAt}
                      </p>
                    )}
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-wrap gap-2">
                      {project.environments?.map((env) => (
                        <Link
                          key={env.id}
                          to={`${path}/${encodeURIComponent(env.name)}`}
                          className="rounded border border-border px-2 py-1 text-xs hover:border-primary hover:text-primary"
                        >
                          {env.name}
                        </Link>
                      ))}
                      {!project.environments?.length && "None yet"}
                    </div>
                  </TableCell>
                  <TableCell>
                    {project.environments?.reduce(
                      (count, env) => count + env.services.length,
                      0,
                    ) ?? 0}
                  </TableCell>
                  <TableCell>
                    {
                      store.runners.filter(
                        (runner) =>
                          runner.tenantId === tenant.id &&
                          runner.projectId === project.id,
                      ).length
                    }
                  </TableCell>
                  <TableCell className="w-12">
                    <ResourceActionMenu
                      label={`Actions for ${project.name}`}
                      actions={[
                        {
                          label: "Open Project",
                          icon: <ExternalLink />,
                          href: path,
                        },
                        {
                          label: "Project settings",
                          icon: <Settings />,
                          href: `${path}/settings`,
                        },
                        {
                          label: "Delete Project",
                          icon: <Trash2 />,
                          destructive: true,
                          disabled: project.deletionTaskId !== null,
                          onSelect: () => onRemove(project),
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
            No Projects match your search.
          </p>
        )}
      </ResourceTable>
      <TablePagination table={table} label="Projects" />
    </section>
  );
}
