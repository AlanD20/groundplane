import { PageHeader } from "@/components/common/page-header";
import { ResourcePanel } from "@/components/common/resource-panel";
import { ResourceRow, ResourceTable } from "@/components/common/resource-table";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { TaskList } from "@/features/task/task-list";
import { useStore } from "@/lib/store";
import type { TaskJournalScope } from "@/lib/types";
import { Network, RefreshCw, Server } from "lucide-react";
import { Link } from "react-router-dom";

const scope: TaskJournalScope = { kind: "workspace", workspace: "platform" };

export default function PlatformInfraPage() {
  const {
    platform,
    platformComponentsLoading,
    platformComponentError,
    refreshPlatformComponents,
  } = useStore();
  return (
    <div className="flex flex-col gap-6">
      <PageHeader title="Components" icon={<Server />} />
      <ResourcePanel title="Platform Components">
        {platformComponentsLoading && (
          <p role="status" className="text-sm text-muted-foreground">
            Loading Components…
          </p>
        )}
        {platformComponentError && (
          <div
            role="alert"
            className="flex items-center justify-between gap-3 text-sm text-destructive"
          >
            <span>{platformComponentError}</span>
            <Button
              variant="outline"
              size="sm"
              onClick={() =>
                void refreshPlatformComponents().catch(() => undefined)
              }
            >
              <RefreshCw className="size-3.5" />
              Retry
            </Button>
          </div>
        )}
        {!platformComponentsLoading && !platformComponentError && (
          <ResourceTable>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Component</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Runtime</TableHead>
                  <TableHead>Network</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {platform.components.map((component) => (
                  <ResourceRow
                    key={component.id}
                    href={`/platform/components/${component.kind}`}
                  >
                    <TableCell>
                      <Link
                        to={`/platform/components/${component.kind}`}
                        className="inline-flex items-center gap-2 font-medium hover:text-primary"
                      >
                        <Network className="size-4 text-muted-foreground" />
                        {component.name}
                      </Link>
                    </TableCell>
                    <TableCell>
                      <StatusBadge status={component.status} />
                    </TableCell>
                    <TableCell>{component.runtime}</TableCell>
                    <TableCell>
                      {component.hostNetwork
                        ? "Host network"
                        : "Container network"}
                    </TableCell>
                  </ResourceRow>
                ))}
              </TableBody>
            </Table>
          </ResourceTable>
        )}
      </ResourcePanel>
      <TaskList scope={scope} title="Platform Tasks" />
    </div>
  );
}
