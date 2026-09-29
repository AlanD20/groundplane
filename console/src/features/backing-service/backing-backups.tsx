import {
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

import {
  ResourcePanel,
  SummaryItem,
  SummaryStrip,
} from "@/components/common/resource-panel";
import { Badge } from "@/components/ui/badge";
import { useStore } from "@/lib/store";
import type { Project } from "@/lib/types";

export function BackupsTab({
  g,
  env,
  svc,
}: {
  g: Project;
  env: NonNullable<Project["environments"]>[number];
  svc: NonNullable<Project["environments"]>[number]["services"][number];
}) {
  const store = useStore();
  const adapter = store.adapters.find((a) => a.key === svc.adapter);
  const consumerBackups = store.tenantProjects.flatMap((p) =>
    (p.environments ?? []).flatMap((e) => {
      const backup = e.backup;
      if (!backup) return [];
      return backup.sources
        .filter(
          (source) =>
            source.kind === "attach" &&
            e.attaches.find((attach) => attach.id === source.ref)?.projectId ===
              g.id,
        )
        .map((s) => {
          const attach = e.attaches.find((candidate) => candidate.id === s.ref);
          return {
            project: p.slug,
            environment: e.name,
            services: attach?.service ?? "—",
            database: s.target,
            kind: s.kind,
            enabled: backup.enabled,
            frequency: backup.frequency,
            keep: backup.keep,
            encryption: backup.encryption,
            records: "Environment Backups",
            lastRun: backup.lastRun ?? "—",
            nextRun: backup.nextRun ?? "—",
          };
        });
    }),
  );
  const enabledCount = consumerBackups.filter((c) => c.enabled).length;
  const table = useTableView(
    consumerBackups,
    {
      consumer: (c) => `${c.project}/${c.environment}`,
      database: (c) => c.database,
      kind: (c) => c.kind,
      policy: (c) => Number(c.enabled),
      last: (c) => Date.parse(c.lastRun) || 0,
      next: (c) => Date.parse(c.nextRun) || 0,
    },
    "consumer",
    "asc",
    g.id,
  );
  const disabledCount = consumerBackups.length - enabledCount;
  const noPolicy = (g.consumers?.length ?? 0) - consumerBackups.length;

  return (
    <ResourcePanel
      title="Consumer backups"
      actions={
        <Badge variant={enabledCount > 0 ? "success" : "muted"}>
          {enabledCount} enabled
        </Badge>
      }
    >
      {adapter?.custom ? (
        <p className="text-xs text-muted-foreground">
          Custom hooks do not provide a managed backup adapter. The operator is
          responsible for this service&apos;s backups, whether or not
          provisioning hooks are configured.
        </p>
      ) : (
        <p className="text-xs text-muted-foreground">
          Backup policies belong to consumer Environments. Configure them on
          each consumer&apos;s Backups page.
        </p>
      )}

      {!adapter?.custom && (
        <SummaryStrip>
          <SummaryItem label="Enabled">{enabledCount}</SummaryItem>
          <SummaryItem label="Disabled">{disabledCount}</SummaryItem>
          <SummaryItem label="No backup source">{noPolicy}</SummaryItem>
        </SummaryStrip>
      )}

      {consumerBackups.length === 0 ? (
        <div className="text-xs text-muted-foreground">
          no consumers with backup sources yet
        </div>
      ) : (
        <div className="overflow-x-auto">
          <Table className="w-full text-sm">
            <TableHeader>
              <TableRow className="border-b border-border text-left text-xs font-semibold text-muted-foreground">
                <TableSortHead
                  sort={table}
                  field="consumer"
                  className="py-2 pr-4"
                >
                  Consumer
                </TableSortHead>
                <TableSortHead
                  sort={table}
                  field="database"
                  className="py-2 pr-4"
                >
                  Database
                </TableSortHead>
                <TableSortHead sort={table} field="kind" className="py-2 pr-4">
                  Kind
                </TableSortHead>
                <TableSortHead
                  sort={table}
                  field="policy"
                  className="py-2 pr-4"
                >
                  Policy
                </TableSortHead>
                <TableHead className="py-2 pr-4">Records</TableHead>
                <TableSortHead sort={table} field="last" className="py-2 pr-4">
                  Last run
                </TableSortHead>
                <TableSortHead sort={table} field="next" className="py-2">
                  Next run
                </TableSortHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {table.rows.map((c) => (
                <TableRow
                  key={`${c.project}-${c.environment}-${c.database}`}
                  className="border-b border-border last:border-0"
                >
                  <TableCell className="py-2 pr-4">
                    <span className="font-mono text-xs">
                      {c.project}/{c.environment}
                    </span>
                    <span className="ml-2 font-mono text-[10px] text-muted-foreground">
                      for {c.services}
                    </span>
                  </TableCell>
                  <TableCell className="py-2 pr-4 font-mono text-xs">
                    {c.database}
                  </TableCell>
                  <TableCell className="py-2 pr-4 font-mono text-xs">
                    {c.kind}
                  </TableCell>
                  <TableCell className="py-2 pr-4">
                    <span className="font-mono text-xs">
                      {c.enabled ? `${c.frequency} · keep ${c.keep}` : "off"}
                    </span>
                    <span className="ml-2 font-mono text-[10px] text-muted-foreground">
                      {c.enabled ? c.encryption : ""}
                    </span>
                  </TableCell>
                  <TableCell className="py-2 pr-4 text-xs">
                    {c.records}
                  </TableCell>
                  <TableCell className="py-2 pr-4 text-xs text-muted-foreground">
                    {c.enabled ? c.lastRun : "—"}
                  </TableCell>
                  <TableCell className="py-2 text-xs text-muted-foreground">
                    {c.enabled ? c.nextRun : "—"}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          <div className="mt-3">
            <TablePagination table={table} label="Consumers" />
          </div>
        </div>
      )}
    </ResourcePanel>
  );
}
