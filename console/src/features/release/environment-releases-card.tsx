"use client";

import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "@/components/ui/table";
import { Link } from "react-router-dom";
import { useRequiredParams } from "@/lib/router";
import { History } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type { Environment, Service } from "@/lib/types";
import {
  TablePagination,
  TableSortHead,
  useTableView,
} from "@/components/common/table-controls";
import { ImageReference } from "@/components/common/image-reference";
import { formatTimestamp } from "@/lib/format-timestamp";

export function ReleasesCard({ env }: { env: Environment }) {
  const params = useRequiredParams("tenant", "project", "env");
  const table = useTableView(
    env.deploys,
    {
      service: (d) => d.service,
      tag: (d) => d.tag,
      strategy: (d) => d.strategy,
      when: (d) => Date.parse(d.when) || 0,
      status: (d) => d.status,
    },
    "when",
    "desc",
    env.id,
  );
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <History className="size-4 text-muted-foreground" /> Deployments
        </CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col">
        <p className="mb-2 text-xs text-muted-foreground">
          Deployment history is recorded as Releases. Open a deployment for its
          exact image and deployment details; track execution in Tasks.
        </p>
        <div className="overflow-x-auto rounded-lg border border-border">
          <Table className="w-full text-sm">
            <TableHeader>
              <TableRow className="border-b border-border text-left text-xs font-semibold text-muted-foreground">
                <TableSortHead
                  sort={table}
                  field="service"
                  className="px-3 py-2"
                >
                  Service
                </TableSortHead>
                <TableSortHead sort={table} field="tag" className="px-3 py-2">
                  Tag
                </TableSortHead>
                <TableHead className="px-3 py-2">Digest</TableHead>
                <TableSortHead
                  sort={table}
                  field="strategy"
                  className="px-3 py-2"
                >
                  Strategy
                </TableSortHead>
                <TableSortHead sort={table} field="when" className="px-3 py-2">
                  When
                </TableSortHead>
                <TableSortHead
                  sort={table}
                  field="status"
                  className="px-3 py-2"
                >
                  Status
                </TableSortHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {table.rows.map((d) => (
                <TableRow
                  key={d.id}
                  className="border-b border-border last:border-0"
                >
                  <TableCell className="px-3 py-2 font-mono text-xs">
                    <Link
                      className="text-primary hover:underline"
                      to={`/t/${params.tenant}/${params.project}/${params.env}/releases/${d.id}`}
                    >
                      {d.service}
                    </Link>
                  </TableCell>
                  <TableCell className="px-3 py-2 font-mono text-xs">
                    <ImageReference value={d.tag} />
                  </TableCell>
                  <TableCell className="max-w-[220px] truncate px-3 py-2 font-mono text-xs text-muted-foreground">
                    {d.digest ? (
                      <ImageReference value={d.digest} />
                    ) : (
                      "Not recorded"
                    )}
                  </TableCell>
                  <TableCell className="px-3 py-2">
                    <StrategyPill strategy={d.strategy} />
                  </TableCell>
                  <TableCell className="px-3 py-2 text-xs text-muted-foreground">
                    {formatTimestamp(d.when, "Time unavailable")}
                  </TableCell>
                  <TableCell className="px-3 py-2">
                    {d.status === "active" ? (
                      <span className="inline-flex items-center gap-1.5 rounded-full bg-success/10 px-2 py-0.5 text-xs text-success">
                        <span className="size-1.5 rounded-full bg-current" />{" "}
                        active
                      </span>
                    ) : (
                      <span className="inline-flex items-center gap-1.5 rounded-full bg-warning/10 px-2 py-0.5 text-xs text-warning">
                        <span className="size-1.5 rounded-full bg-current" />{" "}
                        superseded
                      </span>
                    )}
                  </TableCell>
                </TableRow>
              ))}
              {env.deploys.length === 0 && (
                <TableRow>
                  <TableCell
                    colSpan={6}
                    className="px-3 py-3 text-center text-xs text-muted-foreground"
                  >
                    no deploys yet
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>
        <div className="mt-3">
          <TablePagination table={table} label="Releases" />
        </div>
      </CardContent>
    </Card>
  );
}

export function StrategyPill({ strategy }: { strategy: Service["strategy"] }) {
  if (strategy === "blue-green")
    return (
      <span className="inline-flex items-center gap-1.5 rounded-full bg-primary/10 px-2 py-0.5 text-xs text-primary">
        <span className="size-1.5 rounded-full bg-current" /> blue-green
      </span>
    );
  if (strategy === "rolling")
    return (
      <span className="inline-flex items-center gap-1.5 rounded-full bg-warning/10 px-2 py-0.5 text-xs text-warning">
        <span className="size-1.5 rounded-full bg-current" /> rolling
      </span>
    );
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full bg-muted px-2 py-0.5 text-xs text-muted-foreground">
      <span className="size-1.5 rounded-full bg-current" /> recreate
    </span>
  );
}
