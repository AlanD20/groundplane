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

export function ReleasesCard({ env }: { env: Environment }) {
  const params = useRequiredParams("tenant", "project", "env");
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <History className="size-4 text-muted-foreground" /> Release ledger
        </CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col">
        <p className="mb-2 text-xs text-muted-foreground">
          The per-service release ledger — state, not events: which tag is
          active, which are superseded. This is what rollback reads to
          pre-select the previous tag (a successful redeploy of the current tag
          never advances it). Live execution lives on the Tasks tab.
        </p>
        <div className="overflow-x-auto rounded-lg border border-border">
          <Table className="w-full text-sm">
            <TableHeader>
              <TableRow className="border-b border-border text-left text-xs font-semibold text-muted-foreground">
                <TableHead className="px-3 py-2">Service</TableHead>
                <TableHead className="px-3 py-2">Tag</TableHead>
                <TableHead className="px-3 py-2">Digest</TableHead>
                <TableHead className="px-3 py-2">Strategy</TableHead>
                <TableHead className="px-3 py-2">When</TableHead>
                <TableHead className="px-3 py-2">Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {env.deploys.map((d, i) => (
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
                    {d.tag}
                  </TableCell>
                  <TableCell className="max-w-[220px] truncate px-3 py-2 font-mono text-xs text-muted-foreground">
                    {d.digest}
                  </TableCell>
                  <TableCell className="px-3 py-2">
                    <StrategyPill strategy={d.strategy} />
                  </TableCell>
                  <TableCell className="px-3 py-2 text-xs text-muted-foreground">
                    {d.when}
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
