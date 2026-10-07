import { workspaceSectionClassName } from "@/components/common/workspace-section";
import { Badge } from "@/components/ui/badge";
import { useState } from "react";
("use client");

import { Link } from "react-router-dom";
import { useRequiredParams } from "@/lib/router";
import { History } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type { Environment, Service } from "@/lib/types";
import {
  TablePagination,
  ListToolbar,
  useTableView,
} from "@/components/common/table-controls";
import { ImageReference } from "@/components/common/image-reference";
import { formatTimestamp } from "@/lib/format-timestamp";

export function ReleasesCard({ env }: { env: Environment }) {
  const params = useRequiredParams("tenant", "project", "env");
  const [query, setQuery] = useState("");
  const table = useTableView(
    env.deploys.filter((release) =>
      `${release.service} ${release.tag} ${release.digest}`
        .toLowerCase()
        .includes(query.toLowerCase()),
    ),
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
      <CardContent className="flex flex-col gap-4">
        <p className="mb-2 text-xs text-muted-foreground">
          Deployment history is recorded as Releases. Open a deployment for its
          exact image and deployment details; track execution in Tasks.
        </p>
        <ListToolbar
          label="Deployments"
          query={query}
          onQueryChange={setQuery}
          sort={table}
          fields={[
            { value: "service", label: "Service" },
            { value: "tag", label: "Tag" },
            { value: "strategy", label: "Strategy" },
            { value: "when", label: "Time" },
            { value: "status", label: "Status" },
          ]}
        />
        <div className="space-y-3">
          {table.rows.map((release) => (
            <article
              key={release.id}
              className={workspaceSectionClassName(false, "space-y-3")}
            >
              <div className="flex flex-wrap items-center justify-between gap-3">
                <Link
                  className="min-w-0 break-all text-sm font-medium text-primary hover:underline"
                  to={`/t/${params.tenant}/${params.project}/${params.env}/releases/${release.id}`}
                >
                  {release.service}
                </Link>
                <Badge
                  variant={release.status === "active" ? "success" : "outline"}
                >
                  {release.status}
                </Badge>
              </div>
              <div className="flex min-w-0 flex-wrap items-center gap-3">
                <ImageReference value={release.tag} />
                <StrategyPill strategy={release.strategy} />
              </div>
              <div className="flex min-w-0 flex-wrap items-center justify-between gap-3 border-t border-border pt-3 text-xs text-muted-foreground">
                {release.digest ? (
                  <ImageReference value={release.digest} />
                ) : (
                  <span>Digest not recorded</span>
                )}
                <span>{formatTimestamp(release.when, "Time unavailable")}</span>
              </div>
            </article>
          ))}
          {!table.total && (
            <p className="py-4 text-sm text-muted-foreground">
              {env.deploys.length
                ? "No deployments match your search."
                : "No deployments yet."}
            </p>
          )}
        </div>
        <div className="mt-3">
          <TablePagination table={table} label="Deployments" />
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
