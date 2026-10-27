import { ReleaseHistoryRow } from "./release-history-row";
import { useState } from "react";
("use client");

import { History } from "lucide-react";
import { Select } from "@/components/ui/select";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type { Environment, Service } from "@/lib/types";
import {
  TablePagination,
  ListToolbar,
  useTableView,
} from "@/components/common/table-controls";

export function ReleasesCard({ env }: { env: Environment }) {
  const [query, setQuery] = useState("");
  const [service, setService] = useState("all");
  const [state, setState] = useState("all");
  const [strategy, setStrategy] = useState("all");
  const table = useTableView(
    env.deploys.filter(
      (release) =>
        (service === "all" || release.service === service) &&
        (state === "all" ||
          (state === "serving"
            ? release.status === "active"
            : release.state === state)) &&
        (strategy === "all" || release.strategy === strategy) &&
        `${release.service} ${release.tag} ${release.digest}`
          .toLowerCase()
          .includes(query.toLowerCase()),
    ),
    {
      service: (d) => d.service,
      tag: (d) => d.tag,
      strategy: (d) => d.strategy,
      when: (d) => Date.parse(d.when) || 0,
      status: (d) => d.state,
    },
    "when",
    "desc",
    `${env.id}/${query}/${service}/${state}/${strategy}`,
  );
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <History className="size-4 text-muted-foreground" /> Deployments
        </CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
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
        <div className="flex flex-wrap items-center gap-2">
          <Select
            searchable
            aria-label="Filter deployments by Service"
            value={service}
            onValueChange={setService}
            options={[
              { value: "all", label: "All Services" },
              ...[...new Set(env.deploys.map((release) => release.service))]
                .sort()
                .map((name) => ({ value: name, label: name })),
            ]}
          />
          <Select
            aria-label="Filter deployment state"
            value={state}
            onValueChange={setState}
            options={[
              { value: "all", label: "All states" },
              { value: "serving", label: "Serving" },
              ...[...new Set(env.deploys.map((release) => release.state))]
                .filter((value) => value !== "serving")
                .sort()
                .map((value) => ({ value, label: value.replaceAll("_", " ") })),
            ]}
          />
          <Select
            aria-label="Filter deployment strategy"
            value={strategy}
            onValueChange={setStrategy}
            options={[
              { value: "all", label: "All strategies" },
              ...["recreate", "blue-green", "rolling"].map((value) => ({
                value,
                label: value,
              })),
            ]}
          />
          <span className="text-xs text-muted-foreground">
            {table.field === "when"
              ? table.direction === "desc"
                ? "Newest first"
                : "Oldest first"
              : "Sorted by " + table.field}
          </span>
        </div>
        <div className="space-y-3">
          {table.rows.map((release) => (
            <ReleaseHistoryRow key={release.id} release={release} env={env} />
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
