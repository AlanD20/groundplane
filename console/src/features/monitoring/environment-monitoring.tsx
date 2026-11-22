import { EmptyState } from "@/components/common/empty-state";
import { ResourceRow, ResourceTable } from "@/components/common/resource-table";
import { StatusBadge } from "@/components/common/status-badge";
import {
  CollectionToolbar,
  TablePagination,
  TableSortHead,
  useTableView,
} from "@/components/common/table-controls";
import { Button } from "@/components/ui/button";
import { Select } from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { formatTimestamp } from "@/lib/format-timestamp";
import type { Environment } from "@/lib/types";
import { serviceObservationFromAPI } from "@/features/service/service-observation";
import { Activity, ArrowUpRight, Pause, Play } from "lucide-react";
import { useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { HealthOverview } from "./health-overview";
import { monitoringHealth } from "./health-summary";
import { MonitoringCursorPagination } from "./scoped-monitoring";
import {
  useMonitoringCursor,
  useMonitoringQuery,
} from "./use-monitoring-query";

export function EnvironmentMonitoring({ env }: { env: Environment }) {
  const [search] = useSearchParams();
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState("all");
  const monitoringPage = useMonitoringCursor(env.id);
  const monitoringRequest = useMemo(
    () =>
      ({
        scope: "environment" as const,
        id: env.id,
        range: "1h" as const,
        limit: 50,
        cursor: monitoringPage.cursor,
      }) as const,
    [env.id, monitoringPage.cursor],
  );
  const monitoring = useMonitoringQuery(monitoringRequest);
  const response =
    monitoring.value?.scope.kind === "environment" &&
    monitoring.value.scope.id === env.id
      ? monitoring.value.environment
      : undefined;
  const monitored = new Map(
    (response?.services.items ?? []).map((service) => [service.id, service]),
  );
  const services = env.services.map((service) => {
    const live = monitored.get(service.id);
    return live
      ? {
          ...service,
          runtimeIntent: live.runtime_intent,
          observation: serviceObservationFromAPI(live.observation),
        }
      : service;
  });
  const summary = monitoringHealth(services, monitoring.now);
  const visible = summary.rows.filter(
    (row) =>
      (!response || monitored.has(row.service.id)) &&
      row.service.name.toLowerCase().includes(query.trim().toLowerCase()) &&
      (filter === "all" ||
        (filter === "attention" ? row.attention : row.state === filter)),
  );
  const table = useTableView(
    visible,
    {
      name: (row) => row.service.name,
      state: (row) => row.state,
      sampled: (row) => row.sampledAt ?? "",
    },
    "name",
    "asc",
    `${env.id}/${query}/${filter}`,
  );
  const serviceHref = (id: string) => {
    const next = new URLSearchParams(search);
    next.set("service", id);
    next.set("serviceTab", "monitoring");
    return `?${next}`;
  };
  if (summary.total === 0)
    return (
      <EmptyState
        icon={<Activity />}
        title="No Services to monitor"
        description="Health appears here when this Environment has Services. Open Services to add a workload."
      />
    );
  return (
    <div className="space-y-5">
      <HealthOverview summary={summary} />
      <section
        aria-labelledby="monitoring-services-title"
        className="space-y-4"
      >
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="space-y-1">
            <h2
              id="monitoring-services-title"
              className="text-lg font-semibold"
            >
              Service health
            </h2>
            <p className="text-xs text-muted-foreground">
              Runtime observations in this Environment. Inspect a Service for
              its own monitoring and containers.
            </p>
          </div>
          <div className="flex items-center gap-3">
            <span className="text-xs text-muted-foreground">
              {summary.total} {summary.total === 1 ? "Service" : "Services"}
              {monitoring.paused ? " · live refresh paused" : ""}
            </span>
            <Button
              size="sm"
              variant="outline"
              onClick={() => monitoring.setPaused(!monitoring.paused)}
            >
              {monitoring.paused ? <Play /> : <Pause />}
              {monitoring.paused ? "Resume live" : "Pause live"}
            </Button>
          </div>
        </div>
        {monitoring.error && (
          <p role="alert" className="text-xs text-warning">
            Showing the last available Service observations: {monitoring.error}
          </p>
        )}
        <CollectionToolbar
          query={query}
          onQueryChange={setQuery}
          label={response ? "Service health on this page" : "Service health"}
        >
          <Select
            aria-label="Filter Service health"
            className="w-auto min-w-44"
            value={filter}
            onValueChange={setFilter}
            options={[
              { value: "all", label: "All runtime states" },
              { value: "attention", label: "Needs attention" },
              ...[
                "healthy",
                "running",
                "starting",
                "degraded",
                "stopped",
                "failed",
                "absent",
                "unavailable",
              ].map((state) => ({
                value: state,
                label:
                  state === "running"
                    ? "Running · no healthcheck"
                    : state.charAt(0).toUpperCase() + state.slice(1),
              })),
            ]}
          />
        </CollectionToolbar>
        {table.total === 0 ? (
          <EmptyState
            embedded
            title="No matching Services"
            description="Change the search or runtime filter to see other Services."
            action={
              <Button
                size="sm"
                variant="outline"
                onClick={() => {
                  setQuery("");
                  setFilter("all");
                }}
              >
                Clear filters
              </Button>
            }
          />
        ) : (
          <>
            <ResourceTable>
              <Table aria-label="Environment Service health">
                <TableHeader>
                  <TableRow>
                    <TableSortHead sort={table} field="name">
                      Service
                    </TableSortHead>
                    <TableSortHead sort={table} field="state">
                      Runtime
                    </TableSortHead>
                    <TableHead>Observed replicas</TableHead>
                    <TableHead>Desired runtime</TableHead>
                    <TableHead>Restarts</TableHead>
                    <TableSortHead sort={table} field="sampled">
                      Last observation
                    </TableSortHead>
                    <TableHead>
                      <span className="sr-only">Inspect</span>
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {(response ? table.sortedRows : table.rows).map((row) => (
                    <ResourceRow
                      key={row.service.id}
                      href={serviceHref(row.service.id)}
                    >
                      <TableCell>
                        <Link
                          to={serviceHref(row.service.id)}
                          className="font-medium hover:text-primary hover:underline"
                        >
                          {row.service.name}
                        </Link>
                      </TableCell>
                      <TableCell>
                        <StatusBadge
                          status={row.state}
                          label={
                            row.state === "running"
                              ? "Running · no healthcheck"
                              : undefined
                          }
                        />
                      </TableCell>
                      <TableCell className="tabular-nums">
                        {row.observation.state === "unavailable" ? (
                          "Unavailable"
                        ) : (
                          <span>
                            {row.observation.replicas.healthy +
                              row.observation.replicas.running}
                            /{row.observation.expectedReplicas} running{" "}
                            <span className="block text-xs text-muted-foreground">
                              {row.observation.replicas.healthy} healthy
                            </span>
                          </span>
                        )}
                      </TableCell>
                      <TableCell className="capitalize text-muted-foreground">
                        {row.service.runtimeIntent}
                      </TableCell>
                      <TableCell className="text-xs tabular-nums text-muted-foreground">
                        {monitored.get(row.service.id)?.restart_count ??
                          "Unavailable"}
                        {monitored.get(row.service.id)?.restart_observed_at && (
                          <span className="block">
                            {formatTimestamp(
                              monitored.get(row.service.id)
                                ?.restart_observed_at,
                              "",
                              { seconds: true },
                            )}
                          </span>
                        )}
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        {formatTimestamp(row.sampledAt, "Not observed", {
                          seconds: true,
                        })}
                        {row.state === "unavailable" && row.sampledAt && (
                          <span className="block text-warning">
                            Expired observation
                          </span>
                        )}
                      </TableCell>
                      <TableCell>
                        <Link
                          to={serviceHref(row.service.id)}
                          className="inline-flex items-center gap-1 text-xs text-primary"
                          aria-label={`Inspect ${row.service.name} monitoring`}
                        >
                          Inspect <ArrowUpRight className="size-3.5" />
                        </Link>
                      </TableCell>
                    </ResourceRow>
                  ))}
                </TableBody>
              </Table>
            </ResourceTable>
            {!response && <TablePagination table={table} label="Services" />}
          </>
        )}
        {response && (
          <MonitoringCursorPagination
            label="Services"
            page={monitoringPage}
            nextCursor={response.services.next_cursor}
            loading={monitoring.loading}
          />
        )}
      </section>
    </div>
  );
}
