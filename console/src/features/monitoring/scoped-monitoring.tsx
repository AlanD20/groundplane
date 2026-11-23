import { EmptyState } from "@/components/common/empty-state";
import { ResourcePanel } from "@/components/common/resource-panel";
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
import { formatTimestamp } from "@/lib/format-timestamp";
import { Activity, ChevronLeft, ChevronRight } from "lucide-react";
import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import type { MonitoringRange, MonitoringScopeKind } from "./api";
import { formatMetric, metricAverage } from "./metric-projection";
import { MonitoringMetrics } from "./monitoring-metrics";
import { MonitoringStatusBadge } from "./monitoring-status";
import {
  useMonitoringCursor,
  useMonitoringQuery,
} from "./use-monitoring-query";

export type MonitoringLink = (kind: string, id: string) => string | undefined;

export function ScopedMonitoring({
  scope,
  id,
  name,
  linkFor,
}: {
  scope: MonitoringScopeKind;
  id: string;
  name: string;
  linkFor?: MonitoringLink;
}) {
  const [range, setRange] = useState<MonitoringRange>("1h");
  const pageKey = `${scope}/${id}/${range}`;
  const page = useMonitoringCursor(pageKey);
  const consumerPage = useMonitoringCursor(pageKey);
  const request = useMemo(
    () => ({
      scope,
      id,
      range,
      points: 240,
      limit: 50,
      cursor: page.cursor,
      consumerCursor: consumerPage.cursor,
    }),
    [scope, id, range, page.cursor, consumerPage.cursor],
  );
  const query = useMonitoringQuery(request);
  const response = query.value;
  if (query.loading && !response)
    return (
      <EmptyState
        loading
        icon={<Activity />}
        title="Loading monitoring"
        description={`Reading current and retained evidence for ${name}.`}
      />
    );
  if (!response)
    return (
      <EmptyState
        error
        icon={<Activity />}
        title="Monitoring is unavailable"
        description={
          query.error ?? "The Controller returned no monitoring evidence."
        }
      />
    );
  if (response.scope.kind !== scope || response.scope.id !== id)
    return (
      <EmptyState
        error
        title="Monitoring scope did not match"
        description="The Controller response belongs to a different resource. No values are shown."
      />
    );

  return (
    <div className="space-y-5">
      <MonitoringMetrics
        query={query}
        response={response}
        range={range}
        onRangeChange={setRange}
      />
      {query.pageMatches ? (
        <ScopeDetails
          response={response}
          linkFor={linkFor}
          now={query.now}
          loading={query.loading}
          page={page}
          consumerPage={consumerPage}
        />
      ) : (
        <MonitoringPagePending
          error={query.error}
          page={page}
          consumerPage={consumerPage}
        />
      )}
    </div>
  );
}

function ScopeDetails({
  response,
  linkFor,
  now,
  loading,
  page,
  consumerPage,
}: {
  response: import("./api").MonitoringResponse;
  linkFor?: MonitoringLink;
  now: number;
  loading: boolean;
  page: MonitoringCursor;
  consumerPage: MonitoringCursor;
}) {
  const aggregate = response.tenant ?? response.project;
  const childrenLabel = response.tenant ? "Projects" : "Environments";
  const service = response.service ?? response.backing?.runtime;
  return (
    <>
      {aggregate && (
        <ResourcePanel title={childrenLabel}>
          <Table aria-label={childrenLabel}>
            <TableHeader>
              <TableRow>
                <TableHead>
                  {response.tenant ? "Project" : "Environment"}
                </TableHead>
                <TableHead>Runtime health</TableHead>
                <TableHead>Resource samples</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {(aggregate.children.items ?? []).map((child) => {
                const href = linkFor?.(child.scope.kind, child.scope.id);
                return (
                  <TableRow key={`${child.scope.kind}/${child.scope.id}`}>
                    <TableCell className="font-medium">
                      {href ? (
                        <Link to={href}>{child.scope.name}</Link>
                      ) : (
                        child.scope.name
                      )}
                    </TableCell>
                    <TableCell>
                      <StatusBadge status={child.runtime_state} />
                    </TableCell>
                    <TableCell>
                      <MonitoringStatusBadge live={child.live} now={now} />
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
          {!aggregate.children.items?.length && (
            <p role="status" className="text-sm text-muted-foreground">
              No {childrenLabel.toLowerCase()} on this page.
            </p>
          )}
          <MonitoringCursorPagination
            label={childrenLabel}
            page={page}
            nextCursor={aggregate.children.next_cursor}
            loading={loading}
          />
        </ResourcePanel>
      )}
      {response.host && <HostDetails host={response.host} />}
      {service && (
        <ServiceDetails
          service={service}
          now={now}
          loading={loading}
          page={page}
        />
      )}
      {response.backing && (
        <ResourcePanel title="Consumers">
          {(response.backing.consumers.items ?? []).length ? (
            <ul className="divide-y divide-border text-sm">
              {(response.backing.consumers.items ?? []).map((consumer) => (
                <li
                  key={consumer.id}
                  className="flex justify-between gap-3 py-3"
                >
                  <span>{consumer.name}</span>
                  <span className="text-xs text-muted-foreground">
                    Consumer Service
                  </span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-sm text-muted-foreground">No consumers.</p>
          )}
          <MonitoringCursorPagination
            label="Consumers"
            page={consumerPage}
            nextCursor={response.backing.consumers.next_cursor}
            loading={loading}
          />
        </ResourcePanel>
      )}
    </>
  );
}

function HostDetails({
  host,
}: {
  host: NonNullable<import("./api").MonitoringResponse["host"]>;
}) {
  return (
    <div className="grid items-start gap-5 xl:grid-cols-2">
      <ResourcePanel title="Network interfaces">
        <ul className="divide-y divide-border text-sm">
          {(host.interfaces ?? []).map((network) => (
            <li
              key={network.name}
              className="grid grid-cols-[1fr_auto] gap-3 py-3"
            >
              <span className="font-medium">
                {network.name} · {network.physical ? "physical" : "virtual"}
              </span>
              <span className="text-right text-xs tabular-nums text-muted-foreground">
                RX{" "}
                {formatMetric(
                  network.receive_bytes_per_second ?? null,
                  "bytes/s",
                )}{" "}
                · TX{" "}
                {formatMetric(
                  network.transmit_bytes_per_second ?? null,
                  "bytes/s",
                )}
              </span>
            </li>
          ))}
        </ul>
      </ResourcePanel>
      <ResourcePanel title="Block devices">
        <ul className="divide-y divide-border text-sm">
          {(host.devices ?? []).map((device) => (
            <li
              key={`${device.major}:${device.minor}`}
              className="grid grid-cols-[1fr_auto] gap-3 py-3"
            >
              <span className="font-medium">
                {device.name}{" "}
                {device.root_filesystem ? "· root filesystem" : ""}
              </span>
              <span className="text-right text-xs tabular-nums text-muted-foreground">
                Read{" "}
                {formatMetric(device.read_bytes_per_second ?? null, "bytes/s")}{" "}
                · Write{" "}
                {formatMetric(device.write_bytes_per_second ?? null, "bytes/s")}
              </span>
            </li>
          ))}
        </ul>
      </ResourcePanel>
    </div>
  );
}

function ServiceDetails({
  service,
  now,
  loading,
  page,
}: {
  service: NonNullable<import("./api").MonitoringResponse["service"]>;
  now: number;
  loading: boolean;
  page: MonitoringCursor;
}) {
  return (
    <div className="grid items-start gap-5 xl:grid-cols-2">
      <ResourcePanel title="Container resource usage">
        {service.containers_status === "available" ? (
          <ul className="divide-y divide-border text-sm">
            {(service.containers.items ?? []).map((container) => (
              <li
                key={container.id}
                className="flex items-center justify-between gap-3 py-3"
              >
                <span className="min-w-0">
                  <span className="block font-medium">
                    Replica {container.replica}
                  </span>
                  <span className="block truncate font-mono text-xs text-muted-foreground">
                    {container.id}
                  </span>
                  <span className="mt-1 block text-xs tabular-nums text-muted-foreground">
                    CPU{" "}
                    {formatMetric(
                      metricAverage(container.live.values, "cpu_cores"),
                      "cores",
                    )}{" "}
                    · Memory{" "}
                    {formatMetric(
                      metricAverage(
                        container.live.values,
                        "memory_working_set_bytes",
                      ),
                      "bytes",
                    )}
                  </span>
                </span>
                <span className="text-right">
                  <MonitoringStatusBadge live={container.live} now={now} />
                  <span className="mt-1 block text-xs text-muted-foreground">
                    {container.restart_count} restarts
                  </span>
                </span>
              </li>
            ))}
          </ul>
        ) : (
          <p role="status" className="text-sm text-muted-foreground">
            No current resource measurements for these containers.
          </p>
        )}
        <MonitoringCursorPagination
          label="Containers"
          page={page}
          nextCursor={service.containers.next_cursor}
          loading={loading}
        />
      </ResourcePanel>
      <ResourcePanel title="Persistent volumes">
        {service.volumes?.length ? (
          <ul className="divide-y divide-border text-sm">
            {(service.volumes ?? []).map((volume) => (
              <li key={volume.id} className="flex justify-between gap-3 py-3">
                <span className="font-mono text-xs">{volume.id}</span>
                <span className="text-right text-xs text-muted-foreground">
                  {formatMetric(volume.used_bytes ?? null, "bytes")}
                  <span className="block">
                    {formatTimestamp(volume.observed_at, "Not observed", {
                      seconds: true,
                    })}
                  </span>
                </span>
              </li>
            ))}
          </ul>
        ) : (
          <p className="text-sm text-muted-foreground">
            No persistent Volume observations.
          </p>
        )}
      </ResourcePanel>
    </div>
  );
}

type MonitoringCursor = ReturnType<typeof useMonitoringCursor>;

export function MonitoringPagePending({
  error,
  page,
  consumerPage,
}: {
  error?: string;
  page: MonitoringCursor;
  consumerPage?: MonitoringCursor;
}) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border p-4">
      <p
        role={error ? "alert" : "status"}
        className="text-sm text-muted-foreground"
      >
        {error
          ? `Could not load this monitoring page. ${error}`
          : "Loading monitoring details…"}
      </p>
      <div className="flex flex-wrap gap-2">
        {page.canPrevious && (
          <Button variant="outline" size="sm" onClick={page.previous}>
            {consumerPage?.canPrevious
              ? "Previous resource page"
              : "Previous page"}
          </Button>
        )}
        {consumerPage?.canPrevious && (
          <Button variant="outline" size="sm" onClick={consumerPage.previous}>
            Previous consumer page
          </Button>
        )}
      </div>
    </div>
  );
}

export function MonitoringCursorPagination({
  label,
  page,
  nextCursor,
  loading,
}: {
  label: string;
  page: MonitoringCursor;
  nextCursor?: string | null;
  loading: boolean;
}) {
  if (!page.canPrevious && !nextCursor) return null;
  return (
    <nav
      aria-label={`${label} pagination`}
      className="flex flex-wrap items-center justify-between gap-3 border-t border-border pt-3"
    >
      <span role="status" className="text-xs text-muted-foreground">
        Page {page.page}
      </span>
      <div className="flex items-center gap-2">
        <Button
          variant="outline"
          size="sm"
          disabled={loading || !page.canPrevious}
          onClick={page.previous}
        >
          <ChevronLeft className="size-4" /> Previous
        </Button>
        <Button
          variant="outline"
          size="sm"
          disabled={loading || !nextCursor}
          onClick={() => nextCursor && page.next(nextCursor)}
        >
          Next <ChevronRight className="size-4" />
        </Button>
      </div>
    </nav>
  );
}
