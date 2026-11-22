import { EmptyState } from "@/components/common/empty-state";
import {
  ResourcePanel,
  SummaryItem,
  SummaryStrip,
} from "@/components/common/resource-panel";
import { useWorkspaceFullscreen } from "@/components/common/use-workspace-fullscreen";
import { StatusBadge } from "@/components/common/status-badge";
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
import {
  readChartTimeZone,
  saveChartTimeZone,
} from "@/lib/console-preferences";
import {
  Activity,
  ChevronLeft,
  ChevronRight,
  Maximize,
  Minimize,
  Pause,
  Play,
} from "lucide-react";
import { useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import type {
  MonitoringRange,
  MonitoringResponse,
  MonitoringScopeKind,
} from "./api";
import {
  formatMetric,
  metricAverage,
  metricGroups,
  metricSeries,
  type MonitoringValues,
} from "./metric-projection";
import { MonitoringChart } from "./monitoring-chart";
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
      <ScopeDetails
        response={response}
        linkFor={linkFor}
        now={query.now}
        loading={query.loading}
        page={page}
        consumerPage={consumerPage}
      />
    </div>
  );
}

export function MonitoringMetrics({
  query,
  response,
  range,
  onRangeChange,
}: {
  query: ReturnType<typeof useMonitoringQuery>;
  response: MonitoringResponse;
  range: MonitoringRange;
  onRangeChange: (range: MonitoringRange) => void;
}) {
  const [chartTimeZone, setChartTimeZone] = useState(readChartTimeZone);
  const [metric, setMetric] = useState("cpu");
  const fullscreenButton = useRef<HTMLButtonElement>(null);
  const [fullscreen, setFullscreen] = useWorkspaceFullscreen(undefined, () =>
    fullscreenButton.current?.focus({ preventScroll: true }),
  );
  const selected =
    metricGroups.find((group) => group.id === metric) ?? metricGroups[0];

  const subject =
    response.host ??
    response.tenant ??
    response.project ??
    response.environment ??
    response.service ??
    response.backing?.runtime;
  if (!subject?.live)
    return (
      <EmptyState
        title="Resource usage unavailable"
        description="The Controller returned no resource measurements for this view."
      />
    );

  const series = metricSeries(subject.history ?? [], selected);
  const live = subject.live;
  const owners = live.coverage.eligible_resources;
  const reporting = live.coverage.sampled_resources;
  const timeZone =
    chartTimeZone === "local"
      ? Intl.DateTimeFormat().resolvedOptions().timeZone
      : "UTC";
  const historyUnavailable = response.history_status !== "available";
  const rangeControls = (
    <>
      <Select
        aria-label="Monitoring history range"
        className="w-28"
        value={range}
        onValueChange={(value) => onRangeChange(value as MonitoringRange)}
        options={[
          { value: "1h", label: "1 hour" },
          { value: "6h", label: "6 hours" },
          { value: "24h", label: "24 hours" },
          { value: "7d", label: "7 days" },
        ]}
      />
      <Button
        variant="outline"
        size="sm"
        onClick={() => query.setPaused(!query.paused)}
      >
        {query.paused ? <Play /> : <Pause />}
        {query.paused ? "Resume live" : "Pause live"}
      </Button>
    </>
  );

  return (
    <div className="space-y-5">
      <section
        aria-label={`${response.scope.name} monitoring status`}
        className="rounded-xl border border-primary/25 bg-linear-to-r from-primary/8 via-card to-card p-5 shadow-sm"
      >
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div className="space-y-2">
            <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
              Resource usage
            </p>
            <div className="flex items-center gap-2">
              <MonitoringStatusBadge live={live} now={query.now} />
              {owners > 0 && response.scope.kind !== "host" && (
                <span className="text-xs text-muted-foreground">
                  {reporting} of {owners}{" "}
                  {owners === 1 ? "service" : "services"} reporting
                </span>
              )}
            </div>
            <p className="text-xs text-muted-foreground">
              {live.observed_at
                ? `Observed ${formatTimestamp(live.observed_at, "Unavailable", { seconds: true })}`
                : "No current sample"}
              {query.paused ? " · live refresh paused" : ""}
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            {!fullscreen && rangeControls}
          </div>
        </div>
        {query.error && (
          <p role="alert" className="mt-3 text-xs text-warning">
            Showing the last response; refresh failed: {query.error}
          </p>
        )}
      </section>

      {live.status !== "unavailable" && <LiveSummary values={live.values} />}

      <div
        className={
          fullscreen
            ? "workspace-fullscreen overflow-y-auto bg-card [&>[data-slot=card]]:min-h-full [&>[data-slot=card]]:rounded-none"
            : undefined
        }
      >
        <ResourcePanel
          title={`${selected.label} history`}
          actions={
            <>
              {fullscreen && rangeControls}
              <Select
                aria-label="Monitoring metric"
                className="w-44"
                value={selected.id}
                onValueChange={setMetric}
                options={metricGroups.map((group) => ({
                  value: group.id,
                  label: group.label,
                }))}
              />
              <Select
                aria-label="Chart time zone"
                className="w-32"
                value={chartTimeZone}
                onValueChange={(value) => {
                  const next = value === "local" ? "local" : "utc";
                  setChartTimeZone(next);
                  saveChartTimeZone(next);
                }}
                options={[
                  { value: "utc", label: "UTC" },
                  { value: "local", label: "Local time" },
                ]}
              />
              <Button
                ref={fullscreenButton}
                variant="outline"
                size="sm"
                aria-pressed={fullscreen}
                onClick={() => setFullscreen(!fullscreen)}
              >
                {fullscreen ? <Minimize /> : <Maximize />}
                {fullscreen ? "Exit fullscreen" : "Fullscreen"}
              </Button>
            </>
          }
        >
          {query.loading && response.window.range !== range && (
            <p role="status" className="text-xs text-muted-foreground">
              Loading the selected range…
            </p>
          )}
          {fullscreen && query.error && (
            <p role="alert" className="text-xs text-warning">
              {query.error}
            </p>
          )}
          {historyUnavailable && (
            <p role="status" className="text-xs text-warning">
              Historical storage is unavailable. Current evidence continues to
              refresh independently.
            </p>
          )}
          {series.length ? (
            <MonitoringChart
              scopeKey={`${response.scope.kind}/${response.scope.id}`}
              scopedLabel={response.scope.name}
              rangeLabel={response.window.range}
              window={{
                from: Date.parse(response.window.start),
                to: Date.parse(response.window.end),
              }}
              series={series}
              timeZone={timeZone}
              height={
                fullscreen
                  ? "max(280px, calc(100dvh - 13rem - var(--terminal-dock-height, 0px)))"
                  : undefined
              }
            />
          ) : (
            <EmptyState
              embedded
              icon={<Activity />}
              title={`No ${selected.label.toLowerCase()} history`}
              description="No samples were recorded in this time range."
            />
          )}
        </ResourcePanel>
      </div>
    </div>
  );
}

function LiveSummary({ values }: { values: MonitoringValues }) {
  const cpuCores = metricAverage(values, "cpu_cores");
  const ratio = metricAverage(values, "cpu_utilization_ratio");
  const cpu = cpuCores ?? (ratio === null ? null : ratio * 100);
  const cpuUnit = cpuCores !== null ? "cores" : "%";
  const workingSet = metricAverage(values, "memory_working_set_bytes");
  const memory = workingSet ?? metricAverage(values, "memory_used_bytes");
  const volumeUsage = metricAverage(values, "persistent_volume_used_bytes");
  const hostDiskUsage = metricAverage(values, "disk_used_bytes");
  const storage =
    volumeUsage ??
    hostDiskUsage ??
    metricAverage(values, "writable_layer_bytes");
  const storageLabel =
    volumeUsage !== null
      ? "Persistent volumes"
      : hostDiskUsage !== null
        ? "Disk used"
        : storage !== null
          ? "Writable layers"
          : "Storage";
  return (
    <SummaryStrip>
      <SummaryItem
        label={
          cpuCores !== null || ratio === null ? "CPU used" : "CPU utilization"
        }
      >
        {formatMetric(cpu, cpuUnit)}
      </SummaryItem>
      <SummaryItem
        label={workingSet !== null ? "Memory working set" : "Memory used"}
      >
        {formatMetric(memory, "bytes")}
      </SummaryItem>
      <SummaryItem label="Network receive">
        {formatMetric(
          metricAverage(values, "network_receive_bytes_per_second"),
          "bytes/s",
        )}
      </SummaryItem>
      <SummaryItem label={storageLabel}>
        {formatMetric(storage, "bytes")}
      </SummaryItem>
    </SummaryStrip>
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
      <ResourcePanel title="Containers">
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
            Container detail is unavailable; no partial list is shown.
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
