import { EmptyState } from "@/components/common/empty-state";
import {
  ResourcePanel,
  SummaryItem,
  SummaryStrip,
} from "@/components/common/resource-panel";
import { useWorkspaceFullscreen } from "@/components/common/use-workspace-fullscreen";
import { Button } from "@/components/ui/button";
import { Select } from "@/components/ui/select";
import {
  readChartTimeZone,
  saveChartTimeZone,
} from "@/lib/console-preferences";
import { formatTimestamp } from "@/lib/format-timestamp";
import { Maximize, Minimize, Pause, Play } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import type { MonitoringRange, MonitoringResponse } from "./api";
import {
  formatMetric,
  byteUnits,
  type ByteUnit,
  metricAverage,
  metricGroups,
  metricSeries,
  type MonitoringValues,
} from "./metric-projection";
import { MonitoringChart } from "./monitoring-chart";
import { MonitoringStatusBadge } from "./monitoring-status";
import type { useMonitoringQuery } from "./use-monitoring-query";

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
  const [byteUnit, setByteUnit] = useState<ByteUnit>("Auto");
  const fullscreenButton = useRef<HTMLButtonElement>(null);
  const [fullscreen, setFullscreen] = useWorkspaceFullscreen(undefined, () =>
    fullscreenButton.current?.focus({ preventScroll: true }),
  );
  const selected =
    metricGroups.find((group) => group.id === metric) ?? metricGroups[0];
  const canSelectUnits = selected.metrics[0]?.unit.startsWith("MiB");

  const subject =
    response.host ??
    response.tenant ??
    response.project ??
    response.environment ??
    response.service ??
    response.backing?.runtime;
  const historyUnavailable = response.history_status !== "available";
  const scopeKey = `${response.scope.kind}/${response.scope.id}`;
  const historyKey = `${scopeKey}/${response.window.range}`;
  const availableHistory = historyUnavailable ? undefined : subject?.history;
  const [lastHistory, setLastHistory] = useState<{
    key: string;
    buckets: NonNullable<typeof availableHistory>;
    window: MonitoringResponse["window"];
  }>();
  useEffect(() => {
    if (availableHistory) {
      setLastHistory({
        key: historyKey,
        buckets: availableHistory,
        window: response.window,
      });
    }
  }, [availableHistory, historyKey, response.window]);
  const retainedHistory =
    historyUnavailable && lastHistory?.key === historyKey
      ? lastHistory
      : undefined;
  const history = retainedHistory?.buckets ?? availableHistory;
  const series = useMemo(
    () => metricSeries(history ?? [], selected, byteUnit),
    [history, selected, byteUnit],
  );
  const { start, end } = retainedHistory?.window ?? response.window;
  const chartWindow = useMemo(
    () => ({ from: Date.parse(start), to: Date.parse(end) }),
    [start, end],
  );
  const timeZone = useMemo(
    () =>
      chartTimeZone === "local"
        ? Intl.DateTimeFormat().resolvedOptions().timeZone
        : "UTC",
    [chartTimeZone],
  );
  if (!subject?.live)
    return (
      <EmptyState
        title="Resource usage unavailable"
        description="The Controller returned no resource measurements for this view."
      />
    );

  const live = subject.live;
  const owners = live.coverage.eligible_resources;
  const reporting = live.coverage.sampled_resources;
  const refreshError = query.pageMatches ? query.error : undefined;
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
        {refreshError && (
          <p role="alert" className="mt-3 text-xs text-warning">
            Showing the last response. Refresh failed: {refreshError}
          </p>
        )}
      </section>

      <LiveSummary values={live.status === "unavailable" ? {} : live.values} />

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
                options={metricGroups
                  .filter(
                    (group) =>
                      group.id !== "cpu-utilization" || !!response.host,
                  )
                  .map((group) => ({
                    value: group.id,
                    label: group.label,
                  }))}
              />
              {canSelectUnits && (
                <Select
                  aria-label="Chart units"
                  className="w-24"
                  value={byteUnit}
                  onValueChange={(value) => setByteUnit(value as ByteUnit)}
                  options={byteUnits.map((unit) => ({
                    value: unit,
                    label: unit,
                  }))}
                />
              )}
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
          {fullscreen && refreshError && (
            <p role="alert" className="text-xs text-warning">
              {refreshError}
            </p>
          )}
          {selected.description && (
            <p className="text-xs text-muted-foreground">
              {selected.description}
            </p>
          )}
          <MonitoringChart
            scopeKey={`${scopeKey}/${selected.id}/${byteUnit}`}
            scopedLabel={response.scope.name}
            rangeLabel={response.window.range}
            window={chartWindow}
            series={series}
            timeZone={timeZone}
            height={
              fullscreen
                ? "max(280px, calc(100dvh - 13rem - var(--terminal-dock-height, 0px)))"
                : undefined
            }
          />
          {historyUnavailable && (
            <p role="status" className="text-xs text-warning">
              {retainedHistory?.buckets.length
                ? "Historical storage is unavailable. Showing the last available history."
                : "Historical storage is unavailable."}
            </p>
          )}
        </ResourcePanel>
      </div>
    </div>
  );
}

function LiveSummary({ values }: { values: MonitoringValues }) {
  const cpuCores = metricAverage(values, "cpu_cores");
  const ratio = metricAverage(values, "cpu_utilization_ratio");
  const cpu = cpuCores ?? ratio;
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
