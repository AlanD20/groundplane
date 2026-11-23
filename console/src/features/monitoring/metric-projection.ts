import type { components } from "@/lib/api.generated";
import type {
  MonitoringChartBucketPoint,
  MonitoringChartSeries,
} from "./monitoring-chart";

export type MonitoringValues = components["schemas"]["MonitoringValues"];
export type MonitoringBucket = components["schemas"]["MonitoringBucket"];
export type MonitoringMetric = components["schemas"]["MonitoringMetric"];
export type MetricKey = keyof MonitoringValues;

type MetricDefinition = {
  key: MetricKey;
  label: string;
  unit: string;
  scale: number;
  color: string;
};

export type MetricGroup = {
  id: string;
  label: string;
  metrics: readonly MetricDefinition[];
};

const kib = 1024;
const mib = kib * kib;

const metric = (
  key: MetricKey,
  label: string,
  unit: string,
  scale: number,
  color: string,
): MetricDefinition => ({ key, label, unit, scale, color });

export const metricGroups: readonly MetricGroup[] = [
  {
    id: "cpu",
    label: "CPU",
    metrics: [
      metric("cpu_cores", "Used", "cores", 1, "var(--chart-1)"),
      metric("cpu_limit_cores", "Limit", "cores", 1, "var(--chart-5)"),
    ],
  },
  {
    id: "cpu-utilization",
    label: "CPU utilization",
    metrics: [
      metric(
        "cpu_utilization_ratio",
        "Utilization",
        "%",
        0.01,
        "var(--chart-1)",
      ),
    ],
  },
  {
    id: "memory",
    label: "Memory",
    metrics: [
      metric(
        "memory_working_set_bytes",
        "Working set",
        "MiB",
        mib,
        "var(--chart-1)",
      ),
      metric("memory_used_bytes", "Used", "MiB", mib, "var(--chart-1)"),
      metric(
        "memory_limit_bytes",
        "Workload limit",
        "MiB",
        mib,
        "var(--chart-5)",
      ),
      metric(
        "memory_total_bytes",
        "Host capacity",
        "MiB",
        mib,
        "var(--chart-5)",
      ),
    ],
  },
  {
    id: "network",
    label: "Network",
    metrics: [
      metric(
        "network_receive_bytes_per_second",
        "Receive",
        "MiB/s",
        mib,
        "var(--chart-1)",
      ),
      metric(
        "network_transmit_bytes_per_second",
        "Transmit",
        "MiB/s",
        mib,
        "var(--chart-2)",
      ),
    ],
  },
  {
    id: "storage",
    label: "Storage",
    metrics: [
      metric(
        "persistent_volume_used_bytes",
        "Persistent volumes",
        "MiB",
        mib,
        "var(--chart-1)",
      ),
      metric(
        "writable_layer_bytes",
        "Writable layers",
        "MiB",
        mib,
        "var(--chart-3)",
      ),
      metric("disk_used_bytes", "Host used", "MiB", mib, "var(--chart-1)"),
      metric("disk_total_bytes", "Host capacity", "MiB", mib, "var(--chart-5)"),
    ],
  },
  {
    id: "disk-io",
    label: "Disk I/O",
    metrics: [
      metric(
        "block_read_bytes_per_second",
        "Read",
        "MiB/s",
        mib,
        "var(--chart-1)",
      ),
      metric(
        "block_write_bytes_per_second",
        "Write",
        "MiB/s",
        mib,
        "var(--chart-2)",
      ),
    ],
  },
  {
    id: "disk-ops",
    label: "Disk operations",
    metrics: [
      metric(
        "block_read_operations_per_second",
        "Read",
        "ops/s",
        1,
        "var(--chart-1)",
      ),
      metric(
        "block_write_operations_per_second",
        "Write",
        "ops/s",
        1,
        "var(--chart-2)",
      ),
    ],
  },
];

function scaled(value: number | null | undefined, scale: number) {
  return value === null || value === undefined ? null : value / scale;
}

export function metricSeries(
  buckets: readonly MonitoringBucket[],
  group: MetricGroup,
): MonitoringChartSeries[] {
  return group.metrics
    .map((definition) => ({
      id: definition.key,
      label: definition.label,
      unit: definition.unit,
      color: definition.color,
      points: buckets.map((bucket): MonitoringChartBucketPoint => {
        const value = bucket.values[definition.key] as
          MonitoringMetric | null | undefined;
        return {
          from: Date.parse(bucket.start),
          to: Date.parse(bucket.end),
          average: scaled(value?.average, definition.scale),
          minimum: scaled(value?.minimum, definition.scale),
          maximum: scaled(value?.maximum, definition.scale),
          validSamples: value?.valid_samples ?? 0,
          expectedSamples: value?.expected_samples ?? 0,
          validContributions: value?.valid_contributions ?? 0,
          expectedContributions: value?.expected_contributions ?? 0,
        };
      }),
    }))
    .filter((series) =>
      series.points.some(
        (point) =>
          point.average !== null ||
          point.expectedSamples > 0 ||
          point.expectedContributions > 0,
      ),
    );
}

export function metricAverage(
  values: MonitoringValues,
  key: MetricKey,
): number | null {
  return values[key]?.average ?? null;
}

export function formatMetric(
  value: number | null,
  unit: "cores" | "%" | "bytes" | "bytes/s" | "ops/s",
): string {
  if (value === null || !Number.isFinite(value)) return "Unavailable";
  if (unit === "%") return `${(value * 100).toFixed(1)}%`;
  if (unit === "cores") return `${value.toFixed(2)} cores`;
  if (unit === "ops/s") return `${value.toFixed(1)} ops/s`;
  const suffix = unit === "bytes/s" ? "/s" : "";
  if (value > 0 && value < 0.1) return `<0.1 B${suffix}`;
  const [scale, symbol] =
    value >= mib ? [mib, "MiB"] : value >= kib ? [kib, "KiB"] : [1, "B"];
  const scaled = value / scale;
  return `${scaled.toFixed(scaled >= 100 || value === 0 ? 0 : 1)} ${symbol}${suffix}`;
}
