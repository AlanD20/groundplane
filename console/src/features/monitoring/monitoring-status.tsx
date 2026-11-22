import { StatusBadge } from "@/components/common/status-badge";

type MonitoringLive = NonNullable<
  import("./api").MonitoringResponse["host"]
>["live"];

function monitoringStatus(live: MonitoringLive, now: number) {
  if (live.status !== "fresh") return live.status;
  const observedAt = live.observed_at
    ? Date.parse(live.observed_at)
    : Number.NaN;
  if (!Number.isFinite(observedAt) || observedAt > now) return "unavailable";
  return now - observedAt < 15_000 ? "fresh" : "stale";
}

export function MonitoringStatusBadge({
  live,
  now,
}: {
  live: MonitoringLive;
  now: number;
}) {
  const status = monitoringStatus(live, now);
  return (
    <StatusBadge
      status={
        status === "fresh"
          ? "healthy"
          : status === "stale"
            ? "degraded"
            : "unavailable"
      }
      label={
        status === "fresh" ? "Live" : status === "stale" ? "Stale" : "No data"
      }
    />
  );
}
