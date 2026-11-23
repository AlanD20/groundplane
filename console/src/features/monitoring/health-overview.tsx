import { StatusDot } from "@/components/common/status-badge";
import { cn } from "@/lib/utils";
import type { monitoringHealth } from "./health-summary";

export function HealthOverview({
  summary,
  embedded = false,
}: {
  summary: ReturnType<typeof monitoringHealth>;
  embedded?: boolean;
}) {
  return (
    <section
      aria-label="Runtime health summary"
      className={cn(
        "flex flex-wrap items-center justify-between gap-x-5 gap-y-2 px-4 py-3",
        !embedded && "rounded-xl border border-border bg-card",
      )}
    >
      <div className="min-w-0">
        <h2 className="flex items-center gap-2 text-sm font-semibold">
          <StatusDot status={summary.tone} className="shrink-0" />
          {summary.title}
        </h2>
        <p className="mt-1 text-xs text-muted-foreground">
          {summary.healthy} healthy · {summary.running} running without a
          healthcheck · {summary.reported}/{summary.total} reporting
        </p>
      </div>
      <dl className="flex flex-wrap gap-x-4 gap-y-1 text-xs tabular-nums text-muted-foreground">
        {(
          [
            ["Need attention", summary.attention],
            ["Starting", summary.starting],
            ["Unavailable", summary.unavailable],
          ] as const
        ).map(([label, count]) => (
          <div key={label} className="flex items-baseline gap-1.5">
            <dt>{label}</dt>
            <dd className="font-semibold text-foreground">{count}</dd>
          </div>
        ))}
      </dl>
    </section>
  );
}
