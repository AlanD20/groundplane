import { StatusDot } from "@/components/common/status-badge";
import type { monitoringHealth } from "./health-summary";

export function HealthOverview({
  summary,
}: {
  summary: ReturnType<typeof monitoringHealth>;
}) {
  return (
    <section
      aria-label="Runtime health summary"
      className="relative overflow-hidden rounded-xl border border-primary/25 bg-linear-to-r from-primary/8 via-card to-card shadow-sm"
    >
      <div className="grid items-center gap-6 p-5 sm:p-6 xl:grid-cols-[minmax(0,1fr)_auto]">
        <div className="space-y-3">
          <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
            Health overview
          </p>
          <div className="flex items-start gap-3">
            <StatusDot
              status={summary.tone}
              className="mt-2 size-2.5 shrink-0 ring-4 ring-card"
            />
            <div className="min-w-0 space-y-1.5">
              <h2 className="text-xl font-semibold tracking-tight">
                {summary.title}
              </h2>
              <p className="text-xs text-muted-foreground">
                {summary.healthy} healthy · {summary.running} running without a
                healthcheck · {summary.reported}/{summary.total} reporting
              </p>
            </div>
          </div>
        </div>
        <dl className="grid grid-cols-3 divide-x divide-border rounded-lg bg-card/65 py-4 text-center xl:min-w-80">
          <div className="px-3">
            <dt className="text-xs text-muted-foreground">Need attention</dt>
            <dd className="mt-1 text-2xl font-semibold tabular-nums">
              {summary.attention}
            </dd>
          </div>
          <div className="px-3">
            <dt className="text-xs text-muted-foreground">Starting</dt>
            <dd className="mt-1 text-2xl font-semibold tabular-nums">
              {summary.starting}
            </dd>
          </div>
          <div className="px-3">
            <dt className="text-xs text-muted-foreground">Unavailable</dt>
            <dd className="mt-1 text-2xl font-semibold tabular-nums">
              {summary.unavailable}
            </dd>
          </div>
        </dl>
      </div>
    </section>
  );
}
