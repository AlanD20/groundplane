import { CompactReference } from "@/components/common/compact-reference";
import type { Service } from "@/lib/types";

export function ServiceMetadata({
  service,
  now,
  resourceId = service.id,
  idLabel = "Service ID",
}: {
  service: Service;
  now: number;
  resourceId?: string;
  idLabel?: string;
}) {
  const report = service.observation;
  const available = report && report.state !== "unavailable" ? report : null;
  const expired = available && Date.parse(available.expiresAt) <= now;
  const future = available && Date.parse(available.observedAt) > now;
  const time = (value?: string) =>
    value ? (
      <time dateTime={value} title={value} className="tabular-nums">
        {new Intl.DateTimeFormat(undefined, {
          dateStyle: "medium",
          timeStyle: "medium",
          timeZone: "UTC",
        }).format(new Date(value))}{" "}
        UTC
      </time>
    ) : (
      "Not reported"
    );
  return (
    <dl
      aria-label="Resource identity and Agent report"
      className="grid min-w-0 gap-4 rounded-lg border border-border bg-muted/20 p-4 text-xs sm:grid-cols-3"
    >
      <div className="min-w-0 space-y-1">
        <dt className="text-muted-foreground">{idLabel}</dt>
        <dd>
          <CompactReference value={resourceId} label={idLabel} hideLabel />
        </dd>
      </div>
      <div className="min-w-0 space-y-2 border-t border-border pt-3 sm:border-l sm:border-t-0 sm:pl-4 sm:pt-0">
        <dt className="text-muted-foreground">Last Agent report</dt>
        <dd className="[overflow-wrap:anywhere]">
          {time(available?.observedAt)}
        </dd>
      </div>
      <div className="min-w-0 space-y-2 border-t border-border pt-3 sm:border-l sm:border-t-0 sm:pl-4 sm:pt-0">
        <dt className="text-muted-foreground">Report valid until</dt>
        <dd className="[overflow-wrap:anywhere]">
          {available ? time(available.expiresAt) : "Observation unavailable"}
          {(expired || future) && (
            <span className="ml-2 text-warning">
              {expired ? "Expired" : "Not current"}
            </span>
          )}
        </dd>
      </div>
    </dl>
  );
}
