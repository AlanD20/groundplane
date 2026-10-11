import type { TaskEventResponse } from "./journal-model";
import { taskDuration } from "./task-duration";

type Readiness = NonNullable<TaskEventResponse["health_wait"]>;

const diagnosticDetails: Record<Readiness["diagnostic"], string> = {
  none: "",
  pgrep_missing: "pgrep is missing from the container image",
  curl_missing: "curl is missing from the container image",
  nc_missing: "nc is missing from the container image",
  command_missing:
    "The healthcheck command is missing from the container image",
  command_not_executable:
    "The healthcheck command cannot execute in the container",
};

export function readinessSummary(p: Readiness): string {
  const detail = diagnosticDetails[p.diagnostic];
  if (detail)
    return `${detail}. Change the image or healthcheck before deploying again.`;
  const parts = [
    `${p.ready}/${p.expected} containers ready`,
    `${p.running} running`,
  ];
  if (p.observed < p.expected)
    parts.push(`${p.expected - p.observed} not observed`);
  if (p.observed > p.expected)
    parts.push(`${p.observed - p.expected} extra containers observed`);
  if (p.starting) parts.push(`${p.starting} starting healthchecks`);
  if (p.unhealthy) parts.push(`${p.unhealthy} unhealthy`);
  if (p.exited) parts.push(`${p.exited} exited`);
  return parts.join(", ");
}

export function TaskReadinessDetails({
  value,
  receivedAt,
}: {
  value: Readiness;
  receivedAt?: string;
}) {
  return (
    <div
      className="space-y-1 text-xs"
      aria-label="Latest recorded readiness observation"
    >
      <p>{readinessSummary(value)}</p>
      <p className="text-muted-foreground">
        Observed after {taskDuration(value.elapsed_milliseconds / 1000)} of the{" "}
        {taskDuration(value.timeout_milliseconds / 1000)} readiness time limit.
      </p>
      {receivedAt && (
        <p className="text-muted-foreground">
          Controller received this update at{" "}
          <time dateTime={receivedAt}>
            {new Date(receivedAt).toLocaleTimeString()}
          </time>
          .
        </p>
      )}
    </div>
  );
}
