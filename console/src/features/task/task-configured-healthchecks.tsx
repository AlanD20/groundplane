import { Disclosure } from "@/components/common/disclosure";
import type { TaskStep } from "@/lib/types";

type ConfiguredCheck = NonNullable<TaskStep["healthchecks"]>[number];

const checkNames: Record<ConfiguredCheck["kind"], string> = {
  http: "HTTP (curl)",
  tcp: "TCP (nc)",
  process: "Process (pgrep)",
  custom_exec: "Custom command",
  custom_shell: "Custom shell command",
  disabled: "Disabled",
  image_default: "No authored check; image defaults unchanged",
  unavailable: "Check details unavailable",
};

function checkLabel(check: ConfiguredCheck): string {
  const name = checkNames[check.kind];
  return check.loopback_port
    ? `${name} · loopback port ${check.loopback_port}${check.http_protocol ? ` (${check.http_protocol.toUpperCase()})` : ""}`
    : name;
}

export function TaskConfiguredHealthchecks({
  checks,
}: {
  checks?: TaskStep["healthchecks"];
}) {
  if (!checks?.length) return null;
  return (
    <Disclosure
      className="min-w-0 rounded-lg border border-border bg-surface/40 text-xs"
      summaryClassName="min-w-0 px-3 py-2"
      summary={
        <span className="min-w-0 break-words [overflow-wrap:anywhere]">
          <span className="font-medium">Configured readiness: </span>
          {checks.length === 1
            ? checkLabel(checks[0])
            : `${checks.length} workloads`}
        </span>
      }
    >
      <div className="space-y-3 border-t border-border px-3 py-3">
        <p className="text-muted-foreground">
          Captured when this Task was created. Commands, process patterns and URL
          paths are omitted; these are not today's Service settings.
        </p>
        {checks.map((check) => (
          <div key={check.workload} className="min-w-0 space-y-2">
            <div className="space-y-1">
              <p className="break-words font-medium [overflow-wrap:anywhere]">
                {check.workload}
              </p>
              <p>{checkLabel(check)}</p>
              <p className="text-muted-foreground">
                {check.expected_replicas} expected{" "}
                {check.expected_replicas === 1 ? "replica" : "replicas"}.{" "}
                {check.requires_healthy
                  ? "Every replica must be running and healthy."
                  : "Running is sufficient; GP does not require a healthy result."}
              </p>
            </div>
            {check.kind !== "unavailable" && (
              <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-muted-foreground">
                <CheckDatum label="Interval" value={check.interval} />
                <CheckDatum label="Check timeout" value={check.timeout} />
                <CheckDatum label="Startup grace" value={check.start_period} />
                <CheckDatum label="Startup interval" value={check.start_interval} />
                <CheckDatum label="Retries" value={check.retries?.toString()} />
              </dl>
            )}
          </div>
        ))}
      </div>
    </Disclosure>
  );
}

function CheckDatum({ label, value }: { label: string; value?: string }) {
  return (
    <div className="min-w-0">
      <dt>{label}</dt>
      <dd className="mt-0.5 break-words text-foreground [overflow-wrap:anywhere]">
        {value ?? "No authored override"}
      </dd>
    </div>
  );
}
