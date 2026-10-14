import { DetailRow } from "@/components/common/detail-row";
import { ResourcePanel } from "@/components/common/resource-panel";
import type { operations } from "@/lib/api.generated";

type Maintenance = NonNullable<
  operations["host.show"]["responses"][200]["content"]["application/json"]["etcd"]["maintenance"]
>;

export function StorageMaintenanceStatus({ status }: { status?: Maintenance }) {
  return (
    <ResourcePanel title="Storage maintenance">
      {!status || status.state === "waiting_for_first_pass" ? (
        <p className="text-sm text-muted-foreground">
          Waiting for the first maintenance observation.
        </p>
      ) : (
        <div className="space-y-4">
          <div className="grid gap-x-8 sm:grid-cols-2">
            {status.cleanup && (
              <>
                <DetailRow
                  label="Last cleanup pass"
                  value={`${status.cleanup.state} · ${time(status.cleanup.observed_at)}`}
                />
                <DetailRow
                  label="Removed in that pass"
                  value={`${status.cleanup.idempotency_records} replay markers, ${status.cleanup.tasks} Tasks, ${status.cleanup.diagnostics} diagnostics`}
                />
                {status.cleanup.failure_code && (
                  <DetailRow
                    label="Cleanup failure"
                    value={status.cleanup.failure_code}
                  />
                )}
              </>
            )}
            <DetailRow
              label="Maintenance"
              value={status.state.replaceAll("_", " ")}
            />
            <DetailRow
              label="Observed"
              value={new Date(status.observed_at).toLocaleString()}
            />
            <DetailRow
              label="Quota headroom"
              value={bytes(status.headroom_bytes)}
            />
            <DetailRow
              label="Database quota"
              value={bytes(status.quota_bytes)}
            />
            <DetailRow
              label="Backend file"
              value={bytes(status.backend_bytes)}
            />
            <DetailRow
              label="Backend in use"
              value={bytes(status.in_use_bytes)}
            />
            <DetailRow
              label="Reusable backend space"
              value={bytes(status.reusable_bytes)}
            />
            <DetailRow
              label="Current key/value payload"
              value={bytes(status.current_value_bytes)}
            />
            <DetailRow
              label="WAL files (separate)"
              value={bytes(status.wal_bytes)}
            />
            <DetailRow
              label="Space reclaimed by defrag"
              value={bytes(status.reclaimed_bytes)}
            />
            <DetailRow
              label="Tasks due for cleanup"
              value={String(status.task_backlog)}
            />
            <DetailRow
              label="Diagnostics due for cleanup"
              value={String(status.diagnostic_backlog)}
            />
            <DetailRow
              label="Compacted through revision"
              value={String(status.compacted_through)}
            />
            <DetailRow
              label="Last successful action"
              value={time(status.last_success_at)}
            />
            <DetailRow
              label="Last defragmentation"
              value={time(status.last_defrag_at)}
            />
          </div>
          {status.blocker && (
            <p
              role="status"
              className="rounded-lg border border-warning/40 bg-warning/10 p-3 text-sm break-words"
            >
              History before revision {status.blocker.revision} is protected by{" "}
              {status.blocker.kind.replaceAll("_", " ")} (
              {status.blocker.owner_id}). Its captured input must remain
              available until that operation settles.
            </p>
          )}
          {status.failure_code && (
            <p role="alert" className="text-sm text-destructive">
              Maintenance failed at {time(status.last_failure_at)}:{" "}
              {status.failure_code}.
            </p>
          )}
          <p className="text-xs text-muted-foreground">
            Due counts include records held by recovery or request replay.
            Configure retention and the UTC defragmentation window in Controller
            startup YAML under maintenance; saved settings take effect after a
            Controller restart. Defragmentation briefly pauses etcd requests,
            not application containers.
          </p>
        </div>
      )}
    </ResourcePanel>
  );
}

function bytes(value: number) {
  if (value < 1024) return `${value} B`;
  const unit = Math.min(3, Math.floor(Math.log(value) / Math.log(1024)));
  return `${(value / 1024 ** unit).toFixed(1)} ${["B", "KiB", "MiB", "GiB"][unit]}`;
}

function time(value?: string) {
  return value ? new Date(value).toLocaleString() : "Not recorded";
}
