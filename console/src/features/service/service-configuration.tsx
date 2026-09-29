import { DetailRow } from "@/components/common/detail-row";
import { ImageReference } from "@/components/common/image-reference";
import { CodeEditor } from "@/components/ui/code-editor";
import type { Service } from "@/lib/types";

export function ServiceConfiguration({
  service,
  loading,
  error,
}: {
  service: Service;
  loading: boolean;
  error?: string;
}) {
  return (
    <div className="flex min-w-0 flex-col gap-5">
      <p className="text-xs text-muted-foreground">
        Desired configuration. Saving does not deploy it or change the serving
        Release.
      </p>
      <div className="flex flex-col gap-1.5 text-sm">
        <DetailRow
          label="Image"
          value={<ImageReference value={service.image} />}
        />
        <DetailRow label="Note" value={service.role} />
        <DetailRow label="Zones" value={service.zones.join(", ") || "—"} mono />
        <DetailRow
          label="Strategy"
          value={`${service.strategy}${service.strategy === "rolling" ? " (deferred)" : ""}`}
        />
        <DetailRow
          label="Env files"
          value={service.envFiles.join(", ") || "—"}
          mono
        />
        <DetailRow
          label="Healthcheck"
          value={
            service.healthcheck
              ? service.healthcheck.kind === "http"
                ? `GET ${service.healthcheck.target} · every ${service.healthcheck.interval} · timeout ${service.healthcheck.timeout} · start ${service.healthcheck.startPeriod} · retries ${service.healthcheck.retries}`
                : service.healthcheck.kind === "tcp"
                  ? `TCP ${service.healthcheck.target} · every ${service.healthcheck.interval} · timeout ${service.healthcheck.timeout} · start ${service.healthcheck.startPeriod} · retries ${service.healthcheck.retries}`
                  : `pgrep '${service.healthcheck.target}' · every ${service.healthcheck.interval} · timeout ${service.healthcheck.timeout} · start ${service.healthcheck.startPeriod} · retries ${service.healthcheck.retries}`
              : "none"
          }
          mono
        />
        <DetailRow
          label="Resources"
          value={`${service.resources.mem} · ${service.resources.cpus} cpu`}
          mono
        />
        <DetailRow
          label="Environment"
          value={
            service.environment.map((e) => `${e.key}=${e.value}`).join(", ") ||
            "—"
          }
          mono
        />
        <DetailRow
          label="Mounts"
          value={
            service.mounts
              .map((m) =>
                m.type === "volume"
                  ? `${m.volume} → ${m.mount}`
                  : `${m.file} → ${m.mount} :ro`,
              )
              .join(", ") || "—"
          }
          mono
        />
        {service.command && (
          <DetailRow label="Command" value={service.command} mono />
        )}
        {service.aliases.length > 0 && (
          <DetailRow label="Aliases" value={service.aliases.join(", ")} mono />
        )}
        {service.dependsOn.length > 0 && (
          <DetailRow
            label="Depends on"
            value={service.dependsOn
              .map((d) => `${d} (service_healthy)`)
              .join(", ")}
            mono
          />
        )}
        <DetailRow
          label="Expose"
          value={service.expose.join(", ") || "—"}
          mono
        />
        <DetailRow label="Restart" value={service.restart} mono />
        <DetailRow
          label="Desired replicas"
          value={String(service.replicas)}
          mono
        />
        <DetailRow label="Runtime intent" value={service.runtimeIntent} mono />
        <DetailRow
          label="Labels"
          value={`com.groundplane.managed=true · com.groundplane.service-id=${service.id}`}
          mono
        />
      </div>

      {service.nativeCompose ? (
        <CodeEditor
          id={`service-compose-${service.id}`}
          label="Native Compose desired state"
          value={service.nativeCompose}
          language="yaml"
          readOnly
        />
      ) : loading ? (
        <p role="status" className="text-xs text-muted-foreground">
          Loading native Compose…
        </p>
      ) : (
        !error && (
          <p className="text-xs text-muted-foreground">
            No native Compose is available.
          </p>
        )
      )}
    </div>
  );
}
