import { CopyButton } from "@/components/common/copy-button";
import { DetailRow } from "@/components/common/detail-row";
import { ResourcePanel } from "@/components/common/resource-panel";
import { useStore } from "@/lib/store";
import type { Project } from "@/lib/types";
import { valkeyAuthenticationDetails } from "@/lib/valkey-authentication";

// ---- Service (the full spec, like any service) ----

export function ServiceTab({
  env,
  svc,
}: {
  env: NonNullable<Project["environments"]>[number];
  svc: NonNullable<Project["environments"]>[number]["services"][number];
}) {
  const store = useStore();
  const adapter = store.adapters.find((a) => a.key === svc.adapter);
  const port =
    adapter?.urlScheme === "redis"
      ? 6379
      : adapter?.urlScheme === "pgsql"
        ? 5432
        : undefined;
  const volume = env.volumes[0];
  const authenticationDetails = valkeyAuthenticationDetails(svc.authentication);
  return (
    <>
      <ResourcePanel title="Service configuration">
        <div className="grid gap-x-8 gap-y-1.5 sm:grid-cols-2">
          <Row label="Image" value={svc.image} mono />
          <Row
            label="Adapter"
            value={
              adapter
                ? `${adapter.label} · ${adapter.key}`
                : (svc.adapter ?? "—")
            }
            mono
          />
          {svc.adapter === "custom" && (
            <Row
              label="Hooks"
              value={
                (["attach", "detach", "before_stop", "after_start"] as const)
                  .filter((event) => svc.hooks?.[event])
                  .map((event) => event.replace("_", "-"))
                  .join(", ") || "none (network-only)"
              }
              mono
            />
          )}
          <Row label="Service name" value={svc.serviceName ?? svc.name} mono />
          <Row
            label="Prefix (facts keys)"
            value={svc.prefix ?? adapter?.prefix ?? "—"}
            mono
          />
          {svc.adapter === "valkey:9" && (
            <Row
              label="Authentication"
              value={authenticationDetails?.label ?? "Unavailable"}
              mono
            />
          )}
          <Row label="Runtime intent" value={svc.runtimeIntent} mono />
          <Row label="Strategy" value={svc.strategy} mono />
          <Row
            label="Network"
            value={
              env.zones
                .map(
                  (z) =>
                    `${z.name} · ${z.subnet}${z.internal ? " · internal" : ""}`,
                )
                .join(", ") || "—"
            }
            mono
          />
          <Row
            label="Healthcheck"
            value={
              svc.healthcheck
                ? `${svc.healthcheck.kind} ${svc.healthcheck.target} · every ${svc.healthcheck.interval} · timeout ${svc.healthcheck.timeout} · start ${svc.healthcheck.startPeriod} · retries ${svc.healthcheck.retries}`
                : "none"
            }
            mono
          />
          <Row
            label="Resources"
            value={`${svc.resources.mem} · ${svc.resources.cpus} cpu`}
            mono
          />
          <Row
            label="Expose"
            value={svc.expose.length > 0 ? svc.expose.join(", ") : "—"}
            mono
          />
          <Row
            label="Mounts"
            value={
              svc.mounts
                .map((m) =>
                  m.type === "volume"
                    ? `${m.volume} → ${m.mount}`
                    : `${m.file} → ${m.mount}${m.ro ? " (ro)" : ""}`,
                )
                .join(", ") || "—"
            }
            mono
          />
          <Row label="Env files" value={svc.envFiles.join(", ") || "—"} mono />
          <Row
            label="Env vars"
            value={
              svc.environment.map((e) => `${e.key}=${e.value}`).join(", ") ||
              "—"
            }
            mono
          />
          <Row label="Aliases" value={svc.aliases.join(", ") || "—"} mono />
          <Row
            label="Depends on"
            value={svc.dependsOn.join(", ") || "—"}
            mono
          />
          <Row label="Restart" value={svc.restart} mono />
          <Row label="Desired replicas" value={String(svc.replicas)} mono />
          <Row
            label="Service endpoint"
            value={`${svc.serviceName ?? svc.name}${port ? `:${port}` : ""}`}
            mono
          />
        </div>
      </ResourcePanel>

      <ResourcePanel title="Environment configuration">
        <div className="grid gap-x-8 gap-y-1.5 sm:grid-cols-2">
          <Row
            label="Volume"
            value={
              volume ? `${volume.slug} → ${volume.path ?? "managed path"}` : "—"
            }
            mono
          />
          <Row label="Volume dir" value={env.volumeDir} mono />
          <Row
            label="Env vars"
            value={
              env.envVars.map((e) => `${e.key}=${e.value}`).join(", ") || "—"
            }
            mono
          />
        </div>
      </ResourcePanel>
    </>
  );
}

export function Row({
  label,
  value,
  mono,
  masked,
}: {
  label: string;
  value: string;
  mono?: boolean;
  masked?: boolean;
}) {
  return (
    <DetailRow
      label={label}
      mono={mono}
      value={
        <span className="inline-flex min-w-0 items-start gap-2">
          <span className="min-w-0 break-all">{value}</span>
          {masked && <CopyButton value={value} />}
        </span>
      }
    />
  );
}
