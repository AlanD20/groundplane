import { CompactReference } from "@/components/common/compact-reference";
import { workspaceSectionClassName } from "@/components/common/workspace-section";
import { Button } from "@/components/ui/button";
import { useSearchParams } from "react-router-dom";
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
  const [, setSearch] = useSearchParams();
  const destination = (tab: string, label: string) => (
    <Button
      variant="link"
      size="sm"
      onClick={() =>
        setSearch((current) => {
          const next = new URLSearchParams(current);
          next.set("tab", tab);
          return next;
        })
      }
    >
      {label}
    </Button>
  );
  const authenticationDetails = valkeyAuthenticationDetails(svc.authentication);
  return (
    <>
      {" "}
      <p className="text-sm text-muted-foreground">
        Saved configuration for this shared instance. Runtime observations are
        shown in Overview.
      </p>
      <div className="grid items-start gap-5 xl:grid-cols-2">
        <ResourcePanel
          title="Image & runtime"
          actions={destination("service", "Go to Runtime & image")}
        >
          <DetailRow
            label="Image"
            value={
              <CompactReference
                value={svc.image}
                short={svc.image.split("@")[0]}
                label="Image reference"
                hideLabel
              />
            }
          />
          <Row
            label="Adapter"
            value={
              adapter
                ? `${adapter.label} · ${adapter.key}`
                : (svc.adapter ?? "—")
            }
            mono
          />
          <Row label="Service name" value={svc.serviceName ?? svc.name} mono />
          <Row label="Runtime intent" value={svc.runtimeIntent} mono />
          <Row label="Deployment strategy" value={svc.strategy} mono />
          <Row label="Restart" value={svc.restart} mono />
          <Row label="Desired replicas" value={String(svc.replicas)} mono />
          <Row label="Memory limit" value={svc.resources.mem || "Not set"} />
          <Row
            label="CPU limit"
            value={
              Number(svc.resources.cpus) > 0
                ? `${svc.resources.cpus} cores`
                : "Not set"
            }
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
        </ResourcePanel>
        <ResourcePanel
          title="Networking"
          actions={destination("network", "Go to Networking")}
        >
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
            label="Exposed ports"
            value={svc.expose.length > 0 ? svc.expose.join(", ") : "—"}
            mono
          />
          <Row label="Aliases" value={svc.aliases.join(", ") || "—"} mono />
          <Row
            label="Dependencies"
            value={svc.dependsOn.join(", ") || "—"}
            mono
          />
          <Row
            label="Service endpoint"
            value={`${svc.serviceName ?? svc.name}${port ? `:${port}` : ""}`}
            mono
          />
        </ResourcePanel>
        <ResourcePanel
          title="Healthcheck"
          actions={destination("service", "Go to Runtime & image")}
        >
          {svc.healthcheck ? (
            <>
              <Row
                label="Check"
                value={`${svc.healthcheck.kind.toUpperCase()} ${svc.healthcheck.target}`}
                mono
              />
              <Row
                label="Interval"
                value={svc.healthcheck.interval || "Not specified"}
              />
              <Row
                label="Timeout"
                value={svc.healthcheck.timeout || "Not specified"}
              />
              <Row
                label="Start period"
                value={svc.healthcheck.startPeriod || "Not specified"}
              />
              <Row
                label="Retries"
                value={
                  svc.healthcheck.retries > 0
                    ? String(svc.healthcheck.retries)
                    : "Not specified"
                }
              />
            </>
          ) : (
            <p className="text-sm text-muted-foreground">
              No healthcheck configured.
            </p>
          )}
        </ResourcePanel>
        <ResourcePanel title="Storage">
          {svc.mounts.map((mount, index) => (
            <div
              key={index}
              className={workspaceSectionClassName(false, "space-y-2")}
            >
              <p className="break-all text-sm font-medium">
                {mount.type === "volume"
                  ? env.volumes.find((v) => v.id === mount.volume)?.slug ||
                    mount.volume
                  : mount.file}
              </p>
              <p className="w-fit max-w-full break-all rounded-md bg-muted px-2 py-1 font-mono text-sm">
                {mount.mount}
              </p>
              <p className="text-xs text-muted-foreground">
                {mount.type === "file" || mount.ro
                  ? "Read only"
                  : "Read & write"}
              </p>
            </div>
          ))}
          {!svc.mounts.length && (
            <p className="text-sm text-muted-foreground">
              No mounts configured.
            </p>
          )}
          {env.volumes.map((v) => (
            <DetailRow
              key={v.id}
              label={v.slug}
              value={
                v.path ? (
                  <CompactReference
                    value={v.path}
                    label="Volume path"
                    hideLabel
                  />
                ) : (
                  "Managed path"
                )
              }
            />
          ))}
          <DetailRow
            label="Volume folder"
            value={
              <CompactReference
                value={env.volumeDir}
                label="Volume folder"
                hideLabel
              />
            }
          />
        </ResourcePanel>
      </div>
      <ResourcePanel
        title="Variables & connection fields"
        actions={destination("connections", "Go to Backing connections")}
      >
        {" "}
        <Row
          label="Connection key prefix"
          value={svc.prefix ?? adapter?.prefix ?? "—"}
          mono
        />
        {svc.adapter === "valkey" && (
          <Row
            label="Authentication"
            value={authenticationDetails?.label ?? "Unavailable"}
            mono
          />
        )}
        <Row
          label="Environment files"
          value={svc.envFiles.join(", ") || "—"}
          mono
        />
        <h3 className="text-sm font-medium">Service variables</h3>
        {svc.environment.map((e) => (
          <Row
            key={e.key}
            label={e.key}
            value={e.value || "Empty value"}
            mono
          />
        ))}
        {!svc.environment.length && (
          <p className="text-sm text-muted-foreground">
            No inline Service variables.
          </p>
        )}
        <h3 className="text-sm font-medium">Environment variables</h3>
        {env.envVars.map((e) => (
          <Row
            key={e.key}
            label={e.key}
            value={e.value || "Empty value"}
            mono
          />
        ))}
        {!env.envVars.length && (
          <p className="text-sm text-muted-foreground">
            No inline Environment variables.
          </p>
        )}
      </ResourcePanel>{" "}
    </>
  );
}

export function Row({
  label,
  value,
  mono,
}: {
  label: string;
  value: string;
  mono?: boolean;
}) {
  return (
    <DetailRow
      label={label}
      mono={mono}
      value={
        <span className="inline-flex min-w-0 items-start gap-2">
          <span className="min-w-0 break-all">{value}</span>
        </span>
      }
    />
  );
}
