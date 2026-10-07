import { DetailRow } from "@/components/common/detail-row";
import { ImageReference } from "@/components/common/image-reference";
import {
  ResourcePanel,
  AdvancedDetails,
} from "@/components/common/resource-panel";
import { CopyButton } from "@/components/common/copy-button";
import { CodeEditor } from "@/components/ui/code-editor";
import { Button } from "@/components/ui/button";
import type { Environment, Service } from "@/lib/types";
import {
  serviceDestinations,
  type ServiceDestination,
} from "./workspace-navigation";

export function ServiceConfiguration({
  service,
  env,
  loading,
  error,
  onNavigate,
}: {
  service: Service;
  env?: Environment;
  loading: boolean;
  error?: string;
  onNavigate?: (destination: ServiceDestination) => void;
}) {
  const edit = (destination: ServiceDestination) =>
    onNavigate && (
      <Button size="sm" variant="link" onClick={() => onNavigate(destination)}>
        {destination === "storage"
          ? "Manage mounts"
          : `Go to ${serviceDestinations.find((item) => item.key === destination)?.label}`}
      </Button>
    );
  const zones = service.zones.map(
    (id) =>
      env?.zones.find((zone) => zone.id === id || zone.name === id)?.name ?? id,
  );
  return (
    <div className="min-w-0 space-y-5">
      <div className="rounded-xl border border-primary/20 bg-primary/5 p-4">
        <h2 className="text-lg font-semibold">Desired configuration</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Saved settings for the next deployment. These may differ from the
          currently running containers.
        </p>
      </div>
      <div className="grid gap-5 xl:grid-cols-2">
        <ResourcePanel title="Image & runtime" actions={edit("configuration")}>
          <DetailRow
            label="Image"
            value={<ImageReference value={service.image} />}
          />
          <DetailRow
            label="Command"
            value={
              service.command.length > 0
                ? JSON.stringify(service.command)
                : "Image default"
            }
            mono
          />
          <DetailRow
            label="Entrypoint"
            value={
              service.entrypoint.length > 0
                ? JSON.stringify(service.entrypoint)
                : "Image default"
            }
            mono
          />
          <DetailRow
            label="Working directory"
            value={service.workingDir || "Image default"}
            mono
          />
          <DetailRow
            label="Container user"
            value={service.user || "Image default"}
            mono
          />
          <DetailRow label="Replicas" value={String(service.replicas)} />
          <DetailRow
            label="Memory limit"
            value={service.resources.mem || "Not set"}
          />
          <DetailRow
            label="CPU limit"
            value={service.resources.cpus || "Not set"}
          />
          <DetailRow label="Restart policy" value={service.restart} />
          <DetailRow
            label="Deployment strategy"
            value={`${service.strategy}${service.strategy === "rolling" ? " (deferred)" : ""}`}
          />
          <DetailRow label="Runtime intent" value={service.runtimeIntent} />
          <DetailRow
            label="Log rotation"
            value={
              service.logging.maxSize || service.logging.maxFile > 0
                ? `${service.logging.maxSize || "default size"} · ${service.logging.maxFile > 0 ? `${service.logging.maxFile} files` : "default files"}`
                : "Runtime default"
            }
          />
          {service.role && <DetailRow label="Note" value={service.role} />}
        </ResourcePanel>
        <ResourcePanel title="Networking" actions={edit("network")}>
          <DetailRow label="Zones" value={zones.join(", ") || "None"} />
          <DetailRow
            label="Exposed ports"
            value={service.expose.join(", ") || "None"}
            mono
          />
          <DetailRow
            label="Aliases"
            value={
              Object.entries(service.aliasesByZone)
                .map(([zone, aliases]) => `${zone}: ${aliases.join(", ")}`)
                .join(" · ") || "None"
            }
          />
          <DetailRow
            label="Dependencies"
            value={
              Object.entries(service.dependencies)
                .map(
                  ([name, dependency]) =>
                    `${name} (${dependency.condition.replaceAll("_", " ")})`,
                )
                .join(", ") || "None"
            }
          />
        </ResourcePanel>
        <ResourcePanel title="Healthcheck" actions={edit("configuration")}>
          {service.healthcheck ? (
            <>
              <DetailRow
                label="Check"
                value={`${service.healthcheck.kind.toUpperCase()} ${service.healthcheck.target}`}
                mono
              />
              <DetailRow
                label="Interval"
                value={service.healthcheck.interval}
              />
              <DetailRow label="Timeout" value={service.healthcheck.timeout} />
              <DetailRow
                label="Start period"
                value={service.healthcheck.startPeriod}
              />
              <DetailRow
                label="Retries"
                value={String(service.healthcheck.retries)}
              />
            </>
          ) : (
            <p className="text-sm text-muted-foreground">
              No healthcheck configured.
            </p>
          )}
        </ResourcePanel>
        <ResourcePanel title="Mounts" actions={edit("storage")}>
          {service.mounts.length ? (
            service.mounts.map((mount, index) => (
              <div
                key={index}
                className="space-y-1 rounded-lg border border-border p-3"
              >
                <p className="break-all text-sm font-medium">
                  {mount.type === "volume"
                    ? (env?.volumes.find((volume) => volume.id === mount.volume)
                        ?.slug ?? mount.volume)
                    : mount.file}
                </p>
                <p className="break-all font-mono text-sm">→ {mount.mount}</p>
                <p className="text-xs text-muted-foreground">
                  {mount.type === "file" || mount.ro
                    ? "Read only"
                    : "Read & write"}
                </p>
              </div>
            ))
          ) : (
            <p className="text-sm text-muted-foreground">
              No mounts configured.
            </p>
          )}
        </ResourcePanel>
      </div>
      <ResourcePanel
        title="Variables & environment files"
        actions={edit("entries")}
      >
        {service.environment.map((entry) => (
          <DetailRow
            key={entry.key}
            label={entry.key}
            value={entry.value === "" ? "Empty value" : entry.value}
            mono
          />
        ))}
        {!service.environment.length && (
          <p className="text-sm text-muted-foreground">
            No inline environment values. Managed Entries are listed in
            Variables & files.
          </p>
        )}
        <DetailRow
          label="Environment files"
          value={service.envFiles.join(", ") || "None"}
          mono
        />
      </ResourcePanel>
      <AdvancedDetails title="Generated Compose & managed labels">
        <DetailRow label="Managed" value="com.groundplane.managed=true" mono />
        <DetailRow
          label="Service label"
          value={`com.groundplane.service-id=${service.id}`}
          mono
        />
        {service.nativeCompose ? (
          <>
            <div className="flex justify-end">
              <CopyButton value={service.nativeCompose} label="Copy Compose" />
            </div>
            <CodeEditor
              id={`service-compose-${service.id}`}
              label="Generated desired Compose"
              value={service.nativeCompose}
              language="yaml"
              readOnly
            />
          </>
        ) : (
          <p role="status" className="text-sm text-muted-foreground">
            {loading
              ? "Loading generated Compose…"
              : (error ?? "No generated Compose available.")}
          </p>
        )}
      </AdvancedDetails>
    </div>
  );
}
