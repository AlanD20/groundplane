import { DetailRow } from "@/components/common/detail-row";
import { ImageReference } from "@/components/common/image-reference";
import {
  AdvancedDetails,
  ResourcePanel,
  SummaryItem,
  SummaryStrip,
} from "@/components/common/resource-panel";
import type { Environment, Service } from "@/lib/types";
import { currentServiceObservation, replicaTotal } from "./service-observation";
import { ServiceContainers } from "./service-containers";

export function ServiceOverview({
  service,
  env,
  now = Date.now(),
}: {
  service: Service;
  env: Environment;
  now?: number;
}) {
  const observation = currentServiceObservation(service.observation, now);
  const release =
    observation.state === "unavailable"
      ? undefined
      : service.releaseLedger?.find(
          (release) => release.id === observation.servingReleaseId,
        );
  const counts =
    observation.state === "unavailable"
      ? []
      : ([
          ["Healthy", observation.replicas.healthy],
          ["Running without healthcheck", observation.replicas.running],
          ["Starting", observation.replicas.starting],
          ["Unhealthy", observation.replicas.unhealthy],
          ["Changing", observation.replicas.transitional],
          ["Stopped", observation.replicas.stopped],
          ["Failed", observation.replicas.failed],
        ] as const);
  return (
    <div className="space-y-4">
      <SummaryStrip>
        <SummaryItem label="Containers">
          {observation.state === "unavailable"
            ? "Not reported"
            : `${replicaTotal(observation.replicas)} / ${observation.expectedReplicas}`}
        </SummaryItem>
        <SummaryItem label="Runtime intent">
          {service.runtimeIntent}
        </SummaryItem>
        <SummaryItem label="Memory limit">
          {service.resources.mem || "Not set"}
        </SummaryItem>
        <SummaryItem label="CPU limit">
          {service.resources.cpus || "Not set"}
        </SummaryItem>
      </SummaryStrip>
      {observation.state === "unavailable" ? (
        <p
          role="status"
          className="rounded-lg border border-border p-3 text-xs text-muted-foreground"
        >
          {service.releaseLedger?.length === 0
            ? "Not deployed yet. Choose an available image, then Deploy."
            : "No current container report. Runtime state cannot be confirmed."}
        </p>
      ) : (
        <div className="flex flex-wrap gap-3 text-xs">
          {counts
            .filter(([, count]) => count > 0)
            .map(([label, count]) => (
              <span key={label}>
                {count} {label.toLowerCase()}
              </span>
            ))}
          {replicaTotal(observation.replicas) === 0 && (
            <span className="text-muted-foreground">
              No containers observed
            </span>
          )}
        </div>
      )}
      <ServiceContainers observation={observation} />
      <ResourcePanel
        title={service.adapter ? "Runtime configuration" : "Deployment"}
      >
        <DetailRow
          label="Configured image"
          value={<ImageReference value={service.image} />}
        />
        {!service.adapter && (
          <DetailRow
            label="Serving Release"
            value={
              release ? (
                <ImageReference
                  value={release.tag || release.digest || release.id}
                />
              ) : (
                "Not reported"
              )
            }
          />
        )}
        <DetailRow label="Strategy" value={service.strategy} />
        <DetailRow
          label="Healthcheck"
          value={
            service.healthcheck
              ? `${service.healthcheck.kind}: ${service.healthcheck.target}`
              : "Not configured"
          }
        />
      </ResourcePanel>
      <ResourcePanel title="Connectivity & storage">
        <DetailRow
          label="Zones"
          value={
            service.zones
              .map(
                (ref) =>
                  env.zones.find((zone) => zone.id === ref || zone.name === ref)
                    ?.name ?? ref,
              )
              .join(", ") || "None configured"
          }
        />
        <DetailRow
          label="Exposed ports"
          value={service.expose.join(", ") || "None"}
        />
        <DetailRow
          label="Aliases"
          value={service.aliases.join(", ") || "None"}
        />
        <DetailRow
          label="Mounts"
          value={
            service.mounts
              .map(
                (mount) =>
                  `${mount.type === "volume" ? (env.volumes.find((volume) => volume.id === mount.volume || volume.slug === mount.volume)?.slug ?? mount.volume) : mount.file} → ${mount.mount}`,
              )
              .join(", ") || "None"
          }
        />
        <DetailRow
          label="Routes"
          value={
            env.routes
              .filter((route) => route.targetServiceId === service.id)
              .map((route) => `${route.host}${route.path}`)
              .join(", ") || "None"
          }
        />
      </ResourcePanel>
      <AdvancedDetails>
        <DetailRow label="Service ID" value={service.id} mono />
        {observation.state !== "unavailable" && (
          <>
            <DetailRow
              label="Reported"
              value={new Date(observation.observedAt).toLocaleString()}
            />
            <DetailRow
              label="Report expires"
              value={new Date(observation.expiresAt).toLocaleString()}
            />
            {observation.servingReleaseId && (
              <DetailRow
                label="Serving Release ID"
                value={observation.servingReleaseId}
                mono
              />
            )}
          </>
        )}
        <p className="text-muted-foreground">
          Container health does not confirm application or Route reachability.
        </p>
      </AdvancedDetails>
    </div>
  );
}
