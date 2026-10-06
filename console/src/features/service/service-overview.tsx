import { DetailRow } from "@/components/common/detail-row";
import { ImageReference } from "@/components/common/image-reference";
import {
  AdvancedDetails,
  ResourcePanel,
} from "@/components/common/resource-panel";
import { Button } from "@/components/ui/button";
import { formatTimestamp } from "@/lib/format-timestamp";
import { useStore } from "@/lib/store";
import type { Environment, Service } from "@/lib/types";
import {
  Boxes,
  ChevronRight,
  Database,
  HardDrive,
  Network,
  Route,
} from "lucide-react";
import type { ReactNode } from "react";
import { Link } from "react-router-dom";
import { currentServiceObservation, replicaTotal } from "./service-observation";
import { ServiceContainers } from "./service-containers";

type ConnectedResource = {
  key: string;
  icon: ReactNode;
  name: string;
  kind: string;
  href?: string;
};

export function ServiceOverview({
  service,
  env,
  now = Date.now(),
  onOpenLogs,
  onOpenDeployments,
}: {
  service: Service;
  env: Environment;
  now?: number;
  onOpenLogs?: () => void;
  onOpenDeployments?: () => void;
}) {
  const store = useStore();
  const observation = currentServiceObservation(service.observation, now);
  const releases = service.releaseLedger ?? [];
  const servingRelease =
    observation.state === "unavailable"
      ? undefined
      : releases.find((release) => release.id === observation.servingReleaseId);
  const latestRelease = releases.reduce<(typeof releases)[number] | undefined>(
    (latest, release) =>
      !latest || Date.parse(release.when) > Date.parse(latest.when)
        ? release
        : latest,
    undefined,
  );
  const presentedRelease =
    servingRelease ??
    releases.find((release) => release.status === "active") ??
    latestRelease;
  const totalContainers =
    observation.state === "unavailable"
      ? undefined
      : replicaTotal(observation.replicas);
  const zoneNames = service.zones.map(
    (ref) =>
      env.zones.find((zone) => zone.id === ref || zone.name === ref)?.name ??
      ref,
  );
  const connected: ConnectedResource[] = [
    ...zoneNames.map((name) => ({
      key: `zone-${name}`,
      icon: <Network />,
      name,
      kind: "Network Zone",
      href: "?view=network&panel=zones",
    })),
    ...env.attaches
      .filter(
        (attach) =>
          attach.serviceId === service.id || attach.service === service.name,
      )
      .map((attach) => ({
        key: `attach-${attach.id}`,
        icon: <Database />,
        name: attach.name,
        kind:
          store.getBackingProject(attach.projectId)?.name ??
          "Backing connection",
        href: `/platform/backing-services/${attach.projectId}`,
      })),
    ...service.mounts.map((mount, index) => ({
      key: `mount-${index}-${mount.mount}`,
      icon: <HardDrive />,
      name:
        mount.type === "volume"
          ? (env.volumes.find(
              (volume) =>
                volume.id === mount.volume || volume.slug === mount.volume,
            )?.slug ?? mount.volume)
          : mount.file,
      kind: `${mount.type === "volume" ? "Persistent Volume" : "File"} · ${mount.mount}`,
      href:
        mount.type === "volume"
          ? "?view=configuration&panel=volumes"
          : "?view=configuration&panel=entries",
    })),
    ...env.routes
      .filter((route) => route.targetServiceId === service.id)
      .map((route) => ({
        key: `route-${route.id}`,
        icon: <Route />,
        name: `${route.host}${route.path}`,
        kind: "HTTP Route",
        href: "?view=network&panel=routes",
      })),
  ];
  const healthcheckSummary = !service.healthcheck
    ? "Not configured"
    : observation.state === "healthy"
      ? "Passing"
      : observation.state === "unavailable"
        ? "Not reported"
        : observation.state === "stopped" || observation.state === "absent"
          ? "Not running"
          : observation.state;

  return (
    <div className="space-y-5">
      <section className="flex min-w-0 flex-col gap-4 rounded-xl border border-border bg-card p-5 sm:flex-row sm:items-center">
        <div className="flex size-12 shrink-0 items-center justify-center rounded-xl bg-accent text-primary">
          <Boxes className="size-5" />
        </div>
        <div className="min-w-0 flex-1">
          <p className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
            {servingRelease
              ? "Serving deployment"
              : presentedRelease
                ? "Last deployment"
                : "Configured image"}
          </p>
          <h2 className="mt-1 min-w-0 font-semibold [&_span]:text-base">
            <ImageReference
              value={
                presentedRelease?.tag ||
                presentedRelease?.digest ||
                service.image
              }
            />
          </h2>
          <p className="mt-1 text-xs text-muted-foreground">
            {presentedRelease
              ? `${presentedRelease.status === "active" ? "Active Release" : "Latest Release"} · ${formatTimestamp(presentedRelease.when, "time unavailable")}`
              : "No serving Release has been reported."}
          </p>
        </div>
        {presentedRelease && (
          <span className="shrink-0 rounded-full bg-accent px-2.5 py-1 text-[10px] font-medium text-primary">
            {presentedRelease.strategy}
          </span>
        )}
      </section>

      <div className="flex flex-wrap items-center gap-x-6 gap-y-2 border-b border-border pb-5 text-sm">
        <span>
          <span className="text-muted-foreground">Containers </span>
          {observation.state === "unavailable"
            ? "Not reported"
            : `${totalContainers} / ${observation.expectedReplicas}`}
        </span>
        <span>
          <span className="text-muted-foreground">Healthcheck </span>
          {healthcheckSummary}
        </span>
        {onOpenLogs && (
          <Button variant="outline" size="sm" onClick={onOpenLogs}>
            View logs
          </Button>
        )}
        {onOpenDeployments && (
          <Button variant="ghost" size="sm" onClick={onOpenDeployments}>
            Deployment history
          </Button>
        )}
      </div>
      <div className="space-y-5">
        <ResourcePanel title="Connected resources">
          {connected.length ? (
            <div className="divide-y divide-border rounded-xl border border-border">
              {connected.map((resource) => {
                const row = (
                  <>
                    <span className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-accent text-primary [&_svg]:size-4">
                      {resource.icon}
                    </span>
                    <span className="min-w-0 flex-1">
                      <span className="block truncate text-sm font-medium">
                        {resource.name}
                      </span>
                      <span className="mt-0.5 block truncate text-[11px] text-muted-foreground">
                        {resource.kind}
                      </span>
                    </span>
                    {resource.href && (
                      <ChevronRight className="size-4 shrink-0 text-muted-foreground" />
                    )}
                  </>
                );
                return resource.href ? (
                  <Link
                    key={resource.key}
                    to={resource.href}
                    className="flex min-w-0 items-center gap-3 p-3 transition-colors hover:bg-muted focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-ring"
                  >
                    {row}
                  </Link>
                ) : (
                  <div
                    key={resource.key}
                    className="flex min-w-0 items-center gap-3 p-3"
                  >
                    {row}
                  </div>
                );
              })}
            </div>
          ) : (
            <p role="status" className="text-sm text-muted-foreground">
              No Zones, backing connections, mounts or Routes are configured.
            </p>
          )}
        </ResourcePanel>
      </div>

      <ServiceContainers observation={observation} onOpenLogs={onOpenLogs} />
      <AdvancedDetails title="Agent report & Service ID">
        <DetailRow label="Service ID" value={service.id} mono />
        <DetailRow
          label="Configured image"
          value={<ImageReference value={service.image} />}
        />
        <DetailRow label="Strategy" value={service.strategy} />
        <DetailRow
          label="Memory / CPU limit"
          value={`${service.resources.mem || "Not set"} · ${service.resources.cpus || "Not set"}`}
        />
        {observation.state !== "unavailable" && (
          <>
            <DetailRow
              label="Reported"
              value={formatTimestamp(observation.observedAt, "Not reported")}
            />
            <DetailRow
              label="Report expires"
              value={formatTimestamp(observation.expiresAt, "Not reported")}
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
