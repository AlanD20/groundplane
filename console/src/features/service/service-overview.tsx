import { ResourcePanel } from "@/components/common/resource-panel";
import { ImageReference } from "@/components/common/image-reference";
import { CopyButton } from "@/components/common/copy-button";
import { Button, buttonVariants } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
} from "@/components/ui/dropdown-menu";
import { Badge } from "@/components/ui/badge";
import { formatTimestamp } from "@/lib/format-timestamp";
import type { Environment, Service } from "@/lib/types";
import { currentServiceObservation, replicaTotal } from "./service-observation";
import { ServiceContainers } from "./service-containers";
import type { ServiceDestination } from "./workspace-navigation";

export function ServiceOverview({
  service,
  env,
  now = Date.now(),
  onOpenLogs,
  onOpenDeployments,
  onNavigate,
}: {
  service: Service;
  env: Environment;
  now?: number;
  onOpenLogs?: () => void;
  onOpenDeployments?: () => void;
  onNavigate?: (destination: ServiceDestination) => void;
}) {
  const observation = currentServiceObservation(service.observation, now);
  const releases = service.releaseLedger ?? [];
  const serving =
    observation.state === "unavailable"
      ? undefined
      : releases.find((release) => release.id === observation.servingReleaseId);
  const latest = [...releases].sort(
    (a, b) => Date.parse(b.when) - Date.parse(a.when),
  )[0];
  const routes = env.routes.filter(
    (route) => route.targetServiceId === service.id,
  );
  const connections = env.attaches.filter(
    (attach) =>
      attach.serviceId === service.id || attach.service === service.name,
  );
  const health = !service.healthcheck
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
      <div className="grid gap-3 sm:grid-cols-3">
        {[
          ["Runtime", observation.state],
          [
            "Containers",
            observation.state === "unavailable"
              ? "Not reported"
              : `${replicaTotal(observation.replicas)} / ${observation.expectedReplicas}`,
          ],
          ["Healthcheck", health],
        ].map(([label, value]) => (
          <div
            key={label}
            className="rounded-xl border border-border bg-card p-5"
          >
            <p className="text-sm text-muted-foreground">{label}</p>
            <p className="mt-2 text-xl font-semibold capitalize">{value}</p>
          </div>
        ))}
      </div>
      {observation.state === "unavailable" && (
        <p
          role="status"
          className="rounded-lg border border-warning/30 bg-warning/5 p-3 text-sm"
        >
          A current Agent report is unavailable. Running state and container
          health cannot be confirmed.
        </p>
      )}
      <ResourcePanel
        title={serving ? "Serving deployment" : "Deployment"}
        actions={
          <div className="flex gap-2">
            <Button variant="outline" size="sm" onClick={onOpenLogs}>
              View logs
            </Button>
            <Button variant="ghost" size="sm" onClick={onOpenDeployments}>
              Deployment history
            </Button>
          </div>
        }
      >
        <div className="min-w-0 space-y-3">
          <ImageReference
            value={serving?.tag || serving?.digest || service.image}
          />
          <div className="flex flex-wrap items-center gap-3 text-sm text-muted-foreground">
            {serving ? (
              <>
                <Badge variant="primary">Serving</Badge>
                <span>{formatTimestamp(serving.when, "Time unavailable")}</span>
              </>
            ) : (
              <span>Configured image · no serving deployment confirmed</span>
            )}
            {latest && latest.id !== serving?.id && (
              <span>
                Latest deployment: {latest.status} ·{" "}
                {formatTimestamp(latest.when, "Time unavailable")}
              </span>
            )}
          </div>
        </div>
      </ResourcePanel>
      <ResourcePanel
        title="Networking"
        actions={
          onNavigate && (
            <Button
              variant="outline"
              size="sm"
              onClick={() => onNavigate("network")}
            >
              Manage networking
            </Button>
          )
        }
      >
        <div className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <h3 className="text-sm font-medium">Network Zones</h3>
            <div className="flex flex-wrap gap-2">
              {service.zones.length ? (
                service.zones.map((id) => (
                  <Badge
                    key={id}
                    variant="primary"
                    className="max-w-full whitespace-normal break-all"
                  >
                    {env.zones.find(
                      (zone) => zone.id === id || zone.name === id,
                    )?.name ?? id}
                  </Badge>
                ))
              ) : (
                <p className="text-sm text-muted-foreground">
                  No Zones selected
                </p>
              )}
            </div>
          </div>
          <div className="space-y-2">
            <h3 className="text-sm font-medium">Exposed ports</h3>
            <p className="font-mono text-sm [overflow-wrap:anywhere]">
              {service.expose.join(", ") || "No exposed ports"}
            </p>
          </div>
        </div>
        <h3 className="border-t border-border pt-4 text-sm font-medium">
          HTTP addresses
        </h3>
        {routes.length ? (
          <div className="divide-y divide-border">
            {routes.map((route) => (
              <div
                key={route.id}
                className="flex min-w-0 flex-wrap items-center gap-3 py-3"
              >
                <span className="min-w-0 flex-1 break-all font-mono text-sm">
                  {route.host}
                  {route.path}
                </span>
                <Badge variant="muted">
                  {route.exposure} · {route.status}
                </Badge>
                <CopyButton
                  value={`${route.host}${route.path}`}
                  label="Copy address"
                />
                {route.host && (
                  <DropdownMenu>
                    <DropdownMenuTrigger
                      className={buttonVariants({
                        variant: "outline",
                        size: "sm",
                      })}
                    >
                      Open address
                    </DropdownMenuTrigger>
                    <DropdownMenuContent>
                      {(["https", "http"] as const).map((protocol) => (
                        <DropdownMenuItem
                          key={protocol}
                          render={
                            <a
                              href={`${protocol}://${route.host}${route.path.replace(/\*$/, "")}`}
                              target="_blank"
                              rel="noreferrer"
                            />
                          }
                        >
                          Open with {protocol.toUpperCase()}
                        </DropdownMenuItem>
                      ))}
                    </DropdownMenuContent>
                  </DropdownMenu>
                )}
              </div>
            ))}
          </div>
        ) : (
          <p className="text-sm text-muted-foreground">
            No HTTP routes point to this Service.
          </p>
        )}
        <p className="text-xs text-muted-foreground">
          Route status and container health do not verify application
          reachability.
        </p>
      </ResourcePanel>
      <ServiceContainers observation={observation} onOpenLogs={onOpenLogs} />
      <div className="grid gap-5 lg:grid-cols-2">
        <ResourcePanel
          title="Storage"
          actions={
            onNavigate && (
              <Button
                variant="link"
                size="sm"
                onClick={() => onNavigate("storage")}
              >
                Manage mounts
              </Button>
            )
          }
        >
          {service.mounts.length ? (
            service.mounts.map((mount, index) => (
              <div key={index} className="rounded-lg border border-border p-3">
                <p className="text-sm font-medium break-all">
                  {mount.type === "volume"
                    ? (env.volumes.find(
                        (volume) =>
                          volume.id === mount.volume ||
                          volume.slug === mount.volume,
                      )?.slug ?? mount.volume)
                    : mount.file}
                </p>
                <p className="mt-1 break-all font-mono text-sm text-muted-foreground">
                  → {mount.mount}
                </p>
              </div>
            ))
          ) : (
            <p className="text-sm text-muted-foreground">
              No persistent storage or files mounted.
            </p>
          )}
        </ResourcePanel>
        <ResourcePanel
          title="Backing connections"
          actions={
            onNavigate && (
              <Button
                variant="link"
                size="sm"
                onClick={() => onNavigate("connections")}
              >
                Manage connections
              </Button>
            )
          }
        >
          {connections.length ? (
            connections.map((attach) => (
              <div
                key={attach.id}
                className="rounded-lg border border-border p-3"
              >
                <p className="text-sm font-medium">{attach.name}</p>
                <p className="mt-1 text-sm text-muted-foreground">
                  {attach.database === "—"
                    ? "Backing service connection"
                    : attach.database}
                </p>
              </div>
            ))
          ) : (
            <p className="text-sm text-muted-foreground">
              No backing services connected.
            </p>
          )}
        </ResourcePanel>
      </div>
    </div>
  );
}
