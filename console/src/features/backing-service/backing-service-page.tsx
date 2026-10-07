import { ServiceMetadata } from "@/features/service/service-metadata";
import { EmptyState } from "@/components/common/empty-state";
import { DetailRow } from "@/components/common/detail-row";
import { PageHeader } from "@/components/common/page-header";
import { ResourcePanel } from "@/components/common/resource-panel";
import { ServiceSettings } from "@/features/service/service-settings";
import { CopyButton } from "@/components/common/copy-button";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { PostgresImageUpdate } from "./postgres-image-update";
import { Button } from "@/components/ui/button";
import { backingDestinations } from "@/features/service/workspace-navigation";
import { LogStream } from "@/features/logs/log-viewer";
import { ServiceContainers } from "@/features/service/service-containers";
import { currentServiceObservation } from "@/features/service/service-observation";
import { ServiceStateBadges } from "@/features/service/service-runtime-actions";
import { useVisibleServiceObservations } from "@/features/service/use-service-observation-refresh";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import { ArrowLeft, Database, Power, PowerOff, Trash2 } from "lucide-react";
import { useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { ServiceTab } from "./service-tab";

import { BackupsTab } from "./backing-backups";
import { ConnectionsTab } from "./backing-connections";
import { DesiredStateTab } from "./backing-desired-state";

type PlatformTab = (typeof backingDestinations)[number]["key"];

export default function BackingServiceDetailPage() {
  const params = useRequiredParams("id");
  const store = useStore();
  const g = store.getBackingProject(params.id);
  const env = g?.environments?.[0];
  const svc = env?.services[0];
  const [search, setSearch] = useSearchParams();
  const requested = search.get("tab");
  const tab =
    backingDestinations.find((item) => item.key === requested)?.key ??
    "overview";
  const setTab = (value: PlatformTab) =>
    setSearch((current) => {
      const next = new URLSearchParams(current);
      next.set("tab", value);
      return next;
    });
  const [pendingAction, setPendingAction] = useState<
    "start" | "stop" | "destroy" | null
  >(null);
  const [actionError, setActionError] = useState("");
  const [destroyOpen, setDestroyOpen] = useState(false);
  const observationRefresh = useVisibleServiceObservations({
    environmentIds: env ? [env.id] : [],
    observations: svc ? [svc.observation] : [],
    refreshEnvironment: store.refreshEnvironmentServices,
  });

  if (!g || !env || !svc) {
    return (
      <EmptyState
        icon={<Database />}
        title="Backing service not found"
        description={`${params.id} does not exist.`}
        action={
          <Link to="/platform/backing-services">
            <Button variant="outline">
              <ArrowLeft className="size-4" /> Back to backing services
            </Button>
          </Link>
        }
      />
    );
  }

  const running = svc.runtimeIntent === "running";
  const adapter = store.adapters.find((a) => a.key === svc.adapter);
  const observation = currentServiceObservation(
    svc.observation,
    observationRefresh.now,
  );
  const port =
    adapter?.urlScheme === "redis"
      ? 6379
      : adapter?.urlScheme === "pgsql"
        ? 5432
        : undefined;
  const runLifecycle = async (action: "start" | "stop" | "destroy") => {
    setPendingAction(action);
    setActionError("");
    try {
      await store.runBackingRuntimeAction(g.id, action);
    } catch (error) {
      setActionError(
        error instanceof Error
          ? error.message
          : "Backing-service lifecycle request failed",
      );
    } finally {
      setPendingAction(null);
    }
  };

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title={g.name}
        eyebrow="Backing Service"
        description={
          <>
            {adapter?.label ?? svc.adapter ?? "Backing service"} · Platform /{" "}
            {env.name}
            {g.description ? ` · ${g.description}` : ""}
          </>
        }
        icon={<Database />}
        meta={
          <>
            <ServiceStateBadges service={svc} now={observationRefresh.now} />
          </>
        }
        actions={
          <>
            {running ? (
              <Button
                variant="outline"
                disabled={pendingAction !== null}
                onClick={() => void runLifecycle("stop")}
              >
                <PowerOff className="size-4" /> Stop
              </Button>
            ) : (
              <Button
                disabled={pendingAction !== null}
                onClick={() => void runLifecycle("start")}
              >
                <Power className="size-4" /> Start
              </Button>
            )}
          </>
        }
      />
      <ServiceMetadata
        service={svc}
        now={observationRefresh.now}
        resourceId={g.id}
        idLabel="Backing Service ID"
      />
      {actionError && (
        <p role="alert" className="text-sm text-destructive">
          {actionError}
        </p>
      )}
      {observationRefresh.refreshError && (
        <p role="alert" className="text-sm text-destructive">
          Runtime refresh failed; evidence will expire locally.{" "}
          {observationRefresh.refreshError}
        </p>
      )}

      <div className="min-w-0 space-y-5">
        {tab === "logs" && (
          <h2 className="text-lg font-semibold">
            {backingDestinations.find((item) => item.key === tab)?.label}
          </h2>
        )}
        {tab === "overview" && (
          <div className="space-y-5 pt-5">
            <div className="grid items-start gap-5 lg:grid-cols-2">
              <ResourcePanel
                title="Connect to this service"
                actions={
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setTab("connections")}
                  >
                    Connection details
                  </Button>
                }
              >
                <DetailRow
                  label="Internal host"
                  value={
                    <span className="inline-flex items-center gap-2 break-all">
                      {svc.serviceName ?? svc.name}
                      <CopyButton
                        value={svc.serviceName ?? svc.name}
                        label="Copy internal host"
                      />
                    </span>
                  }
                />
                {port && <DetailRow label="Port" value={String(port)} />}
                <p className="text-sm text-muted-foreground">
                  Applications connect through an Attach. Credentials and
                  connection URLs belong to each connection.
                </p>
              </ResourcePanel>
              <ResourcePanel
                title="Connected applications"
                actions={
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setTab("connections")}
                  >
                    View all ({g.consumers?.length ?? 0})
                  </Button>
                }
              >
                {(g.consumers ?? []).slice(0, 3).map((consumer) => (
                  <Button
                    key={consumer.attachId}
                    variant="ghost"
                    size="content"
                    className="flex w-full items-center justify-between gap-3 px-0 py-2 text-left"
                    onClick={() => setTab("connections")}
                  >
                    <span className="min-w-0">
                      <span className="block text-sm font-medium">
                        {consumer.service}
                      </span>
                      <span className="block text-xs text-muted-foreground">
                        {consumer.project} / {consumer.environment}
                      </span>
                    </span>
                    <span className="text-xs text-muted-foreground">
                      {consumer.database || "Network access"}
                    </span>
                  </Button>
                ))}
                {!g.consumers?.length && (
                  <p className="text-sm text-muted-foreground">
                    No applications connected. Add a backing connection from an
                    application's Environment.
                  </p>
                )}
              </ResourcePanel>
            </div>
            <ServiceContainers
              observation={observation}
              onOpenLogs={() => setTab("logs")}
            />
            <ResourcePanel title="Persistent storage">
              {env.volumes.length ? (
                env.volumes.map((volume) => (
                  <DetailRow
                    key={volume.id}
                    label={volume.slug}
                    value="Retained when runtime stops or is destroyed"
                  />
                ))
              ) : (
                <p className="text-sm text-muted-foreground">
                  No managed Volumes.
                </p>
              )}
            </ResourcePanel>
          </div>
        )}
        {tab === "logs" && (
          <div className="mt-6">
            {tab === "logs" && (
              <LogStream target={{ kind: "service", id: svc.id }} />
            )}
          </div>
        )}
        {tab === "service" && (
          <div className="space-y-5 pt-5">
            <p className="text-sm text-muted-foreground">
              This instance is shared. Changes can affect every connected
              application.
            </p>
            <ServiceSettings
              sections={["workload", "runtime", "healthcheck", "hooks"]}
              env={env}
              service={svc}
              workspace="platform"
              imageAction={
                svc.adapter === "postgres:16" ? (
                  <PostgresImageUpdate env={env} service={svc} />
                ) : undefined
              }
            />
            <ResourcePanel title="Remove containers">
              <p className="text-sm text-muted-foreground">
                Keep this Backing Service, its configuration, connections and
                persistent Volumes. Start recreates the containers. Files stored
                only inside the containers are lost.
              </p>
              <Button
                variant="destructive"
                disabled={
                  pendingAction !== null || svc.runtimeIntent === "absent"
                }
                onClick={() => setDestroyOpen(true)}
              >
                <Trash2 /> Remove containers
              </Button>
            </ResourcePanel>
          </div>
        )}
        {tab === "connections" && (
          <div className="mt-6 flex flex-col gap-6">
            <ConnectionsTab g={g} env={env} svc={svc} />
          </div>
        )}
        {tab === "backups" && (
          <div className="mt-6 flex flex-col gap-4">
            <BackupsTab g={g} env={env} svc={svc} />
          </div>
        )}
        {tab === "network" && (
          <ServiceSettings
            env={env}
            service={svc}
            workspace="platform"
            sections={["network"]}
          />
        )}
        {tab === "report" && (
          <div className="space-y-5">
            <ServiceTab env={env} svc={svc} />
            <DesiredStateTab g={g} env={env} svc={svc} />
          </div>
        )}
      </div>
      {destroyOpen && (
        <TaskRunnerDialog
          open
          onOpenChange={setDestroyOpen}
          title={`Remove containers · ${g.name}`}
          description="Remove this Backing Service's containers. Configuration, connections and persistent Volumes are kept; Start recreates the containers. Files stored only inside the containers are lost."
          type="destroy"
          target={svc.name}
          workspace="platform"
          startLabel="Remove containers"
          steps={[]}
          onDispatch={() => store.runBackingRuntimeAction(g.id, "destroy")}
        />
      )}
    </div>
  );
}
