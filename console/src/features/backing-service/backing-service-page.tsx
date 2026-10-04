import { EmptyState } from "@/components/common/empty-state";
import { PageHeader } from "@/components/common/page-header";
import { SummaryItem, SummaryStrip } from "@/components/common/resource-panel";
import { ServiceFormBody } from "@/components/common/service-form-body";
import { PostgresImageUpdate } from "./postgres-image-update";
import { Button } from "@/components/ui/button";
import { Drawer } from "@/components/ui/drawer";
import { Tabs, TabsList, TabsPanel, TabsTab } from "@/components/ui/tabs";
import { ServiceOverview } from "@/features/service/service-overview";
import { LogStream } from "@/features/logs/log-viewer";
import { ServiceStateBadges } from "@/features/service/service-runtime-actions";
import { useVisibleServiceObservations } from "@/features/service/use-service-observation-refresh";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import { ArrowLeft, Database, Power, PowerOff, Trash2 } from "lucide-react";
import { useState } from "react";
import { Link } from "react-router-dom";
import { ServiceTab } from "./service-tab";

import { BackupsTab } from "./backing-backups";
import { ConnectionsTab } from "./backing-connections";
import { DesiredStateTab } from "./backing-desired-state";

type PlatformTab =
  "overview" | "logs" | "service" | "connections" | "state" | "backups";

export default function BackingServiceDetailPage() {
  const params = useRequiredParams("id");
  const store = useStore();
  const g = store.getBackingProject(params.id);
  const env = g?.environments?.[0];
  const svc = env?.services[0];
  const [tab, setTab] = useState<PlatformTab>("overview");
  const [pendingAction, setPendingAction] = useState<
    "start" | "stop" | "destroy" | null
  >(null);
  const [actionError, setActionError] = useState("");
  const [editOpen, setEditOpen] = useState(false);
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
  const port =
    adapter?.urlScheme === "redis"
      ? 6379
      : adapter?.urlScheme === "pgsql"
        ? 5432
        : undefined;
  // Backups are per consumer: count the attach-backed sources across all
  // environments that attach this backing project.
  const consumerBackupCount = store.tenantProjects.reduce(
    (n, p) =>
      n +
      (p.environments ?? []).reduce(
        (m, e) =>
          m +
          (e.backup?.sources ?? []).filter(
            (source) =>
              source.kind === "attach" &&
              e.attaches.find((attach) => attach.id === source.ref)
                ?.projectId === g.id,
          ).length,
        0,
      ),
    0,
  );
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
        description={g.description}
        icon={<Database />}
        meta={<ServiceStateBadges service={svc} now={observationRefresh.now} />}
        actions={
          <>
            <Button variant="outline" onClick={() => setEditOpen(true)}>
              Edit service
            </Button>
            {svc.adapter === "postgres:16" && <PostgresImageUpdate env={env} service={svc} />}
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
            <Button
              variant="destructive"
              disabled={pendingAction !== null}
              onClick={() => void runLifecycle("destroy")}
              title="Remove runtime and retain durable data"
            >
              <Trash2 className="size-4" /> Destroy
            </Button>
          </>
        }
      />
      {actionError && (
        <p role="alert" className="text-sm text-destructive">
          {actionError}
        </p>
      )}
      <Drawer open={editOpen} onOpenChange={setEditOpen}>
        {editOpen && (
          <ServiceFormBody
            env={env}
            workspace="platform"
            initial={svc}
            onClose={() => setEditOpen(false)}
          />
        )}
      </Drawer>
      {observationRefresh.refreshError && (
        <p role="alert" className="text-sm text-destructive">
          Runtime refresh failed; evidence will expire locally.{" "}
          {observationRefresh.refreshError}
        </p>
      )}

      <SummaryStrip>
        <SummaryItem label="Consumers">{g.consumers?.length ?? 0}</SummaryItem>
        <SummaryItem label="Adapter">
          {adapter?.label ?? svc.adapter ?? "—"}
        </SummaryItem>
        <SummaryItem label="Endpoint">
          {svc.serviceName}
          {port ? `:${port}` : ""}
        </SummaryItem>
        <SummaryItem label="Backup sources">{consumerBackupCount}</SummaryItem>
      </SummaryStrip>

      <Tabs value={tab} onValueChange={(v) => setTab(v as PlatformTab)}>
        <TabsList aria-label="Backing Service sections">
          <TabsTab value="overview">Overview</TabsTab>
          <TabsTab value="logs">Logs</TabsTab>
          <TabsTab value="service">Configuration</TabsTab>
          <TabsTab value="connections">Connections</TabsTab>
          <TabsTab value="state">Desired state</TabsTab>
          <TabsTab value="backups">Backups</TabsTab>
        </TabsList>

        <TabsPanel value="overview" className="mt-6">
          <ServiceOverview
            service={svc}
            env={env}
            now={observationRefresh.now}
          />
        </TabsPanel>
        <TabsPanel value="logs" className="mt-6">
          {tab === "logs" && (
            <LogStream target={{ kind: "service", id: svc.id }} />
          )}
        </TabsPanel>
        <TabsPanel value="service" className="mt-6 flex flex-col gap-6">
          <ServiceTab env={env} svc={svc} />
        </TabsPanel>
        <TabsPanel value="connections" className="mt-6 flex flex-col gap-6">
          <ConnectionsTab g={g} env={env} svc={svc} />
        </TabsPanel>
        <TabsPanel value="state" className="mt-6 flex flex-col gap-4">
          <DesiredStateTab g={g} env={env} svc={svc} />
        </TabsPanel>
        <TabsPanel value="backups" className="mt-6 flex flex-col gap-4">
          <BackupsTab g={g} env={env} svc={svc} />
        </TabsPanel>
      </Tabs>
    </div>
  );
}
