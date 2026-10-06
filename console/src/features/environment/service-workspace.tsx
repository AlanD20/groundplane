import { ServiceStorage } from "@/features/service/service-storage";
import { ImageReference } from "@/components/common/image-reference";
import { PageHeader } from "@/components/common/page-header";
import {
  AdvancedDetails,
  ResourcePanel,
} from "@/components/common/resource-panel";

import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import { ServiceSettings } from "@/features/service/service-settings";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Tabs, TabsList, TabsPanel, TabsTab } from "@/components/ui/tabs";
import { LogStream } from "@/features/logs/log-viewer";
import {
  DeployDialog,
  RollbackDialog,
} from "@/features/release/environment-deploy-controls";
import { EnvVarsCard } from "@/features/entry/environment-entries";
import { ServiceConfiguration } from "@/features/service/service-configuration";
import { ServiceOverview } from "@/features/service/service-overview";
import {
  ServiceOperationDialog,
  ServiceStateBadges,
  type ServiceOperation,
} from "@/features/service/service-runtime-actions";
import { useVisibleServiceObservations } from "@/features/service/use-service-observation-refresh";
import { formatTimestamp } from "@/lib/format-timestamp";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import type { Environment, Service } from "@/lib/types";
import {
  ArrowLeft,
  ArrowUpCircle,
  Ban,
  Boxes,
  ChevronDown,
  ChevronRight,
  CirclePlay,
  CircleStop,
  History,
  Terminal,
  Trash2,
} from "lucide-react";
import { useEffect, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";

type ServiceTab =
  "overview" | "logs" | "releases" | "entries" | "configuration";

const serviceTabs = new Set<ServiceTab>([
  "overview",
  "logs",
  "releases",
  "entries",
  "configuration",
]);

export function ServiceWorkspace({
  env,
  service: summary,
  now: listNow = Date.now(),
  onBack,
}: {
  env: Environment;
  service: Service;
  now?: number;
  onBack: () => void;
}) {
  const params = useRequiredParams("tenant", "project", "env");
  const store = useStore();
  const [search, setSearch] = useSearchParams();
  const [detail, setDetail] = useState<Service>();
  const [reload, setReload] = useState(0);
  const service = {
    ...summary,
    nativeCompose: detail?.nativeCompose,
    releaseLedger: detail?.releaseLedger ?? summary.releaseLedger,
  };
  const servingReleaseId =
    summary.observation.state === "unavailable"
      ? undefined
      : summary.observation.servingReleaseId;
  const [detailError, setDetailError] = useState<string>();
  const [loading, setLoading] = useState(true);
  const [operation, setOperation] = useState<ServiceOperation | null>(null);
  const [releaseAction, setReleaseAction] = useState<
    "deploy" | "rollback" | null
  >(null);
  const requestedTab = search.get("serviceTab") as ServiceTab | null;
  const tab =
    requestedTab && serviceTabs.has(requestedTab) ? requestedTab : "overview";
  const clock = useVisibleServiceObservations({
    environmentIds: [],
    observations: [service.observation],
    refreshEnvironment: store.refreshEnvironmentServices,
  });
  const now = Math.max(listNow, clock.now);
  const selectTab = (nextTab: ServiceTab) =>
    setSearch((current) => {
      const next = new URLSearchParams(current);
      if (nextTab === "overview") next.delete("serviceTab");
      else next.set("serviceTab", nextTab);
      return next;
    });
  const leaveService = () => {
    if (!search.has("service")) {
      onBack();
      return;
    }
    setSearch((current) => {
      const next = new URLSearchParams(current);
      next.delete("service");
      next.delete("serviceTab");
      return next;
    });
  };

  useEffect(() => {
    let current = true;
    setDetail(undefined);
    setLoading(true);
    setDetailError(undefined);
    void store
      .getService(summary.id)
      .then((detail) => {
        if (current) setDetail(detail);
      })
      .catch((error: unknown) => {
        if (current)
          setDetailError(
            error instanceof Error
              ? error.message
              : "Unable to load Service details",
          );
      })
      .finally(() => {
        if (current) setLoading(false);
      });
    return () => {
      current = false;
    };
  }, [summary.id, servingReleaseId, reload, store.getService]);

  return (
    <>
      <section className="flex min-w-0 flex-col gap-5">
        <div>
          <Button variant="ghost" size="sm" onClick={leaveService}>
            <ArrowLeft />
            All Services
          </Button>
        </div>
        <PageHeader
          title={service.name}
          icon={<Boxes />}
          description={service.role || `Service in ${env.name}`}
          meta={<ServiceStateBadges service={service} now={now} />}
          actions={
            <>
              <DropdownMenu>
                <DropdownMenuTrigger
                  className={buttonVariants({ variant: "outline" })}
                >
                  Actions <ChevronDown className="size-4" />
                </DropdownMenuTrigger>
                <DropdownMenuContent>
                  <DropdownMenuItem
                    disabled={service.runtimeIntent === "running"}
                    onClick={() => setOperation("start")}
                  >
                    <CirclePlay /> Start
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    disabled={service.runtimeIntent === "stopped"}
                    onClick={() => setOperation("stop")}
                  >
                    <CircleStop /> Stop
                  </DropdownMenuItem>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem
                    variant="destructive"
                    disabled={service.runtimeIntent === "absent"}
                    onClick={() => setOperation("destroy")}
                  >
                    <Ban /> Destroy runtime
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    variant="destructive"
                    onClick={() => setOperation("remove")}
                  >
                    <Trash2 /> Remove Service
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
              <Button onClick={() => setReleaseAction("deploy")}>
                <ArrowUpCircle />
                Deploy
              </Button>
            </>
          }
        />
        {detailError && (
          <p
            role="alert"
            className="rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-xs text-destructive"
          >
            {detailError}
          </p>
        )}
        <Tabs
          value={tab}
          onValueChange={(value) => selectTab(String(value) as ServiceTab)}
          className="min-w-0 flex-1"
        >
          <TabsList variant="underline" aria-label="Service sections">
            <TabsTab value="overview">Overview</TabsTab>
            <TabsTab value="logs">
              <Terminal /> Logs
            </TabsTab>
            <TabsTab value="releases">Deployments</TabsTab>
            <TabsTab value="entries">Variables & files</TabsTab>
            <TabsTab value="configuration">Settings</TabsTab>
          </TabsList>
          <TabsPanel value="overview" className="pt-5">
            <ServiceOverview
              service={service}
              env={env}
              now={now}
              onOpenLogs={() => selectTab("logs")}
              onOpenDeployments={() => selectTab("releases")}
            />
          </TabsPanel>
          <TabsPanel value="logs" className="pt-5">
            <LogStream target={{ kind: "service", id: service.id }} />
          </TabsPanel>
          <TabsPanel value="releases" className="pt-5">
            <ResourcePanel
              title="Deployments"
              actions={
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setReleaseAction("rollback")}
                >
                  <History className="size-3.5" />
                  Rollback
                </Button>
              }
            >
              {service.releaseLedger?.length ? (
                <div className="divide-y divide-border rounded-xl border border-border">
                  {service.releaseLedger.map((release) => (
                    <Link
                      key={release.id}
                      to={`/t/${params.tenant}/${params.project}/${params.env}/releases/${release.id}`}
                      className="grid min-w-0 grid-cols-[auto_minmax(0,1fr)_auto] items-center gap-3 p-4 transition-colors hover:bg-muted focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-ring"
                    >
                      <span className="flex size-9 items-center justify-center rounded-lg bg-accent text-primary">
                        <History className="size-4" />
                      </span>
                      <span className="min-w-0">
                        <span className="block font-medium">
                          <ImageReference value={release.tag} />
                        </span>
                        <span className="mt-1 block text-[11px] text-muted-foreground">
                          {formatTimestamp(release.when, "Time unavailable")} ·{" "}
                          {release.strategy}
                        </span>
                        <span
                          className="mt-1 block truncate font-mono text-[10px] text-muted-foreground"
                          title={release.digest || release.id}
                        >
                          {release.digest || release.id}
                        </span>
                      </span>
                      <span className="flex items-center gap-2">
                        <Badge
                          variant={
                            release.status === "active" ? "primary" : "outline"
                          }
                        >
                          {release.status}
                        </Badge>
                        <ChevronRight className="size-4 text-muted-foreground" />
                      </span>
                    </Link>
                  ))}
                </div>
              ) : (
                <p role="status" className="text-sm text-muted-foreground">
                  {loading ? "Loading Release history…" : "No Releases yet."}
                </p>
              )}
            </ResourcePanel>
          </TabsPanel>
          <TabsPanel value="entries" className="pt-5">
            <EnvVarsCard env={env} service={service} />
          </TabsPanel>
          <TabsPanel value="configuration" className="space-y-5 pt-5">
            <ServiceSettings
              env={env}
              service={service}
              workspace={params.tenant}
              onSaved={() => setReload((value) => value + 1)}
            />
            <ServiceStorage
              env={env}
              service={service}
              onSaved={() => setReload((value) => value + 1)}
              onFiles={() => selectTab("entries")}
              onVolumes={() =>
                setSearch({ view: "configuration", panel: "volumes" })
              }
            />
            <AdvancedDetails title="Full configuration & Compose">
              <ServiceConfiguration
                service={service}
                loading={loading}
                error={detailError}
              />
            </AdvancedDetails>
          </TabsPanel>
        </Tabs>
      </section>
      {releaseAction === "deploy" && (
        <DeployDialog
          env={env}
          serviceId={service.id}
          open
          onOpenChange={(value) => {
            if (!value) setReleaseAction(null);
          }}
        />
      )}
      {releaseAction === "rollback" && (
        <RollbackDialog
          env={env}
          serviceId={service.id}
          open
          onOpenChange={(value) => {
            if (!value) setReleaseAction(null);
          }}
        />
      )}
      <ServiceOperationDialog
        env={env}
        service={service}
        operation={operation}
        workspace={params.tenant}
        onOpenChange={(value) => {
          if (!value) setOperation(null);
        }}
        onRemoved={() => {
          setOperation(null);
          leaveService();
        }}
      />
    </>
  );
}
