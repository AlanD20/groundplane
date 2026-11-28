import { DeploymentRun } from "@/features/release/deployment-run";
import {
  deploymentReleaseParam,
  deploymentRunLink,
  deploymentRunParam,
} from "@/features/release/deployment-run-navigation";
import { ReleaseHistoryRow } from "@/features/release/release-history-row";
import { ServiceMetadata } from "@/features/service/service-metadata";
import { ServiceStorage } from "@/features/service/service-storage";
import { PageHeader } from "@/components/common/page-header";
import { ResourcePanel } from "@/components/common/resource-panel";
import {
  TablePagination,
  useTableView,
} from "@/components/common/table-controls";

import { Button, buttonVariants } from "@/components/ui/button";
import { Select } from "@/components/ui/select";
import { ServiceSettings } from "@/features/service/service-settings";
import { GitDeliveryRuntime } from "@/features/git-delivery/git-delivery-runtime";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { AttachesCard } from "@/features/attach/environment-attaches";
import {
  serviceDestination,
  type ServiceDestination,
} from "@/features/service/workspace-navigation";
import { LogStream } from "@/features/logs/log-viewer";
import {
  DeployDialog,
  RollbackDialog,
} from "@/features/release/environment-deploy-controls";
import { EnvVarsCard } from "@/features/entry/environment-entries";
import { ServiceConfiguration } from "@/features/service/service-configuration";
import { ServiceOverview } from "@/features/service/service-overview";
import { ServiceMonitoring } from "@/features/monitoring/service-monitoring";
import {
  ServiceOperationDialog,
  ServiceStateBadges,
  type ServiceOperation,
} from "@/features/service/service-runtime-actions";
import { useVisibleServiceObservations } from "@/features/service/use-service-observation-refresh";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import type { Environment, Service } from "@/lib/types";
import {
  ArrowLeft,
  ArrowUpCircle,
  Ban,
  Boxes,
  ChevronDown,
  CirclePlay,
  CircleStop,
  History,
  Trash2,
} from "lucide-react";
import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";

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
  const tenantId = store.tenantProjects.find(
    (project) => project.id === env.projectId,
  )?.tenantId;
  const [search, setSearch] = useSearchParams();
  const [detail, setDetail] = useState<Service>();
  const [reload, setReload] = useState(0);
  const releases = env.deploys.filter(
    (release) => release.serviceId === summary.id,
  );
  const service = {
    ...summary,
    nativeCompose: detail?.nativeCompose,
    releaseLedger: releases,
  };
  const servingReleaseId =
    summary.observation.state === "unavailable"
      ? undefined
      : summary.observation.servingReleaseId;
  const [detailError, setDetailError] = useState<string>();
  const deployments = useTableView(
    releases,
    { when: (release) => Date.parse(release.when) || 0 },
    "when",
    "desc",
    summary.id,
  );
  const [operation, setOperation] = useState<ServiceOperation | null>(null);
  const [releaseAction, setReleaseAction] = useState<
    "deploy" | "rollback" | null
  >(null);
  const tab = serviceDestination(search);
  const clock = useVisibleServiceObservations({
    environmentIds: [],
    observations: [service.observation],
    refreshEnvironment: store.refreshEnvironmentServices,
  });
  const now = Math.max(listNow, clock.now);
  const selectTab = (nextTab: ServiceDestination) =>
    setSearch((current) => {
      const next = new URLSearchParams(current);
      next.delete(deploymentRunParam);
      next.delete(deploymentReleaseParam);
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
      next.delete(deploymentRunParam);
      next.delete(deploymentReleaseParam);
      next.delete("service");
      next.delete("serviceTab");
      return next;
    });
  };

  useEffect(() => {
    if (tab !== "configuration") return;
    let current = true;
    void store
      .getService(summary.id)
      .then((detail) => {
        if (current) {
          setDetail(detail);
          setDetailError(undefined);
        }
      })
      .catch((error: unknown) => {
        if (current)
          setDetailError(
            error instanceof Error
              ? error.message
              : "Unable to load Service details",
          );
      });
    return () => {
      current = false;
    };
  }, [summary.id, servingReleaseId, reload, store.getService, tab]);

  const run = search.get(deploymentRunParam);
  if (tab === "releases" && run) {
    return (
      <DeploymentRun key={run} taskId={run} env={env} serviceId={summary.id} />
    );
  }

  return (
    <>
      <section className="flex min-w-0 flex-col gap-5">
        <div>
          <Button variant="ghost" size="sm" onClick={leaveService}>
            <ArrowLeft />
            Back to Environment
          </Button>
        </div>
        <PageHeader
          title={service.name}
          eyebrow={`${params.tenant} / ${params.project} / ${env.name} / Service`}
          icon={<Boxes />}
          description={service.role || `Service in ${env.name}`}
          meta={
            <>
              <ServiceStateBadges service={service} now={now} />
            </>
          }
          actions={
            <>
              <Button
                variant="outline"
                onClick={() =>
                  setOperation(
                    service.runtimeIntent === "running" ? "stop" : "start",
                  )
                }
              >
                {service.runtimeIntent === "running" ? (
                  <CircleStop />
                ) : (
                  <CirclePlay />
                )}
                {service.runtimeIntent === "running"
                  ? "Stop Service"
                  : "Start Service"}
              </Button>
              <DropdownMenu>
                <DropdownMenuTrigger
                  className={buttonVariants({ variant: "outline" })}
                >
                  Manage <ChevronDown className="size-4" />
                </DropdownMenuTrigger>
                <DropdownMenuContent>
                  <DropdownMenuItem
                    variant="destructive"
                    disabled={service.runtimeIntent === "absent"}
                    onClick={() => setOperation("destroy")}
                  >
                    <Ban /> Remove containers
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    variant="destructive"
                    onClick={() => setOperation("remove")}
                  >
                    <Trash2 /> Delete Service
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
              <Button
                disabled={releaseAction !== null}
                onClick={() => setReleaseAction("deploy")}
              >
                <ArrowUpCircle />
                Deploy
              </Button>
            </>
          }
        />
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

        <ServiceMetadata service={service} now={now} />
        {tab === "configuration" && detailError && (
          <p
            role="alert"
            className="rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-xs text-destructive"
          >
            {detailError}
          </p>
        )}
        <div className="min-w-0 space-y-5">
          {tab === "overview" && (
            <div className="pt-5">
              <ServiceOverview
                service={service}
                env={env}
                now={now}
                onNavigate={selectTab}
                onOpenLogs={() => selectTab("logs")}
                onOpenDeployments={() => selectTab("releases")}
              />
            </div>
          )}
          {tab === "monitoring" && (
            <ServiceMonitoring
              service={service}
              now={now}
              onOpenLogs={() => selectTab("logs")}
            />
          )}
          {tab === "logs" && (
            <div className="space-y-4 pt-5">
              <h2 className="text-lg font-semibold">Logs</h2>
              <LogStream target={{ kind: "service", id: service.id }} />
            </div>
          )}
          {tab === "releases" && (
            <div className="pt-5">
              <ResourcePanel
                title="Deployments"
                actions={
                  <>
                    <Select
                      aria-label="Deployment order"
                      className="w-36"
                      value={deployments.direction}
                      onValueChange={(direction) => {
                        if (direction !== deployments.direction)
                          deployments.sortBy("when");
                      }}
                      options={[
                        { value: "desc", label: "Newest first" },
                        { value: "asc", label: "Oldest first" },
                      ]}
                    />
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={releaseAction !== null}
                      onClick={() => setReleaseAction("rollback")}
                    >
                      <History className="size-3.5" />
                      Rollback
                    </Button>
                  </>
                }
              >
                {deployments.total ? (
                  <div className="space-y-3">
                    {deployments.rows.map((release) => (
                      <ReleaseHistoryRow
                        key={release.id}
                        release={release}
                        to={deploymentRunLink(
                          search,
                          release.taskId,
                          release.id,
                        )}
                      />
                    ))}
                    <TablePagination table={deployments} label="Deployments" />
                  </div>
                ) : (
                  <p role="status" className="text-sm text-muted-foreground">
                    No deployments yet.
                  </p>
                )}
              </ResourcePanel>
            </div>
          )}
          {tab === "git-delivery" && tenantId && (
            <GitDeliveryRuntime
              serviceId={service.id}
              serviceName={service.name}
              environmentId={env.id}
              environmentName={env.name}
              projectId={env.projectId}
              tenantId={tenantId}
              runnerRegistrationHref="?view=operations&panel=runners"
              onConnect={() =>
                setSearch(new URLSearchParams({ view: "settings", panel: "settings" }))
              }
            />
          )}
          {tab === "entries" && (
            <div className="pt-5">
              <EnvVarsCard env={env} service={service} />
            </div>
          )}
          {tab === "configuration" && (
            <div className="space-y-5 pt-5">
              <ServiceSettings
                sections={["workload", "runtime", "healthcheck", "hooks"]}
                env={env}
                service={service}
                workspace={params.tenant}
                onSaved={() => setReload((value) => value + 1)}
              />
            </div>
          )}
          {tab === "storage" && (
            <div>
              <ServiceStorage
                env={env}
                service={service}
                onSaved={() => setReload((value) => value + 1)}
                onFiles={() => selectTab("entries")}
                onVolumes={() =>
                  setSearch({ view: "configuration", panel: "volumes" })
                }
              />
            </div>
          )}
          {tab === "report" && (
            <div>
              <ServiceConfiguration
                env={env}
                onNavigate={selectTab}
                service={service}
              />
            </div>
          )}
          {tab === "connections" && (
            <div className="space-y-5">
              <AttachesCard env={env} service={service} />
            </div>
          )}
          {tab === "network" && (
            <ServiceSettings
              env={env}
              service={service}
              workspace={params.tenant}
              sections={["network"]}
              onSaved={() => setReload((value) => value + 1)}
            />
          )}
        </div>
      </section>
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
