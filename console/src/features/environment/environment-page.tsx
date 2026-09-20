"use client";

import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { useRequiredParams } from "@/lib/router";
import {
  ArrowLeft,
  Boxes,
  Clock,
  History,
  Layers,
  Plug,
  Tag,
} from "lucide-react";
import { useStore } from "@/lib/store";
import { ScriptsCard } from "@/features/script/scripts-card";
import { EnvironmentDeletionFence } from "@/features/environment/deletion-fence";
import { PageHeader } from "@/components/common/page-header";
import { LogViewer } from "@/features/logs/log-viewer";
import { MetaPill } from "@/components/common/meta-pill";
import { StatCard } from "@/components/common/stat-card";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsList, TabsTab, TabsPanel } from "@/components/ui/tabs";
import { EmptyState } from "@/components/common/empty-state";
import { EnvironmentVolumeManager } from "@/features/volume/environment-volume-manager";
import { ReleaseGroupsPanel } from "@/features/release-group/release-group-surface";
import {
  environmentRuntimeHint,
  environmentRuntimeState,
} from "@/features/service/service-observation";
import { useVisibleServiceObservations } from "@/features/service/use-service-observation-refresh";
import { ServicesPanel } from "@/features/environment/services-list";
import { routeSummaryHint } from "@/features/environment/route-summary";
import { DeployControls } from "@/features/release/environment-deploy-controls";
import { Topology } from "@/features/environment/network-topology";
import { RoutesCard } from "@/features/environment/routes-card";
import { AttachesCard } from "@/features/attach/environment-attaches";
import { ServiceFormDialog } from "@/features/service/environment-service-dialog";
import { BlueprintState } from "@/features/blueprint/environment-blueprint-state";
import { RouterCard } from "@/features/environment/router-card";
import { ReleasesCard } from "@/features/release/environment-releases-card";
import { TasksCard } from "@/features/task/environment-tasks-card";
import { BackupsCard } from "@/features/backup/environment-backups";
import { EnvVarsCard } from "@/features/entry/environment-entries";
import { FactsCard } from "@/features/attach/environment-facts";
import { SettingsCard } from "@/features/environment/environment-settings";

type EnvTab =
  | "overview"
  | "services"
  | "state"
  | "router"
  | "releases"
  | "release-groups"
  | "tasks"
  | "backups"
  | "volumes"
  | "environments"
  | "settings"
  | "scripts";

export default function EnvironmentPage() {
  const params = useRequiredParams("tenant", "project", "env");
  const store = useStore();
  const env = store.getEnvironment(params.tenant, params.project, params.env);
  const project = store.getProject(params.tenant, params.project);
  // Deep-linkable tabs: ?tab=tasks opens the Tasks tab (journal click-through).
  const [tab, setTab] = useState<EnvTab>("overview");
  useEffect(() => {
    const t = new URLSearchParams(window.location.search).get("tab");
    if (
      t &&
      [
        "overview",
        "services",
        "state",
        "router",
        "releases",
        "release-groups",
        "tasks",
        "backups",
        "volumes",
        "environments",
        "settings",
        "scripts",
      ].includes(t)
    ) {
      setTab(t as EnvTab);
    }
  }, []);
  const visibleBackingEnvironments = store.backingProjects.flatMap(
    (backing) => backing.environments?.slice(0, 1) ?? [],
  );
  const observationRefresh = useVisibleServiceObservations({
    environmentIds: [
      ...(env ? [env.id] : []),
      ...visibleBackingEnvironments.map((environment) => environment.id),
    ],
    observations: [
      ...(env?.services.map((service) => service.observation) ?? []),
      ...visibleBackingEnvironments.flatMap((environment) =>
        environment.services.map((service) => service.observation),
      ),
    ],
    refreshEnvironment: store.refreshEnvironmentServices,
  });
  if (!env || !project) {
    if (store.tenantsLoading || store.projectsLoading) {
      return <EmptyState icon={<Layers />} title="Loading environment" />;
    }
    return (
      <EmptyState
        icon={<Layers />}
        title="Environment not found"
        description={`${params.tenant}/${params.project}/${params.env} does not exist.`}
        action={
          <Link to={`/t/${params.tenant}`}>
            <Button variant="outline">
              <ArrowLeft className="size-4" /> Back to tenant
            </Button>
          </Link>
        }
      />
    );
  }
  const provisioningFailed = env.provisioningState === "failed";
  const serviceCount = provisioningFailed ? "—" : env.services.length;
  const runtimeState = environmentRuntimeState(
    env.services,
    observationRefresh.now,
  );
  const runtimeHint = environmentRuntimeHint(
    env.services,
    observationRefresh.now,
  );
  const deletionFailure = store.getEnvironmentDeletionFailure(env.id);
  const deletionInProgress =
    env.deletionTaskId !== null || store.isEnvironmentDeletionPending(env.id);
  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title={
          <>
            {env.name}
            <StatusBadge
              status={env.status}
              label={`Provisioning ${env.provisioningState}`}
              className="ml-2 align-middle"
            />
            <StatusBadge
              status={runtimeState}
              label={`Runtime ${runtimeState}`}
              className="ml-2 align-middle"
            />
          </>
        }
        description={env.id}
        icon={<Layers />}
        meta={
          <>
            <MetaPill icon={<Tag />}>release {env.release}</MetaPill>
            <MetaPill icon={<Clock />}>deployed {env.lastDeployAt}</MetaPill>
          </>
        }
        actions={
          <div className="flex flex-wrap gap-2">
            <LogViewer
              target={{ kind: "environment", id: env.id }}
              label="Environment logs"
            />
            <DeployControls
              key={deletionInProgress ? "deletion-fenced" : "editable"}
              env={env}
              disabled={deletionInProgress}
            />
          </div>
        }
      />
      <EnvironmentDeletionFence
        inProgress={deletionInProgress}
        failure={deletionFailure}
        onRetry={() =>
          deletionFailure?.kind === "task"
            ? store.retryTask(deletionFailure.taskId)
            : store.refreshEnvironmentDeletion(env.id)
        }
        retryLabel={
          deletionFailure?.kind === "task" ? "Retry deletion" : "Retry refresh"
        }
      />
      {observationRefresh.refreshError && (
        <p role="alert" className="text-sm text-destructive">
          Service observation refresh failed; displayed runtime evidence will
          expire locally. {observationRefresh.refreshError}
        </p>
      )}
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <StatCard
          icon={<Boxes />}
          label="Services"
          value={serviceCount}
          hint={
            provisioningFailed
              ? `provisioning failed · runtime ${runtimeState}`
              : `runtime ${runtimeState} · ${runtimeHint}`
          }
          tone={
            runtimeState === "healthy" && !provisioningFailed
              ? "success"
              : runtimeState === "failed"
                ? "danger"
                : "warning"
          }
        />
        <StatCard
          icon={<Layers />}
          label="Zones"
          value={env.zones.length}
          hint="network zones"
        />
        <StatCard
          icon={<Plug />}
          label="Routes"
          value={env.routes.length}
          hint={routeSummaryHint(env.routes)}
        />
        <StatCard
          icon={<History />}
          label="Last deploy"
          value={env.release}
          hint={env.lastDeployAt}
        />
      </div>
      <fieldset
        key={deletionInProgress ? "deletion-fenced" : "editable"}
        disabled={deletionInProgress}
        className="contents"
        aria-label={
          deletionInProgress ? "Environment deletion in progress" : undefined
        }
      >
        <Tabs value={tab} onValueChange={(v) => setTab(v as EnvTab)}>
          <TabsList>
            <TabsTab value="overview">Overview</TabsTab>
            <TabsTab value="services">Services</TabsTab>
            <TabsTab value="state">Blueprint</TabsTab>
            <TabsTab value="router">Router</TabsTab>
            <TabsTab value="releases">Releases</TabsTab>
            <TabsTab value="release-groups">Release groups</TabsTab>
            <TabsTab value="tasks">Tasks</TabsTab>
            <TabsTab value="backups">Backups</TabsTab>
            <TabsTab value="volumes">Volumes</TabsTab>
            <TabsTab value="environments">Variables</TabsTab>
            <TabsTab value="scripts">Scripts</TabsTab>
            <TabsTab value="settings">Settings</TabsTab>
          </TabsList>
          <TabsPanel value="overview" className="mt-6 flex flex-col gap-6">
            <Topology env={env} />
            <RoutesCard env={env} />
            <AttachesCard env={env} />
          </TabsPanel>
          <TabsPanel value="services" className="mt-6 flex flex-col gap-4">
            <ServicesPanel
              env={env}
              now={observationRefresh.now}
              refreshing={observationRefresh.refreshing}
              onRefresh={observationRefresh.refreshNow}
              createAction={<ServiceFormDialog env={env} />}
            />
          </TabsPanel>
          <TabsPanel value="state" className="mt-6 flex flex-col gap-4">
            <BlueprintState env={env} />
          </TabsPanel>
          <TabsPanel value="router" className="mt-6 flex flex-col gap-6">
            <RouterCard env={env} />
          </TabsPanel>
          <TabsPanel value="releases" className="mt-6 flex flex-col gap-4">
            <ReleasesCard env={env} />
          </TabsPanel>
          <TabsPanel
            value="release-groups"
            className="mt-6 flex flex-col gap-4"
          >
            <ReleaseGroupsPanel env={env} />
          </TabsPanel>

          <TabsPanel value="tasks" className="mt-6 flex flex-col gap-4">
            <TasksCard env={env} />
          </TabsPanel>

          <TabsPanel value="backups" className="mt-6 flex flex-col gap-4">
            <BackupsCard env={env} />
          </TabsPanel>

          <TabsPanel value="volumes" className="mt-6 flex flex-col gap-4">
            <EnvironmentVolumeManager env={env} />
          </TabsPanel>

          <TabsPanel value="environments" className="mt-6 flex flex-col gap-4">
            <EnvVarsCard env={env} />
            <FactsCard env={env} />
          </TabsPanel>

          <TabsPanel value="scripts" className="mt-6 flex flex-col gap-4">
            <ScriptsCard env={env} />
          </TabsPanel>

          <TabsPanel value="settings" className="mt-6 flex flex-col gap-4">
            <SettingsCard env={env} />
          </TabsPanel>
        </Tabs>
      </fieldset>
    </div>
  );
}
