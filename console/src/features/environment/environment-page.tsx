import RunnerManager from "@/features/runners/runner-manager";
import { EmptyState } from "@/components/common/empty-state";
import { PageHeader } from "@/components/common/page-header";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { AttachesCard } from "@/features/attach/environment-attaches";
import { FactsCard } from "@/features/attach/environment-facts";
import { BackupsCard } from "@/features/backup/environment-backups";
import { EnvironmentConnectorManager } from "@/features/connectors/environment-connector-manager";
import { BlueprintState } from "@/features/blueprint/environment-blueprint-state";
import { EnvVarsCard } from "@/features/entry/environment-entries";
import { LogViewer, LogStream } from "@/features/logs/log-viewer";
import { ReleaseGroupsPanel } from "@/features/release-group/release-group-surface";
import { DeployControls } from "@/features/release/environment-deploy-controls";
import { ReleasesCard } from "@/features/release/environment-releases-card";
import { ScriptsCard } from "@/features/script/scripts-card";
import { ServiceFormDialog } from "@/features/service/environment-service-dialog";
import { environmentRuntimeState } from "@/features/service/service-observation";
import { useVisibleServiceObservations } from "@/features/service/use-service-observation-refresh";
import { TasksCard } from "@/features/task/environment-tasks-card";
import { EnvironmentVolumeManager } from "@/features/volume/environment-volume-manager";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import { ArrowLeft, Boxes, Layers, Network, Plug } from "lucide-react";
import { Link, useSearchParams } from "react-router-dom";
import { EnvironmentDeletionFence } from "./deletion-fence";
import { SettingsCard } from "./environment-settings";
import { Topology } from "./network-topology";
import { RouterCard } from "./router-card";
import { RoutesCard } from "./routes-card";
import { environmentNavigation } from "./workspace-navigation";
import { EnvironmentOverview } from "./environment-overview";
import { ServiceWorkspace } from "./service-workspace";

export default function EnvironmentPage() {
  const params = useRequiredParams("tenant", "project", "env");
  const store = useStore();
  const env = store.getEnvironment(params.tenant, params.project, params.env);
  const project = store.getProject(params.tenant, params.project);
  const [search, setSearch] = useSearchParams();
  const { section, panel } = environmentNavigation(search);
  const backingEnvironments = store.backingProjects.flatMap(
    (backing) => backing.environments?.slice(0, 1) ?? [],
  );
  const observation = useVisibleServiceObservations({
    environmentIds: [
      ...(env ? [env.id] : []),
      ...backingEnvironments.map((environment) => environment.id),
    ],
    observations: [
      ...(env?.services.map((service) => service.observation) ?? []),
      ...backingEnvironments.flatMap((environment) =>
        environment.services.map((service) => service.observation),
      ),
    ],
    refreshEnvironment: async (id, signal) => {
      await store.refreshEnvironmentServices(id, signal);
      if (id === env?.id) await store.refreshEnvironmentReleases(id, signal);
    },
  });
  if (!env || !project)
    return (
      <EmptyState
        icon={<Layers />}
        title={
          store.tenantsLoading || store.projectsLoading
            ? "Loading Environment"
            : "Environment not found"
        }
        description={`${params.tenant} / ${params.project} / ${params.env}`}
        action={
          <Link to={`/t/${params.tenant}`}>
            <Button variant="outline">
              <ArrowLeft />
              Back to Tenant
            </Button>
          </Link>
        }
      />
    );

  const runtime = environmentRuntimeState(env.services, observation.now);
  const deleting =
    env.deletionTaskId !== null || store.isEnvironmentDeletionPending(env.id);
  const deletionFailure = store.getEnvironmentDeletionFailure(env.id);
  const deletionNotice = (
    <EnvironmentDeletionFence
      inProgress={deleting}
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
  );
  const panels: Record<string, React.ReactNode> = {
    runners: (
      <RunnerManager
        key={env.id}
        embedded
        scope={{ kind: "Environment", id: env.id, name: env.name }}
      />
    ),
    overview: <EnvironmentOverview env={env} now={observation.now} />,
    logs: <LogStream target={{ kind: "environment", id: env.id }} />,
    zones: <Topology env={env} />,
    routes: <RoutesCard env={env} />,
    attaches: <AttachesCard env={env} />,
    router: <RouterCard env={env} />,
    blueprint: <BlueprintState env={env} />,
    entries: <EnvVarsCard env={env} />,
    facts: <FactsCard env={env} />,
    volumes: <EnvironmentVolumeManager env={env} />,
    scripts: <ScriptsCard env={env} />,
    tasks: <TasksCard env={env} />,
    releases: <ReleasesCard env={env} />,
    "release-groups": <ReleaseGroupsPanel env={env} />,
    backups: <BackupsCard env={env} />,
    connectors: <EnvironmentConnectorManager env={env} />,
    settings: <SettingsCard env={env} />,
  };
  const selected = env.services.find((s) => s.id === search.get("service"));
  if (selected)
    return (
      <div className="flex min-w-0 flex-col gap-5">
        {deletionNotice}
        <fieldset
          key={deleting ? "deletion-fenced" : "editable"}
          disabled={deleting}
          className="min-w-0"
          aria-label={
            deleting ? "Environment deletion in progress" : selected.name
          }
        >
          <ServiceWorkspace
            key={selected.id}
            env={env}
            service={selected}
            now={observation.now}
            onBack={() =>
              setSearch((current) => {
                const next = new URLSearchParams(current);
                next.delete("service");
                next.delete("serviceTab");
                return next;
              })
            }
          />
        </fieldset>
      </div>
    );
  return (
    <div className="flex min-w-0 flex-col gap-6">
      <PageHeader
        title={
          <span className="flex flex-wrap items-center gap-3">
            {env.name}
            <StatusBadge
              status={runtime}
              label={runtime === "healthy" ? "Operational" : runtime}
            />
          </span>
        }
        eyebrow={`${params.tenant} / ${project.name} / Environment`}
        actions={
          <>
            <ServiceFormDialog env={env} />
            <LogViewer
              target={{ kind: "environment", id: env.id }}
              label="Logs"
            />
            <DeployControls env={env} disabled={deleting} />
          </>
        }
        meta={
          <>
            <span className="inline-flex items-center gap-1.5">
              <Boxes className="size-3.5" />
              {env.services.length} Services
            </span>
            <span className="inline-flex items-center gap-1.5">
              <Network className="size-3.5" />
              {env.zones.length} Zones
            </span>
            <span className="inline-flex items-center gap-1.5">
              <Plug className="size-3.5" />
              {env.routes.length} Routes
            </span>
            {env.provisioningState !== "ready" && (
              <StatusBadge
                status={env.status}
                label={`Provisioning ${env.provisioningState}`}
              />
            )}
          </>
        }
      />
      {deletionNotice}
      {observation.refreshError && (
        <p
          role="alert"
          className="rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-xs text-destructive"
        >
          Unable to refresh runtime. {observation.refreshError}
        </p>
      )}
      <fieldset
        key={deleting ? "deletion-fenced" : "editable"}
        disabled={deleting}
        className="min-w-0"
        aria-label={
          deleting ? "Environment deletion in progress" : section.label
        }
      >
        <section
          key={panel.key}
          aria-label={panel.label}
          className="min-w-0 space-y-5"
        >
          {panel.key !== "overview" && (
            <h2 className="text-lg font-semibold">{panel.label}</h2>
          )}
          {panels[panel.key]}
        </section>
      </fieldset>
    </div>
  );
}
