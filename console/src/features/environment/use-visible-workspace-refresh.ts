import { useRef } from "react";
import { useLocation } from "react-router-dom";
import { useVisibleRefresh } from "@/lib/use-visible-refresh";
import { controllerMutationVersion } from "@/lib/resource-observation";
import type { State, StoreContext } from "@/lib/store-model";
import { listAllTenants } from "@/features/tenant/api";
import { listTenantProjectMetadata } from "@/features/project/api";
import { listAllBackingProjects } from "@/features/backing-service/workspace-read";
import { listEnvironmentMetadata, readEnvironment } from "./workspace-read";
import type { EnvironmentLifecycle } from "./lifecycle-types";
import { environmentFromMetadata } from "./projection";
import { mergeEnvironmentProjectLoads } from "./workspace-reconciliation";

type Options = Pick<
  EnvironmentLifecycle<State>,
  | "environmentGenerations"
  | "shouldPreserveEnvironmentOnLoad"
  | "observeEnvironmentDeletionTasks"
> &
  Pick<
    StoreContext,
    | "refreshAgents"
    | "refreshPlatformComponents"
    | "refreshReusableSecrets"
    | "refreshRunners"
  > & {
    state: State;
    update: (change: (draft: State) => void) => void;
    refreshHost: StoreContext["refreshHost"];
    refreshConnectors: (environmentId: string) => Promise<void>;
  };

export function useVisibleWorkspaceRefresh(options: Options) {
  const { pathname, search } = useLocation();
  const view = new URLSearchParams(search).get("view");
  const current = useRef(options);
  current.current = options;
  useVisibleRefresh(
    async (signal) => {
      const {
        state,
        update,
        environmentGenerations,
        shouldPreserveEnvironmentOnLoad,
        observeEnvironmentDeletionTasks,
      } = current.current;
      const version = controllerMutationVersion();
      const generations = new Map(environmentGenerations.current);
      const isCurrent = () =>
        !signal.aborted && version === controllerMutationVersion();
      const parts = pathname.split("/").filter(Boolean).map(decodeURIComponent);
      const environmentMetadata = new Map<
        string,
        Awaited<ReturnType<typeof listEnvironmentMetadata>>[number]
      >();
      const resourceReads = new Set<string>();
      // Refresh collection membership cheaply. Only visible/new Environments need
      // their full resource collections; do not reload every project's history.
      try {
        const [tenants, metadata] = await Promise.all([
          listAllTenants(signal),
          listTenantProjectMetadata(signal),
        ]);
        const projects = await Promise.all(
          metadata.map(async (project) => {
            const existing = state.tenantProjects.find(
              (value) => value.id === project.id,
            );
            const environments = await listEnvironmentMetadata(
              project.id,
              signal,
            );
            const selected =
              parts[0] === "t" &&
              parts[1] ===
                tenants.find((tenant) => tenant.id === project.tenantId)
                  ?.slug &&
              parts[2] === project.slug
                ? parts[3]
                : undefined;
            return {
              ...project,
              environments: (
                await Promise.all(
                  environments.map(async (environment) => {
                    environmentMetadata.set(environment.id, environment);
                    const cached = existing?.environments?.find(
                      (value) => value.id === environment.id,
                    );
                    if (!cached || selected === environment.name) {
                      resourceReads.add(environment.id);
                      try {
                        return await readEnvironment(environment.id, signal);
                      } catch (error) {
                        if (signal.aborted) throw error;
                        return {
                          ...(cached ?? environmentFromMetadata(environment)),
                          ...environment,
                          resourceLoadError:
                            error instanceof Error
                              ? error.message
                              : "Unable to refresh Environment resources",
                        };
                      }
                    }
                    return { ...cached, ...environment };
                  }),
                )
              ).filter((value) => value !== null),
            };
          }),
        );
        if (!isCurrent()) return;
        observeEnvironmentDeletionTasks(projects);
        update((draft) => {
          draft.tenants = tenants;
          draft.tenantError = null;
          const loaded = projects.map((project) => ({
            ...project,
            environments: project.environments.map((environment) => {
              if (resourceReads.has(environment.id)) return environment;
              const latest = draft.tenantProjects
                .find((value) => value.id === project.id)
                ?.environments?.find((value) => value.id === environment.id);
              return latest
                ? { ...latest, ...environmentMetadata.get(environment.id) }
                : environment;
            }),
          }));
          draft.tenantProjects = mergeEnvironmentProjectLoads(
            draft.tenantProjects,
            loaded,
            generations,
            environmentGenerations.current,
            shouldPreserveEnvironmentOnLoad,
          );
          draft.projectError = null;
        });
        if (
          parts[0] === "platform" &&
          ["overview", "backing-services"].includes(parts[1])
        ) {
          const backings = await listAllBackingProjects(
            projects,
            tenants,
            signal,
          );
          if (!isCurrent()) return;
          update((draft) => {
            draft.backingProjects = mergeEnvironmentProjectLoads(
              draft.backingProjects,
              backings,
              generations,
              environmentGenerations.current,
              shouldPreserveEnvironmentOnLoad,
            );
            draft.backingProjectError = null;
          });
        }
        const visibleProject =
          parts[0] === "t"
            ? projects.find(
                (project) =>
                  project.slug === parts[2] &&
                  tenants.some(
                    (tenant) =>
                      tenant.id === project.tenantId &&
                      tenant.slug === parts[1],
                  ),
              )
            : undefined;
        const visibleEnvironment = visibleProject?.environments.find(
          (environment) => environment.name === parts[3],
        );
        if (visibleEnvironment)
          await current.current.refreshConnectors(visibleEnvironment.id);
        if (parts[0] === "platform")
          await Promise.all([
            current.current.refreshHost(signal),
            current.current.refreshAgents(signal),
            current.current.refreshPlatformComponents(signal),
          ]);
        if (parts.includes("secrets"))
          await current.current.refreshReusableSecrets(signal);
        if (
          parts[0] === "t" &&
          (parts[2] === "runners" || view === "runners")
        ) {
          const tenant = tenants.find((tenant) => tenant.slug === parts[1]);
          if (tenant) await current.current.refreshRunners(tenant.id);
        }
      } catch (error) {
        if (isCurrent())
          update((draft) => {
            draft.projectError =
              error instanceof Error
                ? error.message
                : "Unable to refresh workspace";
          });
      }
    },
    {
      key: `${pathname}/${view ?? ""}`,
      enabled:
        !options.state.projectsLoading &&
        !options.state.tenantsLoading &&
        !options.state.backingProjectsLoading,
    },
  );
}
