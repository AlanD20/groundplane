import { useResourceRefresh } from "@/features/task/use-resource-refresh";
import { controllerMutationVersion } from "@/lib/resource-observation";
import { readEnvironment } from "@/features/environment/workspace-read";
import { listAllTenants } from "@/features/tenant/api";
import { readProject } from "@/features/project/api";
import { RefreshSuperseded } from "@/features/task/resource-refresh-queue";
import { mergeEnvironmentProjectLoads } from "@/features/environment/workspace-reconciliation";

import type { State, StoreContext } from "@/lib/store-model";
import type { EnvironmentLifecycle } from "./lifecycle-types";
type Options = Pick<
  EnvironmentLifecycle<State>,
  "environmentGenerations" | "shouldPreserveEnvironmentOnLoad"
> &
  Pick<
    StoreContext,
    | "refreshAgents"
    | "refreshPlatformComponents"
    | "refreshRunners"
    | "refreshReusableSecrets"
  > & {
    update: (change: (draft: State) => void) => void;
    controllerPlatform: Pick<StoreContext, "refreshHost">;
    backupStore: Pick<StoreContext, "loadBackupPolicy" | "loadRecoveryPoints">;
  };

export function useWorkspaceResourceRefresh({
  environmentGenerations,
  shouldPreserveEnvironmentOnLoad,
  update,
  refreshAgents,
  refreshPlatformComponents,
  refreshRunners,
  refreshReusableSecrets,
  controllerPlatform,
  backupStore,
}: Options) {
  return useResourceRefresh(async (tasks, isCurrent) => {
    const mutationVersion = controllerMutationVersion();
    const completed = tasks.map((response) => response.task);
    const task = completed[completed.length - 1];
    if (task.scope.environment_id) {
      const id = task.scope.environment_id;
      const generation = environmentGenerations.current.get(id) ?? 0;
      const refreshed = await readEnvironment(id);
      if (!isCurrent()) return;
      if (mutationVersion !== controllerMutationVersion())
        throw new RefreshSuperseded();
      if ((environmentGenerations.current.get(id) ?? 0) !== generation)
        throw new RefreshSuperseded();
      if (!shouldPreserveEnvironmentOnLoad(id, generation)) return;
      update((draft) => {
        for (const project of [
          ...draft.tenantProjects,
          ...draft.backingProjects,
        ]) {
          const index =
            project.environments?.findIndex(
              (environment) => environment.id === id,
            ) ?? -1;
          if (!refreshed) {
            if (index >= 0) project.environments!.splice(index, 1);
          } else if (project.id === refreshed.projectId) {
            project.environments ??= [];
            if (index >= 0) project.environments[index] = refreshed;
            else project.environments.push(refreshed);
          }
        }
      });
    } else if (task.scope.project_id) {
      const generations = new Map(environmentGenerations.current);
      const refreshed = await readProject(task.scope.project_id);
      if (mutationVersion !== controllerMutationVersion())
        throw new RefreshSuperseded();
      if (isCurrent())
        update((draft) => {
          for (const projects of [
            draft.tenantProjects,
            draft.backingProjects,
          ]) {
            const index = projects.findIndex(
              (project) => project.id === task.scope.project_id,
            );
            if (!refreshed) {
              if (index >= 0) projects.splice(index, 1);
            } else if (
              (refreshed.kind === "backing") ===
              (projects === draft.backingProjects)
            ) {
              const merged = mergeEnvironmentProjectLoads(
                index >= 0 ? [projects[index]] : [],
                [refreshed],
                generations,
                environmentGenerations.current,
                shouldPreserveEnvironmentOnLoad,
              )[0];
              if (index >= 0) projects[index] = merged;
              else projects.push(merged);
            }
          }
        });
    }
    if (completed.some((entry) => entry.resource_kind === "agent"))
      await refreshAgents();
    if (
      completed.some((entry) => entry.resource_kind === "component") &&
      !task.scope.environment_id
    )
      await refreshPlatformComponents();
    if (
      completed.some((entry) =>
        ["controller", "agent", "etcd", "software"].includes(
          entry.resource_kind ?? "",
        ),
      )
    )
      await controllerPlatform.refreshHost();
    if (
      completed.some((entry) => entry.resource_kind === "runner") &&
      task.scope.tenant_id
    )
      await refreshRunners(task.scope.tenant_id);
    if (completed.some((entry) => entry.resource_kind === "secret"))
      await refreshReusableSecrets();
    if (
      task.scope.tenant_id &&
      !task.scope.project_id &&
      !task.scope.environment_id &&
      completed.some(
        (entry) =>
          entry.resource_kind === "tenant" && entry.action === "remove",
      )
    ) {
      const tenants = await listAllTenants(new AbortController().signal);
      if (isCurrent())
        update((draft) => {
          draft.tenants = tenants;
          draft.tenantProjects = draft.tenantProjects.filter((project) =>
            tenants.some((tenant) => tenant.id === project.tenantId),
          );
          draft.tenantError = null;
        });
    }
    if (
      task.scope.environment_id &&
      completed.some((entry) =>
        ["backup", "backup_prune", "restore", "rotate"].includes(entry.action),
      )
    ) {
      await Promise.all([
        backupStore.loadBackupPolicy(task.scope.environment_id),
        backupStore.loadRecoveryPoints(task.scope.environment_id),
      ]);
    }
  });
}
