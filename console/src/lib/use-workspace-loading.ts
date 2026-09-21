"use client";
import type { Project, Tenant } from "./types";

import { listAllBackingProjects } from "@/features/backing-service/workspace-read";

import { listAllTenants } from "@/features/tenant/api";

import { listAllTenantProjects } from "@/features/project/api";

import { useEffect } from "react";

import {
  environmentGenerationSnapshot,
  mergeEnvironmentProjectLoads,
} from "@/features/environment/workspace-reconciliation";

import { type State } from "./store-model";

import type { EnvironmentLifecycle } from "@/features/environment/lifecycle-types";

type Update = (fn: (draft: State) => void) => void;
type Lifecycle = EnvironmentLifecycle<State>;
type LoadingOptions = Pick<
  Lifecycle,
  | "environmentGenerations"
  | "observeEnvironmentDeletionTasks"
  | "shouldPreserveEnvironmentOnLoad"
  | "getEnvironmentDeletionFailure"
> & { update: Update };
export function useTenantLoading(update: Update) {
  useEffect(() => {
    const controller = new AbortController();
    void listAllTenants(controller.signal).then(
      (tenants) =>
        update((draft) => {
          draft.tenants = tenants;
          draft.tenantsLoading = false;
          draft.tenantError = null;
        }),
      (error: unknown) => {
        if (controller.signal.aborted) return;
        update((draft) => {
          draft.tenantsLoading = false;
          draft.tenantError =
            error instanceof Error ? error.message : "Unable to load tenants";
        });
      },
    );
    return () => controller.abort();
  }, [update]);
}
export function useBackingProjectLoading({
  state,
  backingConsumerInput,
  environmentGenerations,
  observeEnvironmentDeletionTasks,
  shouldPreserveEnvironmentOnLoad,
  update,
}: Omit<LoadingOptions, "getEnvironmentDeletionFailure"> & {
  state: State;
  backingConsumerInput: { projects: Project[]; tenants: Tenant[] };
}) {
  useEffect(() => {
    if (state.projectsLoading || state.tenantsLoading) return;
    const loadGenerations = environmentGenerationSnapshot(
      state.backingProjects,
      environmentGenerations.current,
    );
    const controller = new AbortController();
    void listAllBackingProjects(
      backingConsumerInput.projects,
      backingConsumerInput.tenants,
      controller.signal,
    ).then(
      (projects) => {
        observeEnvironmentDeletionTasks(projects);
        update((draft) => {
          draft.backingProjects = mergeEnvironmentProjectLoads(
            draft.backingProjects,
            projects,
            loadGenerations,
            shouldPreserveEnvironmentOnLoad,
          );
          draft.backingProjectsLoading = false;
          draft.backingProjectError = null;
        });
      },
      (error: unknown) => {
        if (controller.signal.aborted) return;
        update((draft) => {
          draft.backingProjects = [];
          draft.backingProjectsLoading = false;
          draft.backingProjectError =
            error instanceof Error
              ? error.message
              : "Unable to load backing services";
        });
      },
    );
    return () => controller.abort();
  }, [
    backingConsumerInput,
    observeEnvironmentDeletionTasks,
    shouldPreserveEnvironmentOnLoad,
    state.projectsLoading,
    state.tenantsLoading,
    update,
  ]);
}
export function useTenantProjectLoading({
  environmentGenerations,
  observeEnvironmentDeletionTasks,
  shouldPreserveEnvironmentOnLoad,
  getEnvironmentDeletionFailure,
  update,
}: LoadingOptions) {
  useEffect(() => {
    const controller = new AbortController();
    const loadGenerations = new Map(environmentGenerations.current);
    void listAllTenantProjects(controller.signal).then(
      (projects) => {
        observeEnvironmentDeletionTasks(projects);
        update((draft) => {
          draft.tenantProjects = projects.map((project) => {
            const currentProject = draft.tenantProjects.find(
              (candidate) => candidate.id === project.id,
            );
            const currentEnvironments = currentProject?.environments ?? [];
            const loadedIds = new Set(
              (project.environments ?? []).map((environment) => environment.id),
            );
            const environments = (project.environments ?? []).flatMap(
              (environment) => {
                const currentGeneration =
                  environmentGenerations.current.get(environment.id) ?? 0;
                if (
                  currentGeneration !==
                    (loadGenerations.get(environment.id) ?? 0) &&
                  !shouldPreserveEnvironmentOnLoad(
                    environment.id,
                    loadGenerations.get(environment.id) ?? 0,
                  )
                )
                  return [];
                return [
                  currentGeneration !==
                  (loadGenerations.get(environment.id) ?? 0)
                    ? (currentEnvironments.find(
                        (candidate) => candidate.id === environment.id,
                      ) ?? environment)
                    : environment,
                ];
              },
            );
            for (const current of currentEnvironments) {
              const currentGeneration =
                environmentGenerations.current.get(current.id) ?? 0;
              if (
                !loadedIds.has(current.id) &&
                currentGeneration !== (loadGenerations.get(current.id) ?? 0) &&
                shouldPreserveEnvironmentOnLoad(
                  current.id,
                  loadGenerations.get(current.id) ?? 0,
                )
              ) {
                environments.push(current);
              }
            }
            return { ...project, environments };
          });
          draft.projectsLoading = false;
          const deletionFailure = projects
            .flatMap((project) => project.environments ?? [])
            .map((environment) => getEnvironmentDeletionFailure(environment.id))
            .find(
              (
                failure,
              ): failure is NonNullable<
                ReturnType<typeof getEnvironmentDeletionFailure>
              > => failure !== null,
            );
          draft.projectError = deletionFailure?.message ?? null;
        });
      },
      (error: unknown) => {
        if (controller.signal.aborted) return;
        update((draft) => {
          draft.projectsLoading = false;
          draft.projectError =
            error instanceof Error ? error.message : "Unable to load projects";
        });
      },
    );
    return () => controller.abort();
  }, [
    observeEnvironmentDeletionTasks,
    environmentGenerations,
    getEnvironmentDeletionFailure,
    shouldPreserveEnvironmentOnLoad,
    update,
  ]);
}
