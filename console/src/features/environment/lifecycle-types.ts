import { type MutableRefObject } from "react";
import type {
  Environment,
  EnvironmentEntry,
  Project,
  Route,
  Script,
  Service,
} from "@/lib/types";
import {
  type EnvironmentDeletionFailure,
  type EnvironmentMutationKind,
  type EnvironmentRemovalDraft,
  type PendingResourceRemoval,
  type TaskResponse,
} from "@/features/environment/environment-removal-model";

export type EnvironmentDeletionResponse = { task_id: string };

export type EnvironmentLifecycleDraft = EnvironmentRemovalDraft & {
  projectError: string | null;
  environmentDeletionRevision: number;
};

export type EnvironmentLifecycleOptions<
  State extends EnvironmentLifecycleDraft,
> = {
  active: MutableRefObject<boolean>;
  update: (fn: (draft: State) => void) => void;
  requestTask: RequestTask;
  deleteResource: DeleteResource;
  retryResource: RetryResource;
  listEnvironments: (
    projectId: string,
    signal?: AbortSignal,
  ) => Promise<Environment[]>;
  listServices: (
    environmentId: string,
    signal?: AbortSignal,
  ) => Promise<Service[]>;
  listRoutes: (environmentId: string, signal?: AbortSignal) => Promise<Route[]>;
  listEntries: (
    environmentId: string,
    signal?: AbortSignal,
  ) => Promise<EnvironmentEntry[]>;
  listScripts: (
    environmentId: string,
    signal?: AbortSignal,
  ) => Promise<Script[]>;
};

type RequestTask = (
  taskId: string,
  signal?: AbortSignal,
) => Promise<TaskResponse>;
type DeleteResource = (
  resource: "environments" | "services" | "routes" | "entries" | "scripts",
  id: string,
  idempotencyKey: string,
) => Promise<EnvironmentDeletionResponse>;
type RetryResource = (
  taskId: string,
  idempotencyKey: string,
) => Promise<EnvironmentDeletionResponse>;

export type EnvironmentLifecycle<State extends EnvironmentLifecycleDraft> = {
  pendingResourceRemovals: MutableRefObject<
    Map<string, PendingResourceRemoval>
  >;
  requestResourceRemovalTask: RequestTask;
  monitorResourceRemoval: (taskId: string) => void;
  reconcileResourceRemoval: (
    taskId: string,
    task: TaskResponse,
  ) => Promise<void>;
  dispatchResourceRemoval: (removal: PendingResourceRemoval) => Promise<string>;
  retryResourceRemoval: (taskId: string) => Promise<string> | undefined;
  waitForResourceRemoval: (taskId: string) => Promise<TaskResponse>;
  nextEnvironmentGeneration: (
    environmentId: string,
    kind?: EnvironmentMutationKind,
  ) => number;
  settleEnvironmentMutation: (
    environmentId: string,
    generation: number,
    outcome: "succeeded" | "failed",
  ) => void;
  shouldPreserveEnvironmentOnLoad: (
    environmentId: string,
    snapshotGeneration: number,
  ) => boolean;
  getEnvironmentDeletionFailure: (
    environmentId: string,
  ) => EnvironmentDeletionFailure | null;
  refreshEnvironmentDeletion: (environmentId: string) => Promise<void>;
  isEnvironmentDeletionPending: (environmentId: string) => boolean;
  observeEnvironmentDeletionTasks: (projects: Project[]) => void;
  environmentGenerations: MutableRefObject<Map<string, number>>;
};
