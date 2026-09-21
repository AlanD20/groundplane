"use client";

import { type ReleaseActions } from "@/features/release/actions";
import { type EnvironmentActions } from "@/features/environment/actions";
import {
  type TaskJournalStoreState,
  type TaskJournalActions,
} from "@/features/task/use-task-journal";
import { type TaskEventActions } from "@/features/task/use-task-event-streams";
import {
  type AgentActions,
  type AgentState,
} from "@/features/agent/agent-store";
import { type BackingServiceActions } from "@/features/backing-service/actions";
import { type TenantActions } from "@/features/tenant/actions";
import { type ProjectActions } from "@/features/project/actions";
import { type ComponentActions } from "@/features/component/actions";
import {
  type ComponentState,
  type ComponentRefreshActions,
} from "@/features/component/use-component-refresh";
import { type AttachActions } from "@/features/attach/actions";
import { type EntryActions } from "@/features/entry/actions";
import { type ReleaseGroupActions } from "@/features/release-group/actions";
import { type ScriptActions } from "@/features/script/actions";
import { type VolumeActions } from "@/features/volume/actions";
import { type NetworkActions } from "@/features/environment/use-network-actions";
import { type ServiceActions } from "@/features/service/actions";
import {
  type RunnerState,
  type RunnerActions,
} from "@/features/runner/use-runner-store";
import {
  type ConnectorState,
  type ConnectorActions,
} from "@/features/connectors/use-connector-store";
import { emptyTaskJournal } from "@/features/task/journal-model";
import { useBackupStore } from "@/features/backup/use-backup-store";
import {
  type ReusableSecretState,
  type ReusableSecretActions,
} from "@/features/secrets/secret-store";
import { type LogTarget, type TransientLogEvent } from "./transient-logs";
import { useControllerPlatform } from "@/features/platform-controller/use-controller-platform";
import type { Environment, Project, Tenant, PlatformInfra } from "./types";
import { type BlueprintActions } from "@/features/blueprint/api";
import {
  adapters as seedAdapters,
  platform as seedPlatform,
} from "./workspace-seed";
import type {
  EnvironmentDeletionFailure,
  TaskResponse,
} from "@/features/environment/environment-removal-model";

export type State = ReusableSecretState &
  ConnectorState &
  RunnerState &
  TaskJournalStoreState &
  ComponentState &
  AgentState & {
    // UI preference: typed confirmation before revealing a secret value
    requireRevealConfirm: boolean;
    tenants: Tenant[];
    tenantsLoading: boolean;
    tenantError: string | null;
    tenantProjects: Project[];
    projectsLoading: boolean;
    projectError: string | null;
    environmentDeletionRevision: number;
    backingProjects: Project[];
    backingProjectsLoading: boolean;
    backingProjectError: string | null;
    platform: PlatformInfra;
  };

export function seed(): State {
  let requireRevealConfirm = false;
  try {
    requireRevealConfirm =
      typeof window !== "undefined" &&
      localStorage.getItem("groundplane-reveal-confirm") === "1";
  } catch {
    /* private mode */
  }
  return structuredClone({
    requireRevealConfirm,
    tenants: [],
    tenantsLoading: true,
    tenantError: null,
    tenantProjects: [],
    projectsLoading: true,
    projectError: null,
    environmentDeletionRevision: 0,
    backingProjects: [],
    backingProjectsLoading: true,
    backingProjectError: null,
    runners: [],
    runnersLoading: true,
    runnerError: null,
    connectors: [],
    connectorsLoading: true,
    connectorError: null,
    reusableSecrets: [],
    reusableSecretsLoading: true,
    secretError: null,
    activity: [],
    taskJournals: { all: emptyTaskJournal() },
    platform: { ...seedPlatform, components: [], agents: [] },
    platformComponentsLoading: true,
    platformComponentError: null,
    managedConfigFiles: [],
    managedConfigLoading: false,
    managedConfigError: null,
    agentsLoading: true,
    agentError: null,
    agentConfig: null,
    agentConfigLoading: true,
    agentConfigError: null,
  });
}

export type StoreContext = State &
  BlueprintActions &
  ReusableSecretActions &
  ConnectorActions &
  RunnerActions &
  ServiceActions &
  NetworkActions &
  VolumeActions &
  ScriptActions &
  ReleaseGroupActions &
  AttachActions &
  EntryActions &
  TenantActions &
  ProjectActions &
  BackingServiceActions &
  AgentActions &
  TaskJournalActions &
  TaskEventActions &
  EnvironmentActions &
  ReleaseActions &
  ComponentActions &
  ComponentRefreshActions &
  ReturnType<typeof useControllerPlatform> &
  ReturnType<typeof useBackupStore> & {
    adapters: typeof seedAdapters;
    platform: typeof seedPlatform;
    watchLogs: (
      target: LogTarget,
      options: { tail: number; follow: boolean; signal: AbortSignal },
      onEvent: (event: TransientLogEvent) => void,
    ) => Promise<void>;
    setRequireRevealConfirm: (v: boolean) => void;
    refreshEnvironmentReleases: (
      environmentId: string,
      signal?: AbortSignal,
    ) => Promise<void>;
    refreshEnvironmentServices: (
      environmentId: string,
      signal?: AbortSignal,
    ) => Promise<void>;
    getTenant: (slug: string) => Tenant | undefined;
    getProject: (tenantSlug: string, slug: string) => Project | undefined;
    getProjectById: (id: string) => Project | undefined;
    getBackingProject: (id: string) => Project | undefined;
    getEnvironment: (
      tenantSlug: string,
      projectSlug: string,
      envName: string,
    ) => Environment | undefined;
    getEnvironmentDeletionFailure: (
      environmentId: string,
    ) => EnvironmentDeletionFailure | null;
    refreshEnvironmentDeletion: (environmentId: string) => Promise<void>;
    isEnvironmentDeletionPending: (environmentId: string) => boolean;
    // mutations
    retryTask: (taskId: string) => Promise<string>;
    getTask: (taskId: string, signal?: AbortSignal) => Promise<TaskResponse>;
    abortTask: (taskId: string) => Promise<void>;
  };
