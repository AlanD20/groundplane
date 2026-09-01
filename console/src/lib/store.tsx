'use client'
import { taskTypeFromAPI } from '@/lib/task-read-model'
import { assertOptionalBackupPolicyKeep } from '@/lib/backup-policy-contract'
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react'
import type { operations } from './api.generated'
import { watchTransientLogs, type LogTarget, type TransientLogEvent } from './transient-logs'

type BackingServiceCreateRequest = operations['backing-service.create']['requestBody']['content']['application/json']
type BackingServiceCreatedResponse = operations['backing-service.create']['responses'][201]['content']['application/json']
import { hydratePlatformComponents } from './platform-component-hydration'
import type { ConnectorMutationIntent } from './connector-intent'
import type {
  HealthState,
  ActivityEntry,
  Attach,
  BackupPolicyDocument,
  BackupPolicyReplacement,
  BackupPolicyState,
  Connector,
  ConnectorCreateInput,
  ConnectorCredential,
  ConnectorCredentialInput,
  EnvFile,
  DeployRecord,
  Environment,
  EnvironmentEntry,
  EnvironmentComponent,
  Project,
  ReleaseGroup,
  RecoveryPoint,
  RecoveryPointState,
  Route,
  Runner,
  Script,
  ReusableSecret,
  SecretKind,
  Service,
  ServiceRuntimeIntent,
  TaskStep,
  TaskJournalScope,
  TaskJournalState,
  TaskJournalSurface,
  TaskStatus,
  TaskType,
  Tenant,
  Volume,
  VolumeDeletionImpactPage,
  Zone,
  PlatformInfra,
  HostInfo,
} from './types'
import { createBlueprintMultipartBody, type BlueprintApplyRequest } from './blueprint-bundle'
import {
  adapters as seedAdapters,
  platform as seedPlatform,
} from './mock-data'
import { createEnvironmentComponents, routerProjection } from './components'
import { applyAuthoritativeEnvironmentScalars } from './environment-authoritative'
import { useEnvironmentLifecycle, type EnvironmentDeletionFailure, type TaskResponse } from './environment-lifecycle'
import {
  environmentMutationKey,
  loadEnvironmentMutationIntents,
  persistEnvironmentMutationIntents,
  type EnvironmentMutationIntent,
} from './environment-storage'
import { environmentFromAPI } from './environment-projection'
import { environmentGenerationSnapshot, mergeEnvironmentProjectLoads } from './environment-hydration'
import { environmentDeletionGuard } from './environment-guard'
import { observeEnvironmentTask } from './environment-task-observation'
import {
  createVolume,
  editVolume,
  getVolume,
  getVolumeDeletionImpact,
  listAllVolumes,
  removeVolume,
} from '@/features/volume/api'
import { newId, newULID } from './utils'
type TenantPageResponse = operations['tenant.list']['responses'][200]['content']['application/json']
type TenantCreateRequest = operations['tenant.create']['requestBody']['content']['application/json']
type TenantCreateResponse = operations['tenant.create']['responses'][201]['content']['application/json']
type TenantEditRequest = operations['tenant.edit']['requestBody']['content']['application/json']
type TenantEditResponse = operations['tenant.edit']['responses'][200]['content']['application/json']
type TenantRenameRequest = operations['tenant.rename']['requestBody']['content']['application/json']
type TenantRenameResponse = operations['tenant.rename']['responses'][200]['content']['application/json']
type ProjectPageResponse = operations['project.list']['responses'][200]['content']['application/json']
type ProjectCreateRequest = operations['project.create']['requestBody']['content']['application/json']
type ProjectCreateResponse = operations['project.create']['responses'][201]['content']['application/json']
type ProjectShowResponse = operations['project.show']['responses'][200]['content']['application/json']
type ProjectEditRequest = operations['project.edit']['requestBody']['content']['application/json']
type ProjectEditResponse = operations['project.edit']['responses'][200]['content']['application/json']
type ProjectRenameRequest = operations['project.rename']['requestBody']['content']['application/json']
type ProjectRenameResponse = operations['project.rename']['responses'][200]['content']['application/json']
type RunnerPageResponse = operations['runner.list']['responses'][200]['content']['application/json']
type RunnerCreateRequest = operations['runner.create']['requestBody']['content']['application/json']
type RunnerCreateResponse = operations['runner.create']['responses'][202]['content']['application/json']
type RunnerEditRequest = operations['runner.edit']['requestBody']['content']['application/json']
type RunnerEditResponse = operations['runner.edit']['responses'][200]['content']['application/json']
type RunnerRetryRequest = operations['runner.retry']['requestBody']['content']['application/json']
type RunnerRetryResponse = operations['runner.retry']['responses'][202]['content']['application/json']
type RunnerRemoveResponse = operations['runner.remove']['responses'][202]['content']['application/json']
type EnvironmentPageResponse = operations['environment.list']['responses'][200]['content']['application/json']
type EnvironmentResponse = operations['environment.show']['responses'][200]['content']['application/json']
type EnvironmentCreateRequest = operations['environment.create']['requestBody']['content']['application/json']
type EnvironmentTaskAccepted = operations['environment.create']['responses'][202]['content']['application/json']
type EnvironmentEditRequest = operations['environment.edit']['requestBody']['content']['application/json']
type EnvironmentEditResponse = operations['environment.edit']['responses'][200]['content']['application/json']
type EnvironmentRenameRequest = operations['environment.rename']['requestBody']['content']['application/json']
type EnvironmentRenameResponse = operations['environment.rename']['responses'][200]['content']['application/json']
type EnvironmentDeleteResponse = operations['environment.delete']['responses'][202]['content']['application/json']
type BackupPolicyShowResponse = operations['backup.policy.show']['responses'][200]['content']['application/json']
type BackupPolicySetRequest = operations['backup.policy.set']['requestBody']['content']['application/json']
type BackupPolicySetResponse = operations['backup.policy.set']['responses'][200]['content']['application/json']
type RecoveryPointPageResponse = operations['backup.points.list']['responses'][200]['content']['application/json']
type RecoveryPointPageItem = NonNullable<RecoveryPointPageResponse['items']>[number]
type BackupRunTaskAccepted = operations['backup.run']['responses'][202]['content']['application/json']
type BackupKeyRotateResponse = operations['backup.key.rotate']['responses'][202]['content']['application/json']
type TaskRetryResponse = operations['task.retry']['responses'][202]['content']['application/json']
type TaskAbortResponse = operations['task.abort']['responses'][202]['content']['application/json']
type ServiceRuntimeTaskAccepted = operations['service.start']['responses'][202]['content']['application/json']
type BackingRuntimeTaskAccepted = operations['backing-service.start']['responses'][202]['content']['application/json']
type ZonePageResponse = operations['zone.list']['responses'][200]['content']['application/json']
type ZoneCreateRequest = operations['zone.create']['requestBody']['content']['application/json']
type ZoneCreateResponse = operations['zone.create']['responses'][201]['content']['application/json']
type ZoneRemoveResponse = operations['zone.remove']['responses'][202]['content']['application/json']
type ZoneRemovalImpactResponse = operations['zone.removal-impact']['responses'][200]['content']['application/json']
type RoutePageResponse = operations['route.list']['responses'][200]['content']['application/json']
type RouteCreateRequest = operations['route.create']['requestBody']['content']['application/json']
type GeneratedRouteResponse = operations['route.show']['responses'][200]['content']['application/json']
type RouteCreateResponse = GeneratedRouteResponse & { status: Route['status'] }
type RouteCreateAccepted = { route: RouteCreateResponse; task_id: string }
type RouteEditRequest = operations['route.edit']['requestBody']['content']['application/json']
type RouteEditResponse = RouteCreateResponse
type RouteEditAccepted = { route: RouteEditResponse; task_id: string }
type RouteShowResponse = RouteCreateResponse
type ZoneShowResponse = operations['zone.show']['responses'][200]['content']['application/json']
type ScriptPageResponse = operations['script.list']['responses'][200]['content']['application/json']
type ScriptCreateRequest = operations['script.create']['requestBody']['content']['application/json']
type ScriptCreateResponse = operations['script.create']['responses'][201]['content']['application/json']
type ScriptEditRequest = operations['script.edit']['requestBody']['content']['application/json']
type ScriptEditResponse = operations['script.edit']['responses'][200]['content']['application/json']
type ScriptRunResponse = operations['script.run']['responses'][202]['content']['application/json']
type ServicePageResponse = operations['service.list']['responses'][200]['content']['application/json']
type ServiceCreateRequest = operations['service.create']['requestBody']['content']['application/json']
type ServiceCreateResponse = operations['service.create']['responses'][201]['content']['application/json']
type ServiceEditRequest = operations['service.edit']['requestBody']['content']['application/json']
type ServiceEditResponse = operations['service.edit']['responses'][200]['content']['application/json']
type ServiceShowResponse = operations['service.show']['responses'][200]['content']['application/json']
type EntryPageResponse = operations['entry.list']['responses'][200]['content']['application/json']
type EntryResponse = operations['entry.edit']['responses'][200]['content']['application/json']
type EntryCreateRequest = operations['entry.create']['requestBody']['content']['application/json']
type EntryEditRequest = operations['entry.edit']['requestBody']['content']['application/json']
type EntryBulkUpsertRequest = operations['entry.bulk-upsert']['requestBody']['content']['application/json']
type EntryBulkUpsertResponse = operations['entry.bulk-upsert']['responses'][202]['content']['application/json']
type EntryValueResponse = operations['entry.reveal']['responses'][200]['content']['application/json']
type AttachPageResponse = operations['attach.list']['responses'][200]['content']['application/json']
type AttachResponse = NonNullable<AttachPageResponse['items']>[number]
type AttachCreateRequest = operations['attach.create']['requestBody']['content']['application/json']
type AttachTaskAccepted = operations['attach.create']['responses'][202]['content']['application/json']
type AttachRenameRequest = operations['attach.rename']['requestBody']['content']['application/json']
type AttachRenameResponse = operations['attach.rename']['responses'][200]['content']['application/json']
type AttachFactValueResponse = operations['attach.fact.reveal']['responses'][200]['content']['application/json']
type BackingServicePageResponse = operations['backing-service.list']['responses'][200]['content']['application/json']
type BackingServiceResponse = NonNullable<BackingServicePageResponse['items']>[number]
type SecretPageResponse = operations['secret.list']['responses'][200]['content']['application/json']
type SecretResponse = operations['secret.create']['responses'][201]['content']['application/json']
type SecretCreateRequest = operations['secret.create']['requestBody']['content']['application/json']
type SecretTaskAccepted = operations['secret.remove']['responses'][202]['content']['application/json']
type SecretValueResponse = operations['secret.reveal']['responses'][200]['content']['application/json']
type ConnectorPageResponse = operations['connector.list']['responses'][200]['content']['application/json']
type ConnectorResponse = operations['connector.create']['responses'][201]['content']['application/json']
type ConnectorCreateRequest = operations['connector.create']['requestBody']['content']['application/json']
type ConnectorTaskAccepted = operations['connector.remove']['responses'][202]['content']['application/json']
export type BlueprintDocumentResponse =
  operations['blueprint.show']['responses'][200]['content']['application/json']
export type BlueprintValidationResponse =
  operations['blueprint.validate']['responses'][200]['content']['application/json']
type BlueprintTaskAccepted = operations['blueprint.apply']['responses'][202]['content']['application/json']
type HostResponse = operations['host.show']['responses'][200]['content']['application/json']
type TaskPageResponse = operations['task.list']['responses'][200]['content']['application/json']
type TaskPageItem = NonNullable<TaskPageResponse['items']>[number]
type TaskEventResponse = operations['task.events']['responses'][200]['content']['text/event-stream'][number]['data']
type AgentPageResponse = operations['agent.list']['responses'][200]['content']['application/json']
type AgentResponse = operations['agent.show']['responses'][200]['content']['application/json']
type AgentTaskAccepted = operations['agent.join']['responses'][202]['content']['application/json']
type AgentConfigResponse = operations['agent.config.show']['responses'][200]['content']['application/json']
type AgentConfigRequest = operations['agent.config.set']['requestBody']['content']['application/json']
type HierarchyTaskAccepted = { task_id: string }
type ComponentPageResponse = operations['component.list']['responses'][200]['content']['application/json']
type ComponentResponse = NonNullable<ComponentPageResponse['items']>[number]
type ComponentTaskAccepted = operations['component.enable']['responses'][202]['content']['application/json']
type ComponentConfigMutationResponse = operations['component-config.set']['responses'][200]['content']['application/json']

type ReleaseGroupMutationAccepted = operations['release-group.remove']['responses'][202]['content']['application/json']
type ReleaseGroupTaskAccepted = operations['release-group.deploy']['responses'][202]['content']['application/json']
type ReleaseGroupPageResponse = operations['release-group.list']['responses'][200]['content']['application/json']
type ReleaseGroupResponse = operations['release-group.show']['responses'][200]['content']['application/json']
type ReleasePageResponse = operations['release.list']['responses'][200]['content']['application/json']
export type ReleaseDetailResponse = operations['release.show']['responses'][200]['content']['application/json']

type ReusableSecretCreateInput = {
  key: string
  kind: SecretKind
  path?: string
  value: string
} & (
  | { projectId: string; platform?: never }
  | { projectId?: never; platform: true }
)
type ComponentConfigInput =
  | { zone_id: string; caddyfile_template?: string }
  | {
      credential:
        | { mode: 'existing'; secret_id: string }
        | { mode: 'new'; secret_name: string; token: string }
    }
  | {
      upstream_auto: boolean
      upstream_resolvers: string[]
      forwarders: { domain: string; resolvers: string[] }[]
      tailnet_delegation: boolean
      corefile_template: string
    }
type PendingConnectorRemoval = {
  connectorId: string
  environmentId: string
}
function waitForRequest<T>(request: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return request
  if (signal.aborted) return Promise.reject(signal.reason ?? new DOMException('Aborted', 'AbortError'))
  return new Promise<T>((resolve, reject) => {
    const abort = () => reject(signal.reason ?? new DOMException('Aborted', 'AbortError'))
    signal.addEventListener('abort', abort, { once: true })
    void request.then(resolve, reject).finally(() => signal.removeEventListener('abort', abort))
  })
}
const pendingConnectorRemovalStorageKey = 'groundplane-pending-connector-removals'
function loadPendingConnectorRemovals(): Map<string, PendingConnectorRemoval> {
  try {
    const stored = localStorage.getItem(pendingConnectorRemovalStorageKey)
    if (!stored) return new Map()
    const entries = JSON.parse(stored) as [string, PendingConnectorRemoval][]
    if (!Array.isArray(entries)) return new Map()
    return new Map(entries.filter(([taskId, pending]) => (
      typeof taskId === 'string' &&
      typeof pending?.connectorId === 'string' &&
      typeof pending?.environmentId === 'string'
    )))
  } catch {
    return new Map()
  }
}
function persistPendingConnectorRemovals(removals: Map<string, PendingConnectorRemoval>) {
  try {
    if (removals.size === 0) localStorage.removeItem(pendingConnectorRemovalStorageKey)
    else localStorage.setItem(pendingConnectorRemovalStorageKey, JSON.stringify([...removals]))
  } catch {
    /* private mode */
  }
}
const taskStatuses = new Set<TaskStatus>(['pending', 'running', 'completed', 'failed', 'timed_out', 'aborted'])
function emptyTaskJournal(): TaskJournalState {
  return {
    entries: [],
    nextCursor: null,
    loaded: false,
    loading: false,
    loadingMore: false,
    loadError: null,
    failedCursor: null,
  }
}
function taskJournalKey(scope: TaskJournalScope): string {
  switch (scope.kind) {
    case 'all':
      return 'all'
    case 'workspace':
      return `workspace:${scope.workspace}`
    case 'project':
      return `project:${scope.projectId}`
    case 'environment':
      return `environment:${scope.environmentId}`
  }
}
function taskJournalQuery(scope: TaskJournalScope, cursor?: string): string {
  const query = new URLSearchParams({ limit: '50' })
  if (cursor) query.set('cursor', cursor)
  if (scope.kind === 'workspace') query.set('workspace', scope.workspace)
  if (scope.kind === 'project') query.set('project', scope.projectId)
  if (scope.kind === 'environment') query.set('environment', scope.environmentId)
  return query.toString()
}
function requiredTaskTimestamp(value: string, field: string): string {
  if (Number.isNaN(Date.parse(value))) throw new Error(`Controller returned invalid Task ${field}`)
  return value
}
function taskStepState(status: string): TaskStep['state'] {
  switch (status) {
    case 'pending':
      return 'pending'
    case 'completed':
      return 'done'
    case 'running':
      return 'running'
    case 'failed':
    case 'timed_out':
    case 'aborted':
      return 'failed'
    default:
      throw new Error(`Controller returned unknown Task step status ${status}`)
  }
}
function taskTitle(type: TaskType, target: string): string {
  const labels: Record<TaskType, string> = {
    deploy: 'Deploy',
    rollback: 'Rollback',
    backup: 'Backup',
    backup_prune: 'Prune backups',
    restore: 'Restore',
    attach: 'Attach',
    detach: 'Detach',
    run: 'Run',
    script: 'Run script',
    provision: 'Provision',
    create: 'Create',
    start: 'Start',
    stop: 'Stop',
    destroy: 'Destroy',
    remove: 'Remove',
    update: 'Update',
    rotate: 'Rotate',
  }
  return `${labels[type]} · ${target}`
}
function taskFromAPI(value: TaskPageItem): ActivityEntry {
  const task = value
  if (!taskStatuses.has(task.status as TaskStatus)) throw new Error(`Controller returned unknown Task status ${task.status}`)
  if (task.workspace_type !== 'platform' && task.workspace_type !== 'tenant') {
    throw new Error(`Controller returned unknown Task workspace ${task.workspace_type}`)
  }
  if (task.actor !== 'operator' && task.actor !== 'system') {
    throw new Error(`Controller returned unknown Task actor ${task.actor}`)
  }
  if (task.workspace_type === 'tenant' && !task.tenant_id) {
    throw new Error('Controller returned a Tenant-owned Task without tenant_id')
  }
  if (task.workspace_type === 'platform' && task.tenant_id) {
    throw new Error('Controller returned a Platform-owned Task with tenant_id')
  }
  if (task.environment_id && !task.project_id) {
    throw new Error('Controller returned an Environment-owned Task without project_id')
  }

  const createdAt = requiredTaskTimestamp(task.created_at, 'created_at')
  const updatedAt = requiredTaskTimestamp(task.updated_at, 'updated_at')
  const startedAt = task.started_at ? requiredTaskTimestamp(task.started_at, 'started_at') : null
  const finishedAt = task.finished_at ? requiredTaskTimestamp(task.finished_at, 'finished_at') : null
  const type = taskTypeFromAPI(task.type, task.actor)

  return {
    id: task.id,
    operationId: task.operation_id,
    retryOf: task.retry_of,
    planHash: task.plan_hash,
    type,
    title: taskTitle(type, task.target),
    target: task.target,
    workspace: task.workspace_type === 'platform' ? 'platform' : task.tenant_id!,
    workspaceType: task.workspace_type,
    tenantId: task.tenant_id ?? undefined,
    projectId: task.project_id ?? undefined,
    environmentId: task.environment_id ?? undefined,
    status: task.status as TaskStatus,
    actor: task.actor,
    ts: updatedAt,
    createdAt,
    updatedAt,
    startedAt,
    finishedAt,
    steps: task.steps?.map((step) => ({ label: step.name, state: taskStepState(step.status) })),
  }
}

function tenantFromAPI(tenant: TenantCreateResponse): Tenant {
  return {
    id: tenant.id,
    slug: tenant.slug,
		name: tenant.name,
		description: tenant.description,
		deletionTaskId: tenant.deletion_task_id,
  }
}

type AttachCreateInput = {
  serviceId: string
  backingServiceId: string
  name?: string
  credential: { mode: 'new' } | { mode: 'existing'; attachId: string }
  grantAttachIds?: string[]
}

const taskEventKeys = ['attempt', 'ordinal', 'received_at', 'sequence', 'state', 'step_id']

function isTaskEventState(value: unknown): value is TaskEventResponse['state'] {
  return (
    value === 'pending' ||
    value === 'running' ||
    value === 'completed' ||
    value === 'failed' ||
    value === 'aborted' ||
    value === 'timed_out'
  )
}

function parseTaskEvent(data: string): TaskEventResponse {
  const value: unknown = JSON.parse(data)
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('task event must be an object')
  }

  const event = value as Record<string, unknown>
  const keys = Object.keys(event).sort()
  if (keys.length !== taskEventKeys.length || keys.some((key, index) => key !== taskEventKeys[index])) {
    throw new Error('task event has unexpected fields')
  }
  if (typeof event.sequence !== 'number' || !Number.isSafeInteger(event.sequence) || event.sequence < 1) {
    throw new Error('task event sequence is invalid')
  }
  if (typeof event.step_id !== 'string' || event.step_id.length === 0) {
    throw new Error('task event step id is invalid')
  }
  if (!isTaskEventState(event.state)) {
    throw new Error('task event state is invalid')
  }
  if (typeof event.attempt !== 'number' || !Number.isSafeInteger(event.attempt) || event.attempt < 1) {
    throw new Error('task event attempt is invalid')
  }
  if (typeof event.ordinal !== 'number' || !Number.isSafeInteger(event.ordinal) || event.ordinal < 1) {
    throw new Error('task event ordinal is invalid')
  }
  if (typeof event.received_at !== 'string' || Number.isNaN(Date.parse(event.received_at))) {
    throw new Error('task event received_at is invalid')
  }

  return {
    attempt: event.attempt,
    ordinal: event.ordinal,
    received_at: event.received_at,
    sequence: event.sequence,
    state: event.state,
    step_id: event.step_id,
  }
}

class ControllerRequestError extends Error {
  readonly responseReceived = true

  constructor(
    message: string,
    readonly status: number,
    readonly code?: string,
  ) {
    super(message)
    this.name = 'ControllerRequestError'
  }
}

class ControllerTransportError extends Error {
  readonly responseReceived = false

  constructor(message: string, readonly cause: unknown) {
    super(message)
    this.name = 'ControllerTransportError'
  }
}

export function isNoResponseTransportUncertainty(error: unknown): boolean {
  return error instanceof ControllerTransportError && !error.responseReceived
}

function isTaskNotFoundError(error: unknown): boolean {
  return error instanceof ControllerRequestError && (
    error.status === 404 || error.code === 'not_found'
  )
}

async function controllerResponseError(response: Response, method: string, path: string) {
  let detail = `${method} ${path} returned ${response.status}`
  let code: string | undefined
  try {
    const problem = (await response.json()) as { code?: string; detail?: string }
    if (problem.detail) detail = problem.detail
    code = problem.code
  } catch {
    // The status remains actionable even when a proxy returned a non-JSON body.
  }
  return new ControllerRequestError(detail, response.status, code)
}

async function tenantRequest<Response>(
  path: string,
  expectedStatus: number,
  init: {
    method?: 'GET' | 'POST' | 'PATCH' | 'PUT' | 'DELETE'
    body?: unknown
    signal?: AbortSignal
    idempotencyKey?: string
  } = {},
): Promise<Response> {
  const method = init.method ?? 'GET'
  const headers = new Headers({ Accept: 'application/json' })
  if (init.body !== undefined) headers.set('Content-Type', 'application/json')
  if (method !== 'GET') headers.set('Idempotency-Key', init.idempotencyKey ?? newULID())
  const requestBody = init.body === undefined ? undefined : JSON.stringify(init.body)
  let response: globalThis.Response
  try {
    response = await fetch(`/api/v1${path}`, {
      method,
      headers,
      body: requestBody,
      signal: init.signal,
    })
  } catch (error) {
    const detail = error instanceof Error ? error.message : 'request failed before an HTTP response'
    throw new ControllerTransportError(`${method} ${path}: ${detail}`, error)
  }
  if (response.status !== expectedStatus) {
    throw await controllerResponseError(response, method, path)
  }
  return (await response.json()) as Response
}

function requireTaskId(response: { task_id?: string | null }, operation: string): string {
  if (typeof response.task_id !== 'string' || response.task_id.length === 0) {
    throw new Error(`Controller response is missing ${operation} task_id`)
  }
  return response.task_id
}

async function listAllTenants(signal: AbortSignal): Promise<Tenant[]> {
  const tenants: Tenant[] = []
  let cursor = ''
  do {
    const query = new URLSearchParams({ limit: '200' })
    if (cursor) query.set('cursor', cursor)
    const page = await tenantRequest<TenantPageResponse>(`/tenants?${query}`, 200, { signal })
    tenants.push(...(page.items ?? []).map(tenantFromAPI))
    cursor = page.next_cursor ?? ''
  } while (cursor)
  return tenants
}

function agentFromAPI(agent: AgentResponse): PlatformInfra['agents'][number] {
  let status: HealthState
  switch (agent.status) {
    case 'pending':
    case 'healthy':
    case 'degraded':
    case 'stopped':
      status = agent.status
      break
    default:
      throw new Error(`Controller returned unknown Agent status ${agent.status}`)
  }
  return {
    id: agent.id,
    enrollmentTaskId: agent.enrollment_task_id,
    host: agent.host,
    status,
    version: agent.version ?? null,
    labels: { ...agent.labels },
    readyAt: agent.ready_at ?? null,
    lastReportAt: agent.last_report_at ?? null,
    inFlight: agent.in_flight,
  }
}

async function listAllAgents(signal?: AbortSignal): Promise<PlatformInfra['agents']> {
  const agents: PlatformInfra['agents'] = []
  let cursor = ''
  do {
    const query = new URLSearchParams({ limit: '200' })
    if (cursor) query.set('cursor', cursor)
    const page = await tenantRequest<AgentPageResponse>(`/agents?${query}`, 200, { signal })
    agents.push(...(page.items ?? []).map(agentFromAPI))
    cursor = page.next_cursor ?? ''
  } while (cursor)
  return agents
}

function projectFromAPI(project: ProjectCreateResponse | ProjectShowResponse): Project {
  if (project.kind !== 'tenant' && project.kind !== 'backing') {
    throw new Error(`Controller returned unknown project kind ${project.kind}`)
  }
  return {
    id: project.id,
    tenantId: project.tenant_id ?? null,
    slug: project.slug,
    name: project.name,
		description: project.description,
		kind: project.kind,
		deletionTaskId: project.deletion_task_id,
  }
}

function entryFromAPI(entry: EntryResponse): EnvironmentEntry {
  if (entry.type !== 'env' && entry.type !== 'file') {
    throw new Error(`Controller returned unknown Entry type ${entry.type}`)
  }
  if (!entry.exposure || entry.exposure.length === 0) {
    throw new Error(`Controller returned Entry ${entry.id} without exposure`)
  }
  let source: EnvironmentEntry['source']
  switch (entry.source.kind) {
    case 'literal':
      source = { kind: 'literal', literal: entry.source.literal }
      break
    case 'secret_ref':
      if (!entry.source.secret_ref) throw new Error(`Controller returned invalid secret_ref Entry ${entry.id}`)
      source = { kind: 'secret_ref', secretRef: entry.source.secret_ref }
      break
    case 'fact':
      if (!entry.source.attach_id || !entry.source.fact) {
        throw new Error(`Controller returned invalid fact Entry ${entry.id}`)
      }
      source = {
        kind: 'fact',
        attachId: entry.source.attach_id,
        grantAttachId: entry.source.grant_attach_id,
        fact: entry.source.fact,
      }
      break
    default:
      throw new Error(`Controller returned unknown Entry source ${entry.source.kind}`)
  }
  return {
    id: entry.id,
    type: entry.type,
    key: entry.key,
    path: entry.path,
    uid: entry.uid,
    gid: entry.gid,
    source,
    exposure: [...entry.exposure],
    secret: entry.secret,
  }
}

async function listAllEntries(environmentId: string, signal?: AbortSignal): Promise<EnvironmentEntry[]> {
  const entries: EnvironmentEntry[] = []
  let cursor = ''
  do {
    const query = new URLSearchParams({ environment: environmentId, limit: '200' })
    if (cursor) query.set('cursor', cursor)
    const page = await tenantRequest<EntryPageResponse>(`/entries?${query}`, 200, { signal })
    entries.push(...(page.items ?? []).map(entryFromAPI))
    cursor = page.next_cursor ?? ''
  } while (cursor)
  return entries
}

function zoneFromAPI(zone: ZoneCreateResponse | ZoneShowResponse): Zone {
  if (zone.owner_kind !== 'environment' && zone.owner_kind !== 'backing_project') {
    throw new Error(`Controller returned unknown Zone owner kind ${zone.owner_kind}`)
  }
  return {
    id: zone.id,
    environmentId: zone.environment_id,
    name: zone.name,
    subnet: zone.subnet,
    internal: zone.internal,
    ownerKind: zone.owner_kind,
    ownerId: zone.owner_id,
  }
}

async function listAllZones(environmentId: string, signal?: AbortSignal): Promise<Zone[]> {
  const zones: Zone[] = []
  let cursor = ''
  do {
    const query = new URLSearchParams({ environment: environmentId, limit: '200' })
    if (cursor) query.set('cursor', cursor)
    const page = await tenantRequest<ZonePageResponse>(`/zones?${query}`, 200, { signal })
    zones.push(...(page.items ?? []).map(zoneFromAPI))
    cursor = page.next_cursor ?? ''
  } while (cursor)
  return zones
}

function routeFromAPI(route: RouteCreateResponse | RouteEditResponse | RouteShowResponse): Route {
  if (route.exposure !== 'public' && route.exposure !== 'internal') {
    throw new Error(`Controller returned unknown Route exposure ${route.exposure}`)
  }
  if (route.status !== 'unserved' && route.status !== 'pending' && route.status !== 'served' && route.status !== 'degraded') {
    throw new Error(`Controller returned unknown Route status ${route.status}`)
  }
  return {
    id: route.id,
    environmentId: route.environment_id,
    host: route.host ?? '',
    path: route.path,
    exposure: route.exposure,
    targetServiceId: route.target_service_id,
    targetPort: route.target_port,
    status: route.status,
  }
}

async function listAllRoutes(environmentId: string, signal?: AbortSignal): Promise<Route[]> {
  const routes: Route[] = []
  let cursor = ''
  do {
    const query = new URLSearchParams({ environment: environmentId, limit: '200' })
    if (cursor) query.set('cursor', cursor)
    const page = await tenantRequest<RoutePageResponse>(`/routes?${query}`, 200, { signal })
    routes.push(...(page.items ?? []).map((route) => routeFromAPI(route as RouteShowResponse)))
    cursor = page.next_cursor ?? ''
  } while (cursor)
  return routes
}

const scriptHooks: Script['when'][] = [
  'manual',
  'pre-deploy',
  'post-deploy',
  'pre-rollback',
  'post-rollback',
  'on-failure',
]

function scriptFromAPI(script: ScriptCreateResponse | ScriptEditResponse): Script {
  if (!scriptHooks.includes(script.when as Script['when'])) {
    throw new Error(`Controller returned unknown Script hook ${script.when}`)
  }
  return {
    id: script.id,
    environmentId: script.environment_id,
    slug: script.slug,
    serviceId: script.service_id,
    service: script.service,
    body: script.script,
    when: script.when as Script['when'],
    origin: script.origin,
    reconciliationKey: script.reconciliation_key,
    activeGeneration: script.active_generation,
  }
}

async function listAllScripts(environmentId: string, signal?: AbortSignal): Promise<Script[]> {
  const scripts: Script[] = []
  let cursor = ''
  do {
    const query = new URLSearchParams({ environment: environmentId, limit: '200' })
    if (cursor) query.set('cursor', cursor)
    const page = await tenantRequest<ScriptPageResponse>(`/scripts?${query}`, 200, { signal })
    scripts.push(...(page.items ?? []).map(scriptFromAPI))
    cursor = page.next_cursor ?? ''
  } while (cursor)
  return scripts
}

type ServiceMutationInput = Pick<Service, 'name' | 'image' | 'zones' | 'strategy' | 'onFailure' | 'healthcheck' | 'resources' | 'expose' | 'restart' | 'replicas'>

function serviceFromAPI(service: ServiceCreateResponse | ServiceEditResponse | ServiceShowResponse): Service {
  if (service.runtime_intent !== 'running' && service.runtime_intent !== 'stopped' && service.runtime_intent !== 'absent') throw new Error(`Controller returned unknown Service runtime intent ${service.runtime_intent}`)
  const strategy = service.strategy || 'recreate'
  if (strategy !== 'blue-green' && strategy !== 'recreate' && strategy !== 'rolling') throw new Error(`Controller returned unknown Service strategy ${strategy}`)
  const onFailure = service.on_failure ?? 'switch_back'
  if (onFailure !== 'switch_back' && onFailure !== 'leave_active') throw new Error(`Controller returned unknown Service failure policy ${onFailure}`)
  const healthcheck = service.healthcheck
    ? service.healthcheck.http
      ? { kind: 'http' as const, target: service.healthcheck.http, interval: service.healthcheck.interval ?? '', timeout: service.healthcheck.timeout ?? '', startPeriod: service.healthcheck.start_period ?? '', retries: service.healthcheck.retries ?? 0 }
      : service.healthcheck.tcp
        ? { kind: 'tcp' as const, target: service.healthcheck.tcp, interval: service.healthcheck.interval ?? '', timeout: service.healthcheck.timeout ?? '', startPeriod: service.healthcheck.start_period ?? '', retries: service.healthcheck.retries ?? 0 }
        : service.healthcheck.pgrep
          ? { kind: 'pgrep' as const, target: service.healthcheck.pgrep, interval: service.healthcheck.interval ?? '', timeout: service.healthcheck.timeout ?? '', startPeriod: service.healthcheck.start_period ?? '', retries: service.healthcheck.retries ?? 0 }
          : null
    : null
  const restart = service.restart === 'always' || service.restart === 'unless-stopped' ? service.restart : 'no'
  return {
    id: service.id, name: service.name, image: service.image, role: service.label ?? '',
    zones: [...(service.zones ?? [])], strategy, onFailure, healthcheck,
    resources: { mem: service.resources?.mem ?? '', cpus: String(service.resources?.cpus ?? 0) },
    command: service.command?.join(' '),
    mounts: (service.mounts ?? []).map((mount) => mount.volume ? { type: 'volume' as const, volume: mount.volume, mount: mount.mount } : { type: 'file' as const, file: mount.file ?? '', mount: mount.mount, ro: mount.ro ?? false }),
    envFiles: [], environment: [], aliases: Object.values(service.aliases ?? {}).flatMap((values) => values ?? []),
    dependsOn: Object.keys(service.depends_on ?? {}), expose: [...(service.expose ?? [])], restart,
    replicas: service.replicas ?? 1, runtimeIntent: service.runtime_intent,
    status: 'unknown',
    adapter: service.adapter, serviceName: service.name, prefix: service.facts_prefix,
		nativeCompose: 'native_compose' in service ? service.native_compose : undefined,
		releaseLedger: 'release_ledger' in service
			? (service.release_ledger.items ?? []).map((release) => releaseForServiceName(release, service.name))
			: undefined,
  }
}

function serviceMutationBody(input: ServiceMutationInput) {
  const healthcheck = input.healthcheck ? {
    http: input.healthcheck.kind === 'http' ? input.healthcheck.target : undefined,
    tcp: input.healthcheck.kind === 'tcp' ? input.healthcheck.target : undefined,
    pgrep: input.healthcheck.kind === 'pgrep' ? input.healthcheck.target : undefined,
    interval: input.healthcheck.interval, timeout: input.healthcheck.timeout,
    start_period: input.healthcheck.startPeriod, retries: input.healthcheck.retries,
  } : {}
  return {
    image: input.image, zones: input.zones, strategy: input.strategy,
    on_failure: input.onFailure ?? 'switch_back', healthcheck,
    resources: { mem: input.resources.mem, cpus: Number(input.resources.cpus) },
    expose: input.expose, restart: input.restart, replicas: input.replicas,
  }
}

async function listAllServices(environmentId: string, signal?: AbortSignal): Promise<Service[]> {
  const services: Service[] = []
  let cursor = ''
  do {
    const query = new URLSearchParams({ environment: environmentId, limit: '200' })
    if (cursor) query.set('cursor', cursor)
    const page = await tenantRequest<ServicePageResponse>(`/services?${query}`, 200, { signal })
    services.push(...(page.items ?? []).map(serviceFromAPI))
    cursor = page.next_cursor ?? ''
  } while (cursor)
  return services
}

function releaseFromAPI(
  release: NonNullable<ReleasePageResponse['items']>[number],
  services: Service[],
): DeployRecord {
	return releaseForServiceName(
		release,
		services.find((service) => service.id === release.service_id)?.name ?? release.service_id,
	)
}

function releaseForServiceName(
	release: NonNullable<ReleasePageResponse['items']>[number],
	serviceName: string,
): DeployRecord {
  const strategy = release.strategy === 'blue-green' ? 'blue-green' : 'recreate'
  return {
    id: release.id,
		service: serviceName,
    tag: release.tag,
    digest: release.digest ?? '',
    strategy,
    when: release.completed_at ?? release.created_at,
    status: release.serving ? 'active' : 'superseded',
  }
}

async function listAllReleases(environmentId: string, services: Service[], signal?: AbortSignal): Promise<DeployRecord[]> {
  const releases: DeployRecord[] = []
  let cursor = ''
  do {
    const query = new URLSearchParams({ environment_id: environmentId, limit: '200' })
    if (cursor) query.set('cursor', cursor)
    const page = await tenantRequest<ReleasePageResponse>(`/releases?${query}`, 200, { signal })
    releases.push(...(page.items ?? []).map((release) => releaseFromAPI(release, services)))
    cursor = page.next_cursor ?? ''
  } while (cursor)
  return releases
}

function projectReleaseSummary(deploys: DeployRecord[]): Pick<Environment, 'release' | 'previousRelease' | 'lastDeployAt'> {
  const activeTags = [...new Set(deploys.filter((deploy) => deploy.status === 'active').map((deploy) => deploy.tag))]
  const previousTags = [...new Set(
    deploys
      .filter((deploy) => deploy.status === 'superseded' && !activeTags.includes(deploy.tag))
      .map((deploy) => deploy.tag),
  )]
  const latest = deploys.reduce<DeployRecord | undefined>(
    (current, candidate) => !current || candidate.when > current.when ? candidate : current,
    undefined,
  )
  return {
    release: activeTags.length === 0 ? 'none' : activeTags.length === 1 ? activeTags[0] : 'mixed',
    previousRelease: previousTags.length === 1 ? previousTags[0] : undefined,
    lastDeployAt: latest?.when ?? 'never',
  }
}

async function listAllReleaseGroups(environmentId: string, services: Service[], signal?: AbortSignal): Promise<ReleaseGroup[]> {
  const groups: ReleaseGroup[] = []
  const names = new Map(services.map((service) => [service.id, service.name]))
  let cursor = ''
  do {
    const query = new URLSearchParams({ environment_id: environmentId, limit: '200' })
    if (cursor) query.set('cursor', cursor)
    const page = await tenantRequest<ReleaseGroupPageResponse>(`/release-groups?${query}`, 200, { signal })
    groups.push(...(page.items ?? []).map((group) => ({
      id: group.id,
      name: group.name,
      services: (group.service_ids ?? []).map((id) => names.get(id) ?? id),
      order: (group.order ?? []).map((id) => names.get(id) ?? id),
      tag: group.tag,
      onFailure: group.on_failure === 'leave_active' ? ('leave_active' as const) : ('switch_back' as const),
    })))
    cursor = page.next_cursor ?? ''
  } while (cursor)
  return groups
}

export function fetchReleaseDetail(id: string, signal?: AbortSignal): Promise<ReleaseDetailResponse> {
  return tenantRequest<ReleaseDetailResponse>(`/releases/${encodeURIComponent(id)}`, 200, { signal })
}

function attachHealth(status: string): HealthState {
  if (status === 'ready') return 'healthy'
  if (status === 'failed') return 'failed'
  if (status === 'detached') return 'stopped'
  return 'pending'
}

async function revealAttachFactValue(
  attachId: string,
  key: string,
  grantAttachId?: string,
  signal?: AbortSignal,
): Promise<string> {
  const query = new URLSearchParams()
  if (grantAttachId) query.set('grant_attach_id', grantAttachId)
  const suffix = query.size > 0 ? `?${query}` : ''
  const response = await tenantRequest<AttachFactValueResponse>(
    `/attaches/${encodeURIComponent(attachId)}/facts/${encodeURIComponent(key)}${suffix}`,
    200,
    { signal },
  )
  return response.value
}

async function attachFromAPI(attach: AttachResponse, services: Service[], signal?: AbortSignal): Promise<Attach> {
  const factSets = (attach.fact_sets ?? []).map((set) => ({
    grantAttachId: set.grant_attach_id,
    facts: (set.facts ?? []).map((fact) => ({ key: fact.key, secret: fact.secret })),
  }))
  const ready = attach.status === 'ready'
  const revealSuffix = async (set: typeof factSets[number] | undefined, suffix: string) => {
    const fact = set?.facts.find((candidate) => !candidate.secret && candidate.key.endsWith(suffix))
    if (!ready || !fact) return ''
    return revealAttachFactValue(attach.id, fact.key, set?.grantAttachId, signal)
  }
  const own = factSets.find((set) => !set.grantAttachId)
  const [database, role, ...grants] = await Promise.all([
    revealSuffix(own, '_DATABASE'),
    revealSuffix(own, '_ROLE'),
    ...factSets.filter((set) => set.grantAttachId).map((set) => revealSuffix(set, '_DATABASE')),
  ])
  const serviceId = attach.service_id
  return {
    id: attach.id,
    name: attach.name,
    backingProjectId: attach.backing_project_id,
    backingServiceId: attach.backing_service_id,
    backingEnvironmentId: attach.backing_environment_id,
    backingNetworkId: attach.backing_network_id,
    serviceId,
    credential: {
      mode: attach.credential.mode,
      attachId: attach.credential.attach_id,
    },
    grantAttachIds: [...(attach.grant_attach_ids ?? [])],
    factSets,
    projectId: attach.backing_project_id,
    database: database || '—',
    role,
    service: services.find((service) => service.id === serviceId)?.name ?? serviceId,
    grants: grants.filter(Boolean),
    status: attachHealth(attach.status),
  }
}

async function listAllAttaches(environmentId: string, services: Service[], signal?: AbortSignal): Promise<Attach[]> {
  const attaches: AttachResponse[] = []
  let cursor = ''
  do {
    const query = new URLSearchParams({ environment: environmentId, limit: '200' })
    if (cursor) query.set('cursor', cursor)
    const page = await tenantRequest<AttachPageResponse>(`/attaches?${query}`, 200, { signal })
    attaches.push(...(page.items ?? []))
    cursor = page.next_cursor ?? ''
  } while (cursor)
  return Promise.all(attaches.map((attach) => attachFromAPI(attach, services, signal)))
}

async function listAllEnvironments(projectId: string, signal?: AbortSignal): Promise<Environment[]> {
  const environments: Environment[] = []
  let cursor = ''
  do {
    const query = new URLSearchParams({ project: projectId, limit: '200' })
    if (cursor) query.set('cursor', cursor)
    const page = await tenantRequest<EnvironmentPageResponse>(`/environments?${query}`, 200, { signal })
		for (const item of page.items ?? []) {
			const environment = environmentFromAPI(item)
			environments.push(environment)
		}
    cursor = page.next_cursor ?? ''
  } while (cursor)
  return Promise.all(environments.map(async (environment) => {
    const [zones, routes, services, entries, scripts, volumes, components] = await Promise.all([
      listAllZones(environment.id, signal),
      listAllRoutes(environment.id, signal),
      listAllServices(environment.id, signal),
      listAllEntries(environment.id, signal),
      listAllScripts(environment.id, signal),
      listAllVolumes(tenantRequest, environment.id, signal),
      listAllComponents(environment.id, signal),
    ])
    const [attaches, deploys, releaseGroups] = await Promise.all([
      listAllAttaches(environment.id, services, signal),
      listAllReleases(environment.id, services, signal),
      listAllReleaseGroups(environment.id, services, signal),
    ])
    return { ...environment, ...projectReleaseSummary(deploys), zones, routes, services, entries, scripts, attaches, deploys, releaseGroups, volumes, components }
  }))
}

async function listAllComponents(environmentId: string, signal?: AbortSignal): Promise<EnvironmentComponent[]> {
  const page = await tenantRequest<ComponentPageResponse>(
    `/components?environment=${encodeURIComponent(environmentId)}&limit=200`,
    200,
    { signal },
  )
  return (page.items ?? []).map((item) => environmentComponentFromAPI(item, environmentId))
}

function environmentComponentFromAPI(item: ComponentResponse, environmentId: string): EnvironmentComponent {
  if (item.owner !== 'environment' || item.owner_id !== environmentId || item.environment_id !== environmentId) {
    throw new Error('Controller returned a Component outside the Environment owner scope')
  }
  const common = {
    id: item.id,
    owner: 'environment' as const,
    ownerRef: environmentId,
    enabled: item.enabled,
    status: componentHealthState(item.status),
    dependencies: [] as string[],
    generatedServices: item.generated_services ?? [],
  }
	const config = item.config
	if (item.kind === 'caddy') {
		if (!config) {
			return { ...common, kind: 'caddy', config: null, state: item.pinned_ipv4 ? { pinnedIPv4: item.pinned_ipv4 } : {} }
		}
		const zoneID = 'zone_id' in config ? config.zone_id : undefined
		const caddyfileTemplate = 'caddyfile_template' in config ? config.caddyfile_template : undefined
		if (typeof zoneID !== 'string' || (caddyfileTemplate !== undefined && typeof caddyfileTemplate !== 'string')) {
			throw new Error('Controller returned invalid Caddy Component configuration')
		}
		return {
      ...common,
		kind: 'caddy',
		config: { zone_id: zoneID, ...(caddyfileTemplate === undefined ? {} : { caddyfile_template: caddyfileTemplate }) },
      state: item.pinned_ipv4 ? { pinnedIPv4: item.pinned_ipv4 } : {},
    }
	}
	if (item.kind === 'cloudflare-tunnel') {
		if (!config) {
			return { ...common, kind: 'cloudflare-tunnel', config: null, state: {} }
		}
		const secretID = 'secret_id' in config ? config.secret_id : undefined
		if (typeof secretID !== 'string') {
			throw new Error('Controller returned invalid Cloudflare Tunnel Component configuration')
		}
		return {
      ...common,
      kind: 'cloudflare-tunnel',
		config: { secret_id: secretID },
      state: {},
    }
  }
  throw new Error(`Controller returned unknown Environment Component kind ${item.kind}`)
}

function componentHealthState(status: string | undefined): HealthState {
  switch (status) {
    case 'disabled': return 'stopped'
    case 'pending': return 'pending'
    case 'healthy': return 'healthy'
    case 'degraded': return 'degraded'
    default: return 'unknown'
  }
}

async function listPlatformComponents(signal?: AbortSignal): Promise<ComponentResponse[]> {
  const page = await tenantRequest<ComponentPageResponse>('/components?platform=true&limit=200', 200, { signal })
  return page.items ?? []
}

async function listAllTenantProjects(signal: AbortSignal): Promise<Project[]> {
  const projects: Project[] = []
  let cursor = ''
  do {
    const query = new URLSearchParams({ kind: 'tenant', limit: '200' })
    if (cursor) query.set('cursor', cursor)
    const page = await tenantRequest<ProjectPageResponse>(`/projects?${query}`, 200, { signal })
    projects.push(...(page.items ?? []).map(projectFromAPI))
    cursor = page.next_cursor ?? ''
  } while (cursor)
  return Promise.all(projects.map(async (project) => ({
    ...project,
    environments: await listAllEnvironments(project.id, signal),
  })))
}

function backingConsumers(
  backingProjectId: string,
  projects: Project[],
  tenants: Tenant[],
): NonNullable<Project['consumers']> {
  return projects.flatMap((project) => {
    const tenant = tenants.find((candidate) => candidate.id === project.tenantId)
    return (project.environments ?? []).flatMap((environment) =>
      environment.attaches
        .filter((attach) => attach.backingProjectId === backingProjectId)
        .flatMap((attach) => {
          const connectionFactKey = attach.factSets
            .find((set) => !set.grantAttachId)
            ?.facts.find((fact) => fact.key.endsWith('_URL'))?.key
          return [{
            tenant: tenant?.slug ?? project.tenantId ?? '',
            project: project.slug,
            environment: environment.name,
            service: attach.service,
            attachId: attach.id,
            database: attach.database,
            role: attach.role,
            connectionFactKey,
          }]
        }),
    )
  })
}

function backingConsumerRevision(projects: Project[], tenants: Tenant[]): string {
  return JSON.stringify({
    tenants: tenants.map((tenant) => [tenant.id, tenant.slug]),
    projects: projects.map((project) => [
      project.id,
      project.tenantId ?? '',
      project.slug,
      (project.environments ?? []).map((environment) => [
        environment.id,
        environment.name,
        environment.attaches.map((attach) => [
          attach.id,
          attach.backingProjectId,
          attach.service,
          attach.database,
          attach.role,
          attach.factSets.map((set) => [
            set.grantAttachId ?? '',
            set.facts.filter((fact) => fact.key.endsWith('_URL')).map((fact) => fact.key),
          ]),
        ]),
      ]),
    ]),
  })
}

async function listAllBackingProjects(
  tenantProjects: Project[],
  tenants: Tenant[],
  signal: AbortSignal,
): Promise<Project[]> {
  const facades: BackingServiceResponse[] = []
  let cursor = ''
  do {
    const query = new URLSearchParams({ limit: '200' })
    if (cursor) query.set('cursor', cursor)
    const page = await tenantRequest<BackingServicePageResponse>(`/backing-services?${query}`, 200, { signal })
    facades.push(...(page.items ?? []))
    cursor = page.next_cursor ?? ''
  } while (cursor)
  return Promise.all(facades.map(async (facade) => {
    const [projectResponse, environmentResponse, serviceResponse, zones, entries] = await Promise.all([
      tenantRequest<ProjectShowResponse>(`/projects/${encodeURIComponent(facade.project_id)}`, 200, { signal }),
      tenantRequest<EnvironmentResponse>(`/environments/${encodeURIComponent(facade.environment_id)}`, 200, { signal }),
      tenantRequest<ServiceShowResponse>(`/services/${encodeURIComponent(facade.service_id)}`, 200, { signal }),
      listAllZones(facade.environment_id, signal),
      listAllEntries(facade.environment_id, signal),
    ])
    const service = serviceFromAPI(serviceResponse)
    const environment = {
      ...environmentFromAPI(environmentResponse),
      zones,
      services: [service],
      entries,
      routes: [],
      attaches: [],
      components: [],
      backup: undefined,
    }
    const project = projectFromAPI(projectResponse)
    return {
      ...project,
      environments: [environment],
      status: environment.status,
      consumers: backingConsumers(facade.project_id, tenantProjects, tenants),
    }
  }))
}

type ExpectedSecretScope = { projectId: string } | { platform: true }

function reusableSecretFromAPI(secret: SecretResponse, expectedScope: ExpectedSecretScope): ReusableSecret {
  const kind: SecretKind | null = secret.kind === 'env_var' ? 'env' : secret.kind === 'file' ? 'file' : null
  if (!kind) throw new Error('Controller returned a reusable Secret with an invalid kind')

  const base = { id: secret.id, key: secret.key, kind, ref: secret.ref, updatedAt: secret.updated_at }

  if ('projectId' in expectedScope) {
    if (secret.scope !== 'project' || secret.project_id !== expectedScope.projectId) {
      throw new Error('Controller returned a reusable Secret for the wrong project owner')
    }
    return { ...base, scope: 'project', projectId: expectedScope.projectId }
  }
  if (secret.scope !== 'platform' || secret.project_id !== undefined) {
    throw new Error('Controller returned a reusable Secret for the wrong platform owner')
  }
  return { ...base, scope: 'platform' }
}

async function listReusableSecretScope(expectedScope: ExpectedSecretScope, signal?: AbortSignal): Promise<ReusableSecret[]> {
  const secrets: ReusableSecret[] = []
  let cursor = ''
  do {
    const query = new URLSearchParams({ limit: '200' })
    if ('projectId' in expectedScope) query.set('project', expectedScope.projectId)
    else query.set('platform', 'true')
    if (cursor) query.set('cursor', cursor)
    const page = await tenantRequest<SecretPageResponse>('/secrets?' + query.toString(), 200, { signal })
    secrets.push(...(page.items ?? []).map((secret) => reusableSecretFromAPI(secret, expectedScope)))
    cursor = page.next_cursor ?? ''
  } while (cursor)
  return secrets
}

function connectorFromAPI(connector: ConnectorResponse): Connector {
  const credential = (name: 'access_key' | 'secret_key'): ConnectorCredential => {
    const source = connector.credentials[name]
    if (!source) throw new Error(`Controller returned Connector ${connector.id} without ${name}`)
    if (source.kind === 'secret_ref' && source.secret_ref) return { kind: 'ref', name: source.secret_ref }
    if (source.kind === 'direct') return { kind: 'direct' }
    throw new Error(`Controller returned invalid Connector credential ${name}`)
  }
  if (connector.kind !== 's3-compatible') {
    throw new Error(`Controller returned unknown Connector kind ${connector.kind}`)
  }
  return {
    id: connector.id,
    name: connector.name,
    kind: connector.kind,
    scope: 'environment',
    scopeRef: connector.environment_id,
    endpoint: connector.endpoint,
    bucket: connector.bucket,
    prefix: connector.prefix ?? '',
    region: connector.region,
    pathStyle: connector.path_style,
    credentials: {
      accessKey: credential('access_key'),
      secretKey: credential('secret_key'),
    },
  }
}

async function listEnvironmentConnectors(environmentId: string, signal: AbortSignal): Promise<Connector[]> {
  const connectors: Connector[] = []
  let cursor = ''
  do {
    const query = new URLSearchParams({ environment: environmentId, limit: '200' })
    if (cursor) query.set('cursor', cursor)
    const page = await tenantRequest<ConnectorPageResponse>(`/connectors?${query}`, 200, { signal })
    connectors.push(...(page.items ?? []).map(connectorFromAPI))
    cursor = page.next_cursor ?? ''
  } while (cursor)
  return connectors
}

async function listAllConnectors(environmentIds: string[], signal: AbortSignal): Promise<Connector[]> {
  return (await Promise.all(environmentIds.map((id) => listEnvironmentConnectors(id, signal)))).flat()
}

function backupPolicyFromAPI(policy: BackupPolicyShowResponse | BackupPolicySetResponse): BackupPolicyDocument {
  assertOptionalBackupPolicyKeep(policy.keep, 'Backup policy response keep')
  if (policy.encryption !== undefined && policy.encryption !== 'age' && policy.encryption !== 'none') {
    throw new Error(`Controller returned unknown Backup Policy encryption ${policy.encryption}`)
  }
  return {
    enabled: policy.enabled,
    nextRunAt: policy.next_run_at,
    frequency: policy.frequency,
    keep: policy.keep,
    encryption: policy.encryption,
    connectorId: policy.connector_id,
    sources: (policy.sources ?? []).map((source) => {
      if (source.kind !== 'attach' && source.kind !== 'volume' && source.kind !== 'config') {
        throw new Error(`Controller returned unknown Backup Policy source kind ${source.kind}`)
      }
      return { id: source.id, kind: source.kind, targetId: source.target_id }
    }),
    ageRecipient: policy.age_recipient,
    keyEra: policy.key_era,
    keyCreatedAt: policy.key_created_at,
    keyRotatedAt: policy.key_rotated_at,
  }
}

function backupPolicyRequest(input: BackupPolicyReplacement): BackupPolicySetRequest {
  assertOptionalBackupPolicyKeep(input.keep, 'Backup policy request keep')
  return {
    enabled: input.enabled,
    frequency: input.frequency,
    keep: input.keep,
    encryption: input.encryption,
    connector_id: input.connectorId,
    sources: input.sources.map((source) => ({ kind: source.kind, target_id: source.targetId })),
  }
}

function emptyBackupPolicyState(): BackupPolicyState {
  return {
    policy: { enabled: false, nextRunAt: null, sources: [] },
    attaches: [],
    volumes: [],
    loaded: false,
    loading: false,
    saving: false,
    loadError: null,
    saveError: null,
    recoveryPoints: emptyRecoveryPointState(),
  }
}

function emptyRecoveryPointState(): RecoveryPointState {
  return {
    items: [],
    nextCursor: null,
    loaded: false,
    loading: false,
    loadingMore: false,
    loadError: null,
    failedCursor: null,
  }
}

function recoveryPointFromAPI(point: RecoveryPointPageItem): RecoveryPoint {
  if (point.status !== 'verified') throw new Error(`Controller returned unknown Recovery Point status ${point.status}`)
  if (!point.id || !point.source_id || !point.target_id || Number.isNaN(Date.parse(point.created_at))) {
    throw new Error('Controller returned an invalid Recovery Point identity')
  }
  if (!Number.isSafeInteger(point.size_bytes) || point.size_bytes <= 0) {
    throw new Error('Controller returned an invalid Recovery Point size')
  }
  if (point.encrypted) {
    const keyEra = point.key_era ?? 0
    if (!Number.isSafeInteger(keyEra) || keyEra < 1) {
      throw new Error('Controller returned an invalid encrypted Recovery Point era')
    }
    return {
      id: point.id,
      sourceId: point.source_id,
      sourceKind: point.source_kind,
      targetId: point.target_id,
      createdAt: point.created_at,
      sizeBytes: point.size_bytes,
      encrypted: true,
      keyEra,
      status: 'verified',
    }
  }
  if (point.key_era !== undefined) throw new Error('Controller returned an era for an unencrypted Recovery Point')
  return {
    id: point.id,
    sourceId: point.source_id,
    sourceKind: point.source_kind,
    targetId: point.target_id,
    createdAt: point.created_at,
    sizeBytes: point.size_bytes,
    encrypted: false,
    status: 'verified',
  }
}

async function listBackupPolicyAttaches(environmentId: string): Promise<BackupPolicyState['attaches']> {
  const attaches: BackupPolicyState['attaches'] = []
  let cursor = ''
  do {
    const query = new URLSearchParams({ environment: environmentId, limit: '200' })
    if (cursor) query.set('cursor', cursor)
    const page = await tenantRequest<AttachPageResponse>(`/attaches?${query}`, 200)
    attaches.push(...(page.items ?? []).map((attach) => ({
      id: attach.id,
      name: attach.name,
      backingProjectId: attach.backing_project_id,
      backingServiceId: attach.backing_service_id,
    })))
    cursor = page.next_cursor ?? ''
  } while (cursor)
  return attaches
}

async function listBackupPolicyVolumes(environmentId: string): Promise<BackupPolicyState['volumes']> {
  const volumes = await listAllVolumes(tenantRequest, environmentId)
  return volumes.map((volume) => ({ id: volume.id, slug: volume.slug, key: volume.key }))
}

function mutableBackupPolicyState(state: State, environmentId: string): BackupPolicyState {
  state.backupPolicies[environmentId] ??= emptyBackupPolicyState()
  return state.backupPolicies[environmentId]
}

function hostFromAPI(host: HostResponse): HostInfo {
  return {
    hostname: host.hostname,
    arch: host.arch,
    os: host.os,
    uptime: host.uptime,
    cpu: { model: host.cpu.model, cores: host.cpu.cores, load: host.cpu.load },
    memory: { total: host.memory.total, used: host.memory.used, usedPct: host.memory.used_pct },
    disk: { total: host.disk.total, used: host.disk.used, usedPct: host.disk.used_pct },
    swap: { total: host.swap.total, used: host.swap.used, usedPct: host.swap.used_pct },
    docker: host.docker,
    etcd: { node: host.etcd.node, status: host.etcd.status as HealthState, dbSize: host.etcd.db_size },
    controller: {
      service: host.controller.service,
      status: host.controller.status as HealthState,
      version: host.controller.version,
    },
    agent: {
      status: host.agent.status as HealthState,
      pullInterval: host.agent.pull_interval,
      maxConcurrent: host.agent.max_concurrent,
      labels: [...(host.agent.labels ?? [])],
    },
  }
}

function refreshReleaseGroupTags(environment: Environment) {
  for (const group of environment.releaseGroups) {
    const activeTags = group.order.map(
      (service) => environment.deploys.find(
        (record) => record.service === service && record.status === 'active',
      )?.tag,
    )
    const distinct = new Set(activeTags)
    group.tag = activeTags.every(Boolean) && distinct.size === 1 ? activeTags[0] : undefined
  }
}

type State = {
  // UI preference: typed confirmation before revealing a secret value
  requireRevealConfirm: boolean
  tenants: Tenant[]
  tenantsLoading: boolean
  tenantError: string | null
  tenantProjects: Project[]
  projectsLoading: boolean
  projectError: string | null
  environmentDeletionRevision: number
  backingProjects: Project[]
  backingProjectsLoading: boolean
  backingProjectError: string | null
  runners: Runner[]
  runnersLoading: boolean
  runnerError: string | null
  connectors: Connector[]
  connectorsLoading: boolean
  connectorError: string | null
  backupPolicies: Record<string, BackupPolicyState>
  reusableSecrets: ReusableSecret[]
  reusableSecretsLoading: boolean
  secretError: string | null
  activity: ActivityEntry[]
  taskJournals: Record<string, TaskJournalState>
  platform: PlatformInfra
  platformComponentsLoading: boolean
  platformComponentError: string | null
  agentsLoading: boolean
  agentError: string | null
  agentConfig: AgentConfigResponse | null
  agentConfigLoading: boolean
  agentConfigError: string | null
  host: HostInfo | null
  hostLoading: boolean
  hostError: string | null
}

function seed(): State {
  let requireRevealConfirm = false
  try {
    requireRevealConfirm = typeof window !== 'undefined' && localStorage.getItem('groundplane-reveal-confirm') === '1'
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
    backupPolicies: {},
    reusableSecrets: [],
    reusableSecretsLoading: true,
    secretError: null,
    activity: [],
    taskJournals: { all: emptyTaskJournal() },
    platform: { ...seedPlatform, components: [], agents: [] },
    platformComponentsLoading: true,
    platformComponentError: null,
    agentsLoading: true,
    agentError: null,
    agentConfig: null,
    agentConfigLoading: true,
    agentConfigError: null,
    host: null,
    hostLoading: true,
    hostError: null,
  })
}

type StoreContext = State & {
  adapters: typeof seedAdapters
  host: HostInfo | null
  platform: typeof seedPlatform
  watchLogs: (target: LogTarget, options: { tail: number; follow: boolean; signal: AbortSignal }, onEvent: (event: TransientLogEvent) => void) => Promise<void>
  setRequireRevealConfirm: (v: boolean) => void
  refreshPlatformComponents: (signal?: AbortSignal) => Promise<PlatformInfra['components']>
  refreshEnvironmentComponents: (environmentId: string, signal?: AbortSignal) => Promise<EnvironmentComponent[]>
  refreshEnvironmentReleases: (environmentId: string, signal?: AbortSignal) => Promise<void>
  refreshAgents: (signal?: AbortSignal) => Promise<PlatformInfra['agents']>
  setAgentConfig: (agentId: string, config: AgentConfigRequest) => Promise<AgentConfigResponse>
  joinAgent: () => Promise<AgentTaskAccepted>
  updateAgent: (agentId: string) => Promise<AgentTaskAccepted>
  removeAgent: (agentId: string) => Promise<AgentTaskAccepted>
  // selectors
  getTenant: (slug: string) => Tenant | undefined
  getProject: (tenantSlug: string, slug: string) => Project | undefined
  getProjectById: (id: string) => Project | undefined
  getBackingProject: (id: string) => Project | undefined
  getEnvironment: (tenantSlug: string, projectSlug: string, envName: string) => Environment | undefined
  getBackupPolicyState: (environmentId: string) => BackupPolicyState
  loadBackupPolicy: (environmentId: string) => Promise<void>
  loadRecoveryPoints: (environmentId: string, cursor?: string) => Promise<void>
  replaceBackupPolicy: (environmentId: string, policy: BackupPolicyReplacement) => Promise<BackupPolicyDocument>
  runBackup: (environmentId: string) => Promise<string>
  rotateBackupKey: (environmentId: string) => Promise<string>
  exportBackupKey: (environmentId: string, signal?: AbortSignal) => Promise<void>
  getTaskJournal: (scope: TaskJournalScope) => TaskJournalState
  loadTaskJournal: (surface: TaskJournalSurface, scope: TaskJournalScope, cursor?: string) => Promise<void>
  getTaskJournalDetail: (taskId: string, signal?: AbortSignal) => Promise<ActivityEntry>
  getEnvironmentDeletionFailure: (environmentId: string) => EnvironmentDeletionFailure | null
  refreshEnvironmentDeletion: (environmentId: string) => Promise<void>
  isEnvironmentDeletionPending: (environmentId: string) => boolean
  // mutations
  retryTask: (taskId: string) => Promise<string>
  commitDeploy: (envId: string, service: string, tag: string, strategy: Service['strategy']) => Promise<string>
  commitRollback: (envId: string, service: string, tag: string) => Promise<string>
  addReleaseGroup: (envId: string, group: ReleaseGroup) => Promise<ReleaseGroup>
  updateReleaseGroup: (envId: string, groupId: string, patch: Pick<ReleaseGroup, 'name' | 'services' | 'order' | 'onFailure'>) => Promise<ReleaseGroup>
  removeReleaseGroup: (envId: string, groupId: string) => Promise<string>
  deployReleaseGroup: (envId: string, groupId: string, tag?: string) => Promise<string>
  rollbackReleaseGroup: (envId: string, groupId: string) => Promise<string>
  addTenant: (t: { slug: string; name: string; description: string }) => Promise<Tenant>
  updateTenant: (slug: string, patch: { name: string; description: string }) => Promise<Tenant>
  renameTenant: (slug: string, nextSlug: string) => Promise<Tenant>
	removeTenant: (tenantId: string) => Promise<string>
  addProject: (p: { tenantId: string; slug: string; name: string; description: string }) => Promise<Project>
  editProject: (projectId: string, name: string) => Promise<Project>
  renameProject: (projectId: string, slug: string) => Promise<Project>
	deleteProject: (projectId: string) => Promise<string>
  addEnvironment: (projectId: string, name: string, networkPool: string) => Promise<EnvironmentTaskAccepted>
  editEnvironment: (envId: string, networkPool: string) => Promise<Environment>
  renameEnvironment: (envId: string, name: string) => Promise<Environment>
  getTask: (taskId: string, signal?: AbortSignal) => Promise<TaskResponse>
  abortTask: (taskId: string) => Promise<void>
  watchTaskEvents: (
    taskId: string,
    onEvent: (event: TaskEventResponse) => void,
    onMalformed: (message: string) => void,
  ) => () => void
  getBlueprint: (envId: string) => Promise<BlueprintDocumentResponse>
  validateBlueprint: (
    envId: string,
    request: BlueprintApplyRequest,
    expectedRevision: string,
  ) => Promise<BlueprintValidationResponse>
  applyBlueprint: (
    envId: string,
    request: BlueprintApplyRequest,
    expectedRevision?: string,
  ) => Promise<BlueprintTaskAccepted>
  deleteEnvironment: (envId: string) => Promise<string>
  addZone: (envId: string, input: { name: string; subnet: string; internal: boolean }) => Promise<Zone>
  getZone: (zoneId: string) => Promise<Zone>
  getZoneRemovalImpact: (zoneId: string) => Promise<ZoneRemovalImpactResponse>
  removeZone: (envId: string, zoneId: string, impactToken: string) => Promise<string>
  addService: (envId: string, input: ServiceMutationInput) => Promise<Service>
  updateService: (envId: string, serviceId: string, input: ServiceMutationInput) => Promise<Service>
	getService: (serviceId: string) => Promise<Service>
	deleteService: (envId: string, serviceId: string) => Promise<string>
  runServiceRuntimeAction: (
    envId: string,
    serviceId: string,
    action: 'start' | 'stop' | 'destroy',
  ) => Promise<string>
  addRoute: (envId: string, route: Omit<Route, 'id' | 'environmentId' | 'status'>) => Promise<Route>
  getRoute: (routeId: string) => Promise<Route>
  updateRoute: (envId: string, routeId: string, patch: Pick<Route, 'exposure'>) => Promise<Route>
  removeRoute: (envId: string, routeId: string) => Promise<string>
  addVolume: (envId: string, input: { slug: string; key?: string }) => Promise<Volume>
  getVolume: (volumeId: string) => Promise<Volume>
  updateVolume: (envId: string, volumeId: string, patch: Pick<Volume, 'slug'>) => Promise<Volume>
  getVolumeDeletionImpact: (volumeId: string, cursor?: string, limit?: number) => Promise<VolumeDeletionImpactPage>
  removeVolume: (envId: string, volumeId: string, impactToken: string, confirmKey: string) => Promise<string>
	addScript: (envId: string, script: Pick<Script, 'slug' | 'service' | 'body' | 'when'>) => Promise<Script>
	updateScript: (envId: string, scriptId: string, patch: Partial<Pick<Script, 'slug' | 'body' | 'when'>>) => Promise<Script>
	runScript: (scriptId: string) => Promise<string>
  removeScript: (envId: string, scriptId: string) => Promise<string>
  addAttach: (envId: string, input: AttachCreateInput) => Promise<string>
  renameAttach: (envId: string, attachId: string, name: string) => Promise<void>
  removeAttach: (envId: string, attachId: string) => Promise<string>
  revealAttachFact: (attachId: string, key: string, grantAttachId?: string) => Promise<string>
  addEntry: (envId: string, input: Omit<EntryCreateRequest, 'environment_id'>) => Promise<EnvironmentEntry>
  bulkUpsertEntries: (envId: string, input: Omit<EntryBulkUpsertRequest, 'environment_id'>) => Promise<EntryBulkUpsertResponse>
  updateEntry: (envId: string, entryId: string, input: EntryEditRequest) => Promise<EnvironmentEntry>
  removeEntry: (envId: string, entryId: string) => Promise<string>
  revealEntry: (entryId: string) => Promise<string>
  createReusableSecret: (input: ReusableSecretCreateInput) => Promise<ReusableSecret>
  removeReusableSecret: (id: string) => Promise<{ task_id: string }>
  refreshReusableSecrets: (signal?: AbortSignal) => Promise<void>
  revealReusableSecret: (id: string) => Promise<string>
  addConnector: (c: ConnectorCreateInput, intent: ConnectorMutationIntent) => Promise<Connector>
  removeConnector: (id: string, intent: ConnectorMutationIntent) => Promise<string>
  refreshRunners: (tenantId: string, projectIds: string[]) => Promise<void>
  createRunner: (input: {
    slug: string
    tenantId?: string
    projectId?: string
    githubUrl: string
    labels: string[]
    registrationToken: string
  }) => Promise<string>
  renameRunner: (runnerId: string, slug: string) => Promise<Runner>
  retryRunner: (runnerId: string, registrationToken: string) => Promise<string>
  removeRunner: (runnerId: string) => Promise<string>
	runBackingRuntimeAction: (id: string, action: 'start' | 'stop' | 'destroy') => Promise<string>
  addBackingProject: (input: BackingServiceCreateRequest) => Promise<BackingServiceCreatedResponse>
  setComponentEnabled: (componentId: string, enabled: boolean) => Promise<string>
  reconcileEnvironmentComponent: (componentId: string) => Promise<string>
  updateComponentConfig: (componentId: string, config: ComponentConfigInput) => Promise<string | null>
}

const Ctx = createContext<StoreContext | null>(null)

export function StoreProvider({ children }: { children: React.ReactNode }) {
	const providerActive = useRef(true)
	const environmentTaskControllers = useRef(new Set<AbortController>())
	const [state, setState] = useState<State>(seed)
	const pendingZoneRemovals = useRef(new Map<string, { envId: string; zoneId: string }>())
	const taskEventSources = useRef(new Set<EventSource>())
	const pendingConnectorRemovals = useRef(loadPendingConnectorRemovals())
	const environmentMutationIntents = useRef(loadEnvironmentMutationIntents())
	const connectorRemovalPolls = useRef(new Set<string>())
  const connectorRemovalRequests = useRef(new Map<string, Promise<string>>())
  const connectorFullLoadGeneration = useRef(0)
  const connectorEnvironmentGenerations = useRef(new Map<string, number>())
  const backupPolicyGenerations = useRef(new Map<string, number>())
  const backupPolicyLoads = useRef(new Map<string, Promise<void>>())
  const backupPointGenerations = useRef(new Map<string, number>())
  const backupPointLoads = useRef(new Map<string, Promise<void>>())
  const backupPolicySaves = useRef(
    new Map<string, { fingerprint: string; promise: Promise<BackupPolicyDocument> }>(),
  )
  const backupPolicyReplayKeys = useRef(new Map<string, { fingerprint: string; key: string }>())
	const connectorRemovalPollBackoff = useRef(
		new Map<string, { failures: number; nextAttemptAt: number }>(),
	)
	const taskJournalEpochs = useRef(new Map<string, number>())
	const reusableSecretLoadGeneration = useRef(0)

	const update = useCallback((fn: (draft: State) => void) => {
	if (!providerActive.current) return
    setState((prev) => {
	  if (!providerActive.current) return prev
      const next = structuredClone(prev)
      fn(next)
      return next
    })
	}, [])
	const persistEnvironmentMutationIntent = useCallback((intent: EnvironmentMutationIntent) => {
		environmentMutationIntents.current.set(intent.key, intent)
		persistEnvironmentMutationIntents(environmentMutationIntents.current)
	}, [])
	const clearEnvironmentMutationIntent = useCallback((key: string) => {
		environmentMutationIntents.current.delete(key)
		persistEnvironmentMutationIntents(environmentMutationIntents.current)
	}, [])
	const refreshRunners = useCallback(async (tenantId: string, projectIds: string[]) => {
		update((draft) => {
			draft.runnersLoading = true
			draft.runnerError = null
		})
		try {
			const paths = [
				`/runners?tenant=${encodeURIComponent(tenantId)}`,
				...projectIds.map((projectId) => `/runners?project=${encodeURIComponent(projectId)}`),
			]
			const pages = await Promise.all(paths.map((path) => tenantRequest<RunnerPageResponse>(path, 200)))
			const runners = new Map<string, Runner>()
			for (const page of pages) {
				for (const runner of page.items ?? []) {
					runners.set(runner.id, {
						id: runner.id,
						slug: runner.slug,
						tenantId: runner.tenant_id,
						projectId: runner.project_id ?? null,
						githubUrl: runner.github_url,
						name: runner.name,
						labels: runner.labels ?? [],
						lifecycle: runner.lifecycle,
						createTaskId: runner.create_task_id,
						removeTaskId: runner.remove_task_id,
						online: runner.online,
						observedAt: runner.observed_at,
						createdAt: runner.created_at,
					})
				}
			}
			update((draft) => {
				draft.runners = [...runners.values()].sort((left, right) => left.id.localeCompare(right.id))
				draft.runnersLoading = false
			})
		} catch (error) {
			update((draft) => {
				draft.runnersLoading = false
				draft.runnerError = error instanceof Error ? error.message : 'Unable to load Runners'
			})
		}
	}, [update])
	const createRunner = useCallback(async (input: {
		slug: string
		tenantId?: string
		projectId?: string
		githubUrl: string
		labels: string[]
		registrationToken: string
	}) => {
		const body: RunnerCreateRequest = {
			slug: input.slug,
			github_url: input.githubUrl,
			labels: input.labels,
			registration_token: input.registrationToken,
			...(input.projectId ? { project_id: input.projectId } : { tenant_id: input.tenantId }),
		}
		const response = await tenantRequest<RunnerCreateResponse>('/runners', 202, { method: 'POST', body })
		body.registration_token = ''
		if (!response.task_id) throw new Error('Controller response is missing Runner creation task_id')
		return response.task_id
	}, [])
	const renameRunner = useCallback(async (runnerId: string, slug: string) => {
		const body: RunnerEditRequest = { slug }
		const response = await tenantRequest<RunnerEditResponse>(
			`/runners/${encodeURIComponent(runnerId)}`,
			200,
			{ method: 'PATCH', body },
		)
		const renamed: Runner = {
			id: response.id,
			slug: response.slug,
			tenantId: response.tenant_id,
			projectId: response.project_id ?? null,
			githubUrl: response.github_url,
			name: response.name,
			labels: response.labels ?? [],
			lifecycle: response.lifecycle,
			createTaskId: response.create_task_id,
			removeTaskId: response.remove_task_id,
			online: response.online,
			observedAt: response.observed_at,
			createdAt: response.created_at,
		}
		update((draft) => {
			const index = draft.runners.findIndex((runner) => runner.id === renamed.id)
			if (index >= 0) draft.runners[index] = renamed
		})
		return renamed
	}, [update])
	const retryRunner = useCallback(async (runnerId: string, registrationToken: string) => {
		const body: RunnerRetryRequest = { registration_token: registrationToken }
		const response = await tenantRequest<RunnerRetryResponse>(
			`/runners/${encodeURIComponent(runnerId)}/retry`,
			202,
			{ method: 'POST', body },
		)
		body.registration_token = ''
		if (!response.task_id) throw new Error('Controller response is missing Runner retry task_id')
		return response.task_id
	}, [])
	const removeRunner = useCallback(async (runnerId: string) => {
		const response = await tenantRequest<RunnerRemoveResponse>(
			`/runners/${encodeURIComponent(runnerId)}`,
			202,
			{ method: 'DELETE' },
		)
		if (!response.task_id) throw new Error('Controller response is missing Runner removal task_id')
		return response.task_id
	}, [])

  const requestEnvironmentTask = useCallback(
    (taskId: string, signal?: AbortSignal) =>
      tenantRequest<TaskResponse>(`/tasks/${encodeURIComponent(taskId)}`, 200, { signal }),
    [],
  )
  const deleteEnvironmentResource = useCallback(
    (resource: 'environments' | 'services' | 'routes' | 'entries' | 'scripts', resourceId: string, idempotencyKey: string) =>
      tenantRequest<EnvironmentDeleteResponse>(
        `/${resource}/${encodeURIComponent(resourceId)}`,
        202,
        { method: 'DELETE', idempotencyKey },
      ),
    [],
  )
  const retryEnvironmentResource = useCallback(
    (taskId: string, idempotencyKey: string) => tenantRequest<TaskRetryResponse>(
      `/tasks/${encodeURIComponent(taskId)}/retry`,
      202,
      { method: 'POST', idempotencyKey },
    ),
    [],
  )
  const environmentLifecycle = useEnvironmentLifecycle<State>({
    active: providerActive,
    update,
    requestTask: requestEnvironmentTask,
    deleteResource: deleteEnvironmentResource,
    retryResource: retryEnvironmentResource,
    listEnvironments: listAllEnvironments,
    listServices: listAllServices,
    listRoutes: listAllRoutes,
    listEntries: listAllEntries,
    listScripts: listAllScripts,
  })
  const {
    pendingResourceRemovals,
    requestResourceRemovalTask,
    monitorResourceRemoval,
    reconcileResourceRemoval,
    dispatchResourceRemoval,
    retryResourceRemoval,
    waitForResourceRemoval,
    nextEnvironmentGeneration,
    settleEnvironmentMutation,
    shouldPreserveEnvironmentOnLoad,
    getEnvironmentDeletionFailure,
    refreshEnvironmentDeletion,
    isEnvironmentDeletionPending,
    observeEnvironmentDeletionTasks,
    environmentGenerations,
  } = environmentLifecycle

  const assertEnvironmentMutable = useCallback((environmentId: string, action: string) => {
    if (environmentDeletionGuard([...state.tenantProjects, ...state.backingProjects], environmentId, isEnvironmentDeletionPending)) {
      throw new Error(`Environment ${environmentId} deletion is in progress; ${action} is disabled`)
    }
  }, [isEnvironmentDeletionPending, state.backingProjects, state.tenantProjects])

  useEffect(() => () => {
    for (const source of taskEventSources.current) source.close()
    taskEventSources.current.clear()
    for (const controller of environmentTaskControllers.current) controller.abort()
    environmentTaskControllers.current.clear()
  }, [])

  const clearPendingConnectorRemoval = useCallback((taskId: string) => {
    pendingConnectorRemovals.current.delete(taskId)
    connectorRemovalPollBackoff.current.delete(taskId)
    persistPendingConnectorRemovals(pendingConnectorRemovals.current)
  }, [])

  const refreshConnectorEnvironment = useCallback(async (environmentId: string) => {
    const refreshGeneration =
      (connectorEnvironmentGenerations.current.get(environmentId) ?? 0) + 1
    connectorEnvironmentGenerations.current.set(environmentId, refreshGeneration)
    try {
      const refreshed = await listEnvironmentConnectors(
        environmentId,
        new AbortController().signal,
      )
      if (
        connectorEnvironmentGenerations.current.get(environmentId) !==
        refreshGeneration
      ) {
        return
      }
      update((draft) => {
        draft.connectors = [
          ...draft.connectors.filter((connector) => connector.scopeRef !== environmentId),
          ...refreshed,
        ]
        draft.connectorError = null
      })
    } catch (error) {
      if (
        connectorEnvironmentGenerations.current.get(environmentId) !==
        refreshGeneration
      ) {
        return
      }
      update((draft) => {
        draft.connectorError = error instanceof Error
          ? error.message
          : 'Unable to refresh Connectors'
      })
    }
  }, [update])

  const reconcileConnectorRemoval = useCallback(async (taskId: string, task: TaskResponse) => {
    const pending = pendingConnectorRemovals.current.get(taskId)
    if (!pending) return
    if (
      task.type !== 'remove' ||
      task.target !== pending.connectorId ||
      task.environment_id !== pending.environmentId
    ) {
      clearPendingConnectorRemoval(taskId)
      await refreshConnectorEnvironment(pending.environmentId)
      return
    }
    if (!['completed', 'failed', 'timed_out', 'aborted'].includes(task.status)) return
    clearPendingConnectorRemoval(taskId)
    if (task.status !== 'completed') return
    update((draft) => {
      draft.connectors = draft.connectors.filter(
        (candidate) => candidate.id !== pending.connectorId,
      )
      draft.connectorError = null
    })
    await refreshConnectorEnvironment(pending.environmentId)
  }, [clearPendingConnectorRemoval, refreshConnectorEnvironment, update])

  useEffect(() => {
    const timer = setInterval(() => {
      const now = Date.now()
      for (const taskId of pendingConnectorRemovals.current.keys()) {
        const backoff = connectorRemovalPollBackoff.current.get(taskId)
        if (connectorRemovalPolls.current.has(taskId) || (backoff && backoff.nextAttemptAt > now)) {
          continue
        }
        connectorRemovalPolls.current.add(taskId)
        void tenantRequest<TaskResponse>(`/tasks/${encodeURIComponent(taskId)}`, 200)
          .then(async (task) => {
            connectorRemovalPollBackoff.current.delete(taskId)
            update((draft) => {
              draft.connectorError = null
            })
            await reconcileConnectorRemoval(taskId, task)
          })
          .catch(async (error: unknown) => {
            const pending = pendingConnectorRemovals.current.get(taskId)
            if (!pending) return
            if (isTaskNotFoundError(error)) {
              clearPendingConnectorRemoval(taskId)
              await refreshConnectorEnvironment(pending.environmentId)
              return
            }
            const failures = (connectorRemovalPollBackoff.current.get(taskId)?.failures ?? 0) + 1
            const delay = Math.min(30_000, 1_000 * (2 ** Math.min(failures - 1, 5)))
            connectorRemovalPollBackoff.current.set(taskId, {
              failures,
              nextAttemptAt: Date.now() + delay,
            })
            update((draft) => {
              draft.connectorError = error instanceof Error
                ? error.message
                : 'Unable to observe Connector removal Task'
            })
          })
          .finally(() => connectorRemovalPolls.current.delete(taskId))
      }
    }, 1000)
    return () => clearInterval(timer)
  }, [clearPendingConnectorRemoval, reconcileConnectorRemoval, refreshConnectorEnvironment, update])

  const refreshPlatformComponents = useCallback(async (signal?: AbortSignal) => {
    const hydrated = hydratePlatformComponents(await listPlatformComponents(signal))
    update((draft) => {
      draft.platform.components = hydrated.components
      if (hydrated.dns) draft.platform.dns = hydrated.dns
      draft.platformComponentsLoading = false
      draft.platformComponentError = null
    })
    return hydrated.components
  }, [update])

  const refreshEnvironmentComponents = useCallback(async (environmentId: string, signal?: AbortSignal) => {
    const components = await listAllComponents(environmentId, signal)
    update((draft) => {
      for (const project of [...draft.tenantProjects, ...draft.backingProjects]) {
        const environment = project.environments?.find((candidate) => candidate.id === environmentId)
        if (!environment) continue
        environment.components = components
        return
      }
    })
    return components
  }, [update])

  const refreshEnvironmentReleases = useCallback(async (environmentId: string, signal?: AbortSignal) => {
    const environment = findEnv(state, environmentId)
    if (!environment) throw new Error(`Environment ${environmentId} is not loaded`)
    try {
      const [deploys, releaseGroups] = await Promise.all([
        listAllReleases(environmentId, environment.services, signal),
        listAllReleaseGroups(environmentId, environment.services, signal),
      ])
      update((draft) => {
        const current = findEnv(draft, environmentId)
        if (!current) return
        Object.assign(current, projectReleaseSummary(deploys))
        current.deploys = deploys
        current.releaseGroups = releaseGroups
        refreshReleaseGroupTags(current)
      })
    } catch (error) {
      update((draft) => {
        draft.projectError = error instanceof Error ? error.message : 'Unable to refresh release state'
      })
      throw error
    }
  }, [state, update])

  const refreshAgents = useCallback(async (signal?: AbortSignal) => {
    const agents = await listAllAgents(signal)
    update((draft) => {
      draft.platform.agents = agents
      draft.agentsLoading = false
      draft.agentError = null
    })
    return agents
  }, [update])

  const reusableSecretProjectIds = useMemo(
    () => state.tenantProjects.map((project) => project.id).sort().join(','),
    [state.tenantProjects],
  )

  const refreshReusableSecrets = useCallback(async (signal?: AbortSignal) => {
    const loadGeneration = reusableSecretLoadGeneration.current + 1
    reusableSecretLoadGeneration.current = loadGeneration
    update((draft) => { draft.reusableSecretsLoading = true })
    try {
      const projectIds = reusableSecretProjectIds ? reusableSecretProjectIds.split(',') : []
      const scopes: ExpectedSecretScope[] = [{ platform: true }, ...projectIds.map((projectId) => ({ projectId }))]
      const pages = await Promise.all(scopes.map((scope) => listReusableSecretScope(scope, signal)))
      if (reusableSecretLoadGeneration.current !== loadGeneration) return
      update((draft) => {
        draft.reusableSecrets = pages.flat()
        draft.reusableSecretsLoading = false
        draft.secretError = null
      })
    } catch (error) {
      if (signal?.aborted || reusableSecretLoadGeneration.current !== loadGeneration) return
      update((draft) => {
        draft.reusableSecretsLoading = false
        draft.secretError = error instanceof Error ? error.message : 'Unable to load reusable Secrets'
      })
      throw error
    }
  }, [reusableSecretProjectIds, update])

  const connectorEnvironmentIds = useMemo(
    () => [...state.tenantProjects, ...state.backingProjects]
      .flatMap((project) => project.environments ?? [])
      .map((environment) => environment.id)
      .sort()
      .join(','),
    [state.backingProjects, state.tenantProjects],
  )

  const backingConsumerStateRevision = backingConsumerRevision(state.tenantProjects, state.tenants)
  const backingConsumerInput = useMemo(() => ({
    projects: state.tenantProjects,
    tenants: state.tenants,
  }), [backingConsumerStateRevision])

  useEffect(() => {
    const controller = new AbortController()
    void refreshPlatformComponents(controller.signal).catch((error: unknown) => {
      if (controller.signal.aborted) return
      update((draft) => {
        draft.platformComponentsLoading = false
        draft.platformComponentError = error instanceof Error ? error.message : 'Unable to load Platform Components'
      })
    })
    return () => controller.abort()
  }, [refreshPlatformComponents, update])

  useEffect(() => {
    const controller = new AbortController()
    void tenantRequest<HostResponse>('/host', 200, { signal: controller.signal }).then(
      (host) => update((draft) => {
        draft.host = hostFromAPI(host)
        draft.hostLoading = false
        draft.hostError = null
      }),
      (error: unknown) => {
        if (controller.signal.aborted) return
        update((draft) => {
          draft.host = null
          draft.hostLoading = false
          draft.hostError = error instanceof Error ? error.message : 'Unable to load Host health'
        })
      },
    )
    return () => controller.abort()
  }, [update])

  useEffect(() => {
    const controller = new AbortController()
    void listAllTenants(controller.signal).then(
      (tenants) => update((draft) => {
        draft.tenants = tenants
        draft.tenantsLoading = false
        draft.tenantError = null
      }),
      (error: unknown) => {
        if (controller.signal.aborted) return
        update((draft) => {
          draft.tenantsLoading = false
          draft.tenantError = error instanceof Error ? error.message : 'Unable to load tenants'
        })
      },
    )
    return () => controller.abort()
  }, [update])

  useEffect(() => {
    if (state.projectsLoading || state.tenantsLoading) return
    const loadGenerations = environmentGenerationSnapshot(state.backingProjects, environmentGenerations.current)
    const controller = new AbortController()
    void listAllBackingProjects(backingConsumerInput.projects, backingConsumerInput.tenants, controller.signal).then(
      (projects) => {
        observeEnvironmentDeletionTasks(projects)
        update((draft) => {
        draft.backingProjects = mergeEnvironmentProjectLoads(
          draft.backingProjects,
          projects,
          loadGenerations,
          shouldPreserveEnvironmentOnLoad,
        )
          draft.backingProjectsLoading = false
          draft.backingProjectError = null
        })
      },
      (error: unknown) => {
        if (controller.signal.aborted) return
        update((draft) => {
          draft.backingProjects = []
          draft.backingProjectsLoading = false
          draft.backingProjectError = error instanceof Error ? error.message : 'Unable to load backing services'
        })
      },
    )
    return () => controller.abort()
  }, [backingConsumerInput, observeEnvironmentDeletionTasks, shouldPreserveEnvironmentOnLoad, state.projectsLoading, state.tenantsLoading, update])

  useEffect(() => {
    const controller = new AbortController()
    void refreshAgents(controller.signal).then(
      () => undefined,
      (error: unknown) => {
        if (controller.signal.aborted) return
        update((draft) => {
          draft.agentsLoading = false
          draft.agentError = error instanceof Error ? error.message : 'Unable to load Agents'
        })
      },
    )
    return () => controller.abort()
  }, [refreshAgents, update])

  const localAgentID = state.platform.agents[0]?.id ?? ''

  useEffect(() => {
    if (state.agentsLoading) return
    if (!localAgentID) {
      update((draft) => {
        draft.agentConfig = null
        draft.agentConfigLoading = false
        draft.agentConfigError = null
      })
      return
    }
    const controller = new AbortController()
    const path = `/agents/${encodeURIComponent(localAgentID)}/config`
    void tenantRequest<AgentConfigResponse>(path, 200, { signal: controller.signal }).then(
      (config) => update((draft) => {
        draft.agentConfig = config
        draft.agentConfigLoading = false
        draft.agentConfigError = null
      }),
      (error: unknown) => {
        if (controller.signal.aborted) return
        update((draft) => {
          draft.agentConfigLoading = false
          draft.agentConfigError = error instanceof Error ? error.message : 'Unable to load Agent config'
        })
      },
    )
    return () => controller.abort()
  }, [localAgentID, state.agentsLoading, update])

  useEffect(() => {
    const controller = new AbortController()
    const loadGenerations = new Map(environmentGenerations.current)
    void listAllTenantProjects(controller.signal).then(
      (projects) => {
        observeEnvironmentDeletionTasks(projects)
        update((draft) => {
        draft.tenantProjects = projects.map((project) => {
          const currentProject = draft.tenantProjects.find((candidate) => candidate.id === project.id)
          const currentEnvironments = currentProject?.environments ?? []
          const loadedIds = new Set((project.environments ?? []).map((environment) => environment.id))
          const environments = (project.environments ?? []).flatMap((environment) => {
            const currentGeneration = environmentGenerations.current.get(environment.id) ?? 0
            if (
              currentGeneration !== (loadGenerations.get(environment.id) ?? 0) &&
              !shouldPreserveEnvironmentOnLoad(environment.id, loadGenerations.get(environment.id) ?? 0)
            ) return []
            return [currentGeneration !== (loadGenerations.get(environment.id) ?? 0)
              ? currentEnvironments.find((candidate) => candidate.id === environment.id) ?? environment
              : environment]
          })
          for (const current of currentEnvironments) {
            const currentGeneration = environmentGenerations.current.get(current.id) ?? 0
            if (
              !loadedIds.has(current.id) &&
              currentGeneration !== (loadGenerations.get(current.id) ?? 0) &&
              shouldPreserveEnvironmentOnLoad(current.id, loadGenerations.get(current.id) ?? 0)
            ) {
              environments.push(current)
            }
          }
          return { ...project, environments }
        })
        draft.projectsLoading = false
        const deletionFailure = projects
          .flatMap((project) => project.environments ?? [])
          .map((environment) => getEnvironmentDeletionFailure(environment.id))
          .find((failure): failure is NonNullable<ReturnType<typeof getEnvironmentDeletionFailure>> => failure !== null)
        draft.projectError = deletionFailure?.message ?? null
        })
      },
      (error: unknown) => {
        if (controller.signal.aborted) return
        update((draft) => {
          draft.projectsLoading = false
          draft.projectError = error instanceof Error ? error.message : 'Unable to load projects'
        })
      },
    )
    return () => controller.abort()
  }, [observeEnvironmentDeletionTasks, environmentGenerations, getEnvironmentDeletionFailure, shouldPreserveEnvironmentOnLoad, update])

  useEffect(() => {
    if (state.projectsLoading || state.backingProjectsLoading) return
    const controller = new AbortController()
    void refreshReusableSecrets(controller.signal).catch(() => undefined)
    return () => controller.abort()
  }, [refreshReusableSecrets, state.backingProjectsLoading, state.projectsLoading])

  useEffect(() => {
    if (state.projectsLoading || state.backingProjectsLoading) return
    const controller = new AbortController()
    const environmentIds = connectorEnvironmentIds ? connectorEnvironmentIds.split(',') : []
    const loadGeneration = connectorFullLoadGeneration.current + 1
    connectorFullLoadGeneration.current = loadGeneration
    const environmentGenerations = new Map(
      environmentIds.map((environmentId) => [
        environmentId,
        connectorEnvironmentGenerations.current.get(environmentId) ?? 0,
      ]),
    )
    void listAllConnectors(environmentIds, controller.signal).then(
      (connectors) => {
        if (connectorFullLoadGeneration.current !== loadGeneration) return
        const unchangedEnvironments = new Set(environmentIds.filter(
          (environmentId) => (
            connectorEnvironmentGenerations.current.get(environmentId) ?? 0
          ) === environmentGenerations.get(environmentId),
        ))
        const requestedEnvironments = new Set(environmentIds)
        update((draft) => {
          draft.connectors = [
            ...draft.connectors.filter(
              (connector) => (
                requestedEnvironments.has(connector.scopeRef) &&
                !unchangedEnvironments.has(connector.scopeRef)
              ),
            ),
            ...connectors.filter(
              (connector) => unchangedEnvironments.has(connector.scopeRef),
            ),
          ]
          draft.connectorsLoading = false
          draft.connectorError = null
        })
      },
      (error: unknown) => {
        if (
          controller.signal.aborted ||
          connectorFullLoadGeneration.current !== loadGeneration
        ) {
          return
        }
        update((draft) => {
          draft.connectorsLoading = false
          draft.connectorError = error instanceof Error ? error.message : 'Unable to load Connectors'
        })
      },
    )
    return () => controller.abort()
  }, [connectorEnvironmentIds, state.backingProjectsLoading, state.projectsLoading, update])

  const findEnv = (draft: State, envId: string): Environment | undefined => {
    for (const p of [...draft.tenantProjects, ...draft.backingProjects]) {
      const e = p.environments?.find((x) => x.id === envId)
      if (e) return e
    }
    return undefined
  }

  const loadTaskJournal = useCallback<StoreContext['loadTaskJournal']>(async (surface, scope, cursor) => {
    const key = taskJournalKey(scope)
    const epoch = (taskJournalEpochs.current.get(key) ?? 0) + 1
    taskJournalEpochs.current.set(key, epoch)
    update((draft) => {
      const journal = draft.taskJournals[key] ?? emptyTaskJournal()
      journal.loadError = null
      journal.failedCursor = null
      journal.loading = !cursor
      journal.loadingMore = !!cursor
      draft.taskJournals[key] = journal
    })
    try {
      const page = await tenantRequest<TaskPageResponse>(`/${surface}?${taskJournalQuery(scope, cursor)}`, 200)
      const entries = (page.items ?? []).map(taskFromAPI)
      if (taskJournalEpochs.current.get(key) !== epoch) return
      update((draft) => {
        const journal = draft.taskJournals[key] ?? emptyTaskJournal()
        journal.entries = cursor ? [...journal.entries, ...entries] : entries
        journal.nextCursor = page.next_cursor ?? null
        journal.loaded = true
        journal.loading = false
        journal.loadingMore = false
        journal.loadError = null
        journal.failedCursor = null
        draft.taskJournals[key] = journal
      })
    } catch (error) {
      if (taskJournalEpochs.current.get(key) !== epoch) return
      update((draft) => {
        const journal = draft.taskJournals[key] ?? emptyTaskJournal()
        journal.loaded = true
        journal.loading = false
        journal.loadingMore = false
        journal.loadError = error instanceof Error ? error.message : 'Unable to load Tasks'
        journal.failedCursor = cursor ?? null
        draft.taskJournals[key] = journal
      })
      throw error
    }
  }, [update])

  useEffect(() => {
    void loadTaskJournal('tasks', { kind: 'all' }).catch(() => undefined)
  }, [loadTaskJournal])

  const loadBackupPolicy = useCallback(async (environmentId: string) => {
    const saving = backupPolicySaves.current.get(environmentId)
    if (saving) return saving.promise.then(() => undefined)
    const current = backupPolicyLoads.current.get(environmentId)
    if (current) return current
    const generation = (backupPolicyGenerations.current.get(environmentId) ?? 0) + 1
    backupPolicyGenerations.current.set(environmentId, generation)
    update((draft) => {
      const policy = mutableBackupPolicyState(draft, environmentId)
      policy.loading = true
      policy.loadError = null
    })
    const request = (async () => {
      try {
        const [response, attaches, volumes] = await Promise.all([
          tenantRequest<BackupPolicyShowResponse>(
            `/environments/${encodeURIComponent(environmentId)}/backup-policy`,
            200,
          ),
          listBackupPolicyAttaches(environmentId),
          listBackupPolicyVolumes(environmentId),
        ])
        if (backupPolicyGenerations.current.get(environmentId) !== generation) return
        update((draft) => {
          const policy = mutableBackupPolicyState(draft, environmentId)
          policy.policy = backupPolicyFromAPI(response)
          policy.attaches = attaches
          policy.volumes = volumes
          policy.loaded = true
          policy.loading = false
          policy.loadError = null
          policy.saveError = null
        })
      } catch (error) {
        if (backupPolicyGenerations.current.get(environmentId) === generation) {
          update((draft) => {
            const policy = mutableBackupPolicyState(draft, environmentId)
            policy.loading = false
            policy.loadError = error instanceof Error ? error.message : 'Unable to load Backup Policy'
          })
        }
        throw error
      }
    })()
    backupPolicyLoads.current.set(environmentId, request)
    try {
      await request
    } finally {
      if (backupPolicyLoads.current.get(environmentId) === request) backupPolicyLoads.current.delete(environmentId)
    }
  }, [update])

  const loadRecoveryPoints = useCallback(async (environmentId: string, cursor?: string) => {
    const loadKey = `${environmentId}:${cursor ?? ''}`
    const current = backupPointLoads.current.get(loadKey)
    if (current) return current
    const generation = (backupPointGenerations.current.get(environmentId) ?? 0) + 1
    backupPointGenerations.current.set(environmentId, generation)
    update((draft) => {
      const policy = mutableBackupPolicyState(draft, environmentId)
      const points = policy.recoveryPoints
      if (cursor) points.loadingMore = true
      else points.loading = true
      points.loadError = null
      points.failedCursor = null
    })
    const request = (async () => {
      try {
        const query = new URLSearchParams()
        if (cursor) query.set('cursor', cursor)
        const suffix = query.size === 0 ? '' : `?${query}`
        const response = await tenantRequest<RecoveryPointPageResponse>(
          `/environments/${encodeURIComponent(environmentId)}/recovery-points${suffix}`,
          200,
        )
        if (backupPointGenerations.current.get(environmentId) !== generation) return
        const items = (response.items ?? []).map(recoveryPointFromAPI)
        update((draft) => {
          const points = mutableBackupPolicyState(draft, environmentId).recoveryPoints
          points.items = cursor ? [...points.items, ...items] : items
          points.nextCursor = response.next_cursor ?? null
          points.loaded = true
          points.loading = false
          points.loadingMore = false
          points.loadError = null
          points.failedCursor = null
        })
      } catch (error) {
        if (backupPointGenerations.current.get(environmentId) === generation) {
          update((draft) => {
            const points = mutableBackupPolicyState(draft, environmentId).recoveryPoints
            points.loaded = true
            points.loading = false
            points.loadingMore = false
            points.loadError = error instanceof Error ? error.message : 'Unable to load Recovery Points'
            points.failedCursor = cursor ?? null
          })
        }
        throw error
      }
    })()
    backupPointLoads.current.set(loadKey, request)
    try {
      await request
    } finally {
      if (backupPointLoads.current.get(loadKey) === request) backupPointLoads.current.delete(loadKey)
    }
  }, [update])

  const replaceBackupPolicy = useCallback(async (
    environmentId: string,
    input: BackupPolicyReplacement,
  ): Promise<BackupPolicyDocument> => {
    assertEnvironmentMutable(environmentId, 'Backup Policy mutation')
    const body = backupPolicyRequest(input)
    const fingerprint = JSON.stringify(body)
    const inFlight = backupPolicySaves.current.get(environmentId)
    if (inFlight) {
      if (inFlight.fingerprint === fingerprint) return inFlight.promise
      throw new Error('A different Backup Policy replacement is already in progress')
    }
    const priorReplay = backupPolicyReplayKeys.current.get(environmentId)
    const replay = priorReplay?.fingerprint === fingerprint
      ? priorReplay
      : { fingerprint, key: newULID() }
    backupPolicyReplayKeys.current.set(environmentId, replay)
    backupPolicyGenerations.current.set(
      environmentId,
      (backupPolicyGenerations.current.get(environmentId) ?? 0) + 1,
    )
    update((draft) => {
      const policy = mutableBackupPolicyState(draft, environmentId)
      policy.saving = true
      policy.saveError = null
    })
    const request = (async () => {
      try {
        const response = await tenantRequest<BackupPolicySetResponse>(
          `/environments/${encodeURIComponent(environmentId)}/backup-policy`,
          200,
          { method: 'PUT', body, idempotencyKey: replay.key },
        )
        const policy = backupPolicyFromAPI(response)
        if (backupPolicyReplayKeys.current.get(environmentId)?.key === replay.key) {
          backupPolicyReplayKeys.current.delete(environmentId)
        }
        update((draft) => {
          const current = mutableBackupPolicyState(draft, environmentId)
          current.policy = policy
          current.loaded = true
          current.saving = false
          current.loadError = null
          current.saveError = null
        })
        return policy
      } catch (error) {
        update((draft) => {
          const policy = mutableBackupPolicyState(draft, environmentId)
          policy.saving = false
          policy.saveError = error instanceof Error ? error.message : 'Unable to save Backup Policy'
        })
        throw error
      }
    })()
    backupPolicySaves.current.set(environmentId, { fingerprint, promise: request })
    try {
      return await request
    } finally {
      if (backupPolicySaves.current.get(environmentId)?.promise === request) {
        backupPolicySaves.current.delete(environmentId)
      }
    }
  }, [assertEnvironmentMutable, update])

  const runBackup = useCallback(async (environmentId: string): Promise<string> => {
    assertEnvironmentMutable(environmentId, 'Backup run')
    const response = await tenantRequest<BackupRunTaskAccepted>(
      `/environments/${encodeURIComponent(environmentId)}/backup-run`,
      202,
      { method: 'POST' },
    )
    return requireTaskId(response, 'Backup run')
  }, [assertEnvironmentMutable])

  const rotateBackupKey = useCallback(async (environmentId: string): Promise<string> => {
    assertEnvironmentMutable(environmentId, 'Backup key rotation')
    const response = await tenantRequest<BackupKeyRotateResponse>(
      `/environments/${encodeURIComponent(environmentId)}/rotate-key`,
      202,
      { method: 'POST' },
    )
    if (!response.task_id) throw new Error('Controller returned an empty backup key rotation task id')
    return response.task_id
  }, [assertEnvironmentMutable])

  const exportBackupKey = useCallback(async (environmentId: string, signal?: AbortSignal): Promise<void> => {
    const path = `/environments/${encodeURIComponent(environmentId)}/export-key`
    let response: globalThis.Response | null = null
    let blob: Blob | null = null
    let objectURL: string | null = null
    let anchor: HTMLAnchorElement | null = null
    try {
      try {
        response = await fetch(`/api/v1${path}`, {
          method: "POST",
          headers: { Accept: "text/plain" },
          cache: "no-store",
          signal,
        })
      } catch (error) {
        throw new ControllerTransportError(
          `POST ${path}: ${error instanceof Error ? error.message : "request failed before an HTTP response"}`,
          error,
        )
      }
      if (response.status !== 200) throw await controllerResponseError(response, "POST", path)
      if (response.headers.get("Cache-Control")?.toLowerCase() !== "no-store") {
        throw new Error("Controller backup key export response is not marked no-store")
      }
      if (response.headers.get("Content-Type")?.toLowerCase() !== "text/plain; charset=utf-8") {
        throw new Error("Controller backup key export response has an invalid content type")
      }
      const disposition = response.headers.get("Content-Disposition") ?? ""
      const filenamePattern = new RegExp(
        '^attachment; filename="(groundplane-' + environmentId + '-age-era-[1-9][0-9]*-identity\\.txt)"$',
      )
      const filename = disposition.match(filenamePattern)?.[1]
      if (!filename) throw new Error("Controller backup key export response has an invalid attachment name")
      blob = await response.blob()
      objectURL = URL.createObjectURL(blob)
      anchor = document.createElement("a")
      anchor.href = objectURL
      anchor.download = filename
      anchor.click()
    } finally {
      if (anchor) {
        anchor.removeAttribute("href")
        anchor.remove()
      }
      if (objectURL) URL.revokeObjectURL(objectURL)
      anchor = null
      objectURL = null
      blob = null
      response = null
    }
  }, [])

  const value = useMemo<StoreContext>(() => {
    return {
      ...state,
      adapters: seedAdapters,
      host: state.host,
      platform: state.platform,
      watchLogs: watchTransientLogs,
      setRequireRevealConfirm: (v) => {
        update((d) => {
          d.requireRevealConfirm = v
        })
        try {
          localStorage.setItem('groundplane-reveal-confirm', v ? '1' : '0')
        } catch {
          /* private mode */
        }
      },
      refreshPlatformComponents,
      refreshEnvironmentComponents,
      refreshEnvironmentReleases,
      refreshAgents,
      setAgentConfig: async (agentId, config) => {
        const path = `/agents/${encodeURIComponent(agentId)}/config`
        const updated = await tenantRequest<AgentConfigResponse>(path, 200, { method: 'PUT', body: config })
        update((draft) => {
          draft.agentConfig = updated
          draft.agentConfigError = null
        })
        return updated
      },
      joinAgent: async () => {
        const accepted = await tenantRequest<AgentTaskAccepted>('/agents', 202, { method: 'POST' })
        await refreshAgents()
        return accepted
      },
      updateAgent: async (agentId) => {
        const accepted = await tenantRequest<AgentTaskAccepted>(
          `/agents/${encodeURIComponent(agentId)}/update`,
          202,
          { method: 'POST' },
        )
        await refreshAgents()
        return accepted
      },
      removeAgent: async (agentId) => {
        const accepted = await tenantRequest<AgentTaskAccepted>(
          `/agents/${encodeURIComponent(agentId)}`,
          202,
          { method: 'DELETE' },
        )
        await refreshAgents()
        return accepted
      },
      getTenant: (slug) => state.tenants.find((t) => t.slug === slug),
      getProject: (tenantSlug, slug) => {
        const tenantId = state.tenants.find((tenant) => tenant.slug === tenantSlug)?.id
        return state.tenantProjects.find((project) => project.tenantId === tenantId && project.slug === slug)
      },
      getProjectById: (id) => state.tenantProjects.find((p) => p.id === id),
      getBackingProject: (id) => state.backingProjects.find((p) => p.id === id),
      getEnvironment: (tenantSlug, projectSlug, envName) => {
        const tenantId = state.tenants.find((tenant) => tenant.slug === tenantSlug)?.id
        return state.tenantProjects
          .find((project) => project.tenantId === tenantId && project.slug === projectSlug)
          ?.environments?.find((environment) => environment.name === envName)
      },
      getBackupPolicyState: (environmentId) => state.backupPolicies[environmentId] ?? emptyBackupPolicyState(),
      loadBackupPolicy,
      loadRecoveryPoints,
      replaceBackupPolicy,
      runBackup,
      rotateBackupKey,
      exportBackupKey,
      getTaskJournal: (scope) => state.taskJournals[taskJournalKey(scope)] ?? emptyTaskJournal(),
      loadTaskJournal,
      getTaskJournalDetail: async (taskId, signal) =>
        taskFromAPI(await tenantRequest<TaskResponse>(`/tasks/${encodeURIComponent(taskId)}`, 200, { signal })),
      getEnvironmentDeletionFailure,
      refreshEnvironmentDeletion,
      isEnvironmentDeletionPending,
      retryTask: (taskId) => retryResourceRemoval(taskId) ?? tenantRequest<TaskRetryResponse>(
		  `/tasks/${encodeURIComponent(taskId)}/retry`,
		  202,
		  { method: 'POST' },
		).then((accepted) => {
		  if (!accepted.task_id) throw new Error('Controller response is missing task_id')
		  return accepted.task_id
		}),
      commitDeploy: async (envId, service, tag, strategy) => {
        assertEnvironmentMutable(envId, 'deployment')
        const target = findEnv(state, envId)?.services.find((candidate) => candidate.name === service)
        if (!target) throw new Error(`Service ${service} no longer exists`)
        const body: operations['service.deploy']['requestBody']['content']['application/json'] = {
          tag,
          strategy,
          on_failure: 'switch_back',
        }
        const accepted = await tenantRequest<
          operations['service.deploy']['responses'][202]['content']['application/json']
        >(`/services/${encodeURIComponent(target.id)}/deploy`, 202, { method: 'POST', body })
        if (!accepted.task_id) throw new Error('Controller response is missing deploy task_id')
        return accepted.task_id
      },
      commitRollback: async (envId, service, tag) => {
        assertEnvironmentMutable(envId, 'rollback')
        const target = findEnv(state, envId)?.services.find((candidate) => candidate.name === service)
        if (!target) throw new Error(`Service ${service} no longer exists`)
        const body: operations['service.rollback']['requestBody']['content']['application/json'] = { tag }
        const accepted = await tenantRequest<
          operations['service.rollback']['responses'][202]['content']['application/json']
        >(`/services/${encodeURIComponent(target.id)}/rollback`, 202, { method: 'POST', body })
        if (!accepted.task_id) throw new Error('Controller response is missing rollback task_id')
        return accepted.task_id
      },
      addTenant: async (tenant) => {
        const body: TenantCreateRequest = tenant
        const created = tenantFromAPI(await tenantRequest<TenantCreateResponse>('/tenants', 201, {
          method: 'POST', body,
        }))
        update((draft) => {
          draft.tenants.push(created)
        })
        return created
      },
      updateTenant: async (slug, patch) => {
        const current = state.tenants.find((tenant) => tenant.slug === slug)
        if (!current) throw new Error(`Tenant ${slug} no longer exists`)
        const body: TenantEditRequest = patch
        const updated = tenantFromAPI(await tenantRequest<TenantEditResponse>(
          `/tenants/${encodeURIComponent(current.id)}`,
          200,
          { method: 'PATCH', body },
        ))
        update((draft) => {
          const index = draft.tenants.findIndex((tenant) => tenant.id === updated.id)
          if (index >= 0) draft.tenants[index] = updated
        })
        return updated
      },
      renameTenant: async (slug, nextSlug) => {
        const current = state.tenants.find((tenant) => tenant.slug === slug)
        if (!current) throw new Error(`Tenant ${slug} no longer exists`)
        const body: TenantRenameRequest = { slug: nextSlug }
        const renamed = tenantFromAPI(await tenantRequest<TenantRenameResponse>(
          `/tenants/${encodeURIComponent(current.id)}/rename`,
          200,
          { method: 'POST', body },
        ))
        update((draft) => {
          const index = draft.tenants.findIndex((tenant) => tenant.id === renamed.id)
          if (index >= 0) draft.tenants[index] = renamed
        })
        return renamed
      },
	removeTenant: async (tenantId) => {
		const accepted = await tenantRequest<HierarchyTaskAccepted>(
			`/tenants/${encodeURIComponent(tenantId)}`,
			202,
			{ method: 'DELETE' },
		)
		if (!accepted.task_id) throw new Error('Controller response is missing task_id')
		return accepted.task_id
	},
      addProject: async (project) => {
        const body: ProjectCreateRequest = {
          tenant_id: project.tenantId,
          slug: project.slug,
          name: project.name,
          description: project.description,
        }
        const created = projectFromAPI(await tenantRequest<ProjectCreateResponse>('/projects', 201, {
          method: 'POST', body,
        }))
        update((draft) => {
          draft.tenantProjects.push(created)
        })
        return created
      },
      editProject: async (projectId, name) => {
        const body: ProjectEditRequest = { name }
        const updated = projectFromAPI(await tenantRequest<ProjectEditResponse>(
          `/projects/${encodeURIComponent(projectId)}`,
          200,
          { method: 'PATCH', body },
        ))
        update((draft) => {
          const index = draft.tenantProjects.findIndex((project) => project.id === updated.id)
          if (index >= 0) draft.tenantProjects[index] = updated
        })
        return updated
      },
      renameProject: async (projectId, slug) => {
        const body: ProjectRenameRequest = { slug }
        const renamed = projectFromAPI(await tenantRequest<ProjectRenameResponse>(
          `/projects/${encodeURIComponent(projectId)}/rename`,
          200,
          { method: 'POST', body },
        ))
        update((draft) => {
          const index = draft.tenantProjects.findIndex((project) => project.id === renamed.id)
          if (index >= 0) draft.tenantProjects[index] = renamed
        })
        return renamed
      },
	deleteProject: async (projectId) => {
		const accepted = await tenantRequest<HierarchyTaskAccepted>(
			`/projects/${encodeURIComponent(projectId)}`,
			202,
			{ method: 'DELETE' },
		)
		if (!accepted.task_id) throw new Error('Controller response is missing task_id')
		return accepted.task_id
	},
      addEnvironment: async (projectId, name, networkPool) => {
        const body: EnvironmentCreateRequest = { project_id: projectId, name, network_pool: networkPool }
        const key = environmentMutationKey('create', [projectId, name, networkPool])
        let intent = environmentMutationIntents.current.get(key) ?? {
          key,
          kind: 'create' as const,
          idempotencyKey: `groundplane:${newULID()}`,
          projectId,
          name,
          networkPool,
        }
        persistEnvironmentMutationIntent(intent)
        let observedTaskStatus: TaskResponse['status'] | undefined
        try {
          const accepted = intent.taskId
            ? { task_id: intent.taskId }
            : await tenantRequest<EnvironmentTaskAccepted>('/environments', 202, {
              method: 'POST',
              body,
              idempotencyKey: intent.idempotencyKey,
            })
          if (!accepted.task_id) throw new Error('Controller response is missing create task_id')
          if (!intent.taskId) {
            intent = { ...intent, taskId: accepted.task_id }
            persistEnvironmentMutationIntent(intent)
          }
          const observationController = new AbortController()
          environmentTaskControllers.current.add(observationController)
          let task: TaskResponse
          try {
            task = await observeEnvironmentTask(requestEnvironmentTask, accepted.task_id, providerActive, observationController.signal)
          } finally {
            environmentTaskControllers.current.delete(observationController)
          }
          observedTaskStatus = task.status
          if (task.id !== accepted.task_id || task.type !== 'create' || !task.target) {
            clearEnvironmentMutationIntent(key)
            throw new Error(`Controller create Task ${accepted.task_id} did not identify an Environment target`)
          }
          if (task.project_id !== projectId) {
            clearEnvironmentMutationIntent(key)
            throw new Error(`Controller create Task ${accepted.task_id} belongs to a different Project`)
          }
          if (task.status !== 'completed') {
            clearEnvironmentMutationIntent(key)
            throw new Error(`Controller create Task ${accepted.task_id} ${task.status}`)
          }
          const environments = await listAllEnvironments(projectId)
          const created = environments.find((environment) => environment.id === task.target && environment.projectId === projectId)
          if (!created) {
            clearEnvironmentMutationIntent(key)
            throw new Error(`Controller did not publish Environment for task ${accepted.task_id}`)
          }
          const generation = nextEnvironmentGeneration(created.id, 'create')
          update((draft) => {
            if ((environmentGenerations.current.get(created.id) ?? 0) !== generation) return
            const project = draft.tenantProjects.find((candidate) => candidate.id === projectId)
            if (!project) return
            project.environments = project.environments ?? []
            const index = project.environments.findIndex((environment) => environment.id === created.id)
            if (index >= 0) project.environments[index] = created
            else project.environments.push(created)
          })
          if (created.provisioningState === 'failed') {
            settleEnvironmentMutation(created.id, generation, 'failed')
            clearEnvironmentMutationIntent(key)
            throw new Error(`Controller Environment ${created.id} provisioning failed`)
          }
          settleEnvironmentMutation(created.id, generation, 'succeeded')
          clearEnvironmentMutationIntent(key)
          return accepted
        } catch (error) {
          if (observedTaskStatus && observedTaskStatus !== 'completed') clearEnvironmentMutationIntent(key)
          const message = error instanceof Error ? error.message : 'Unable to create Environment'
          update((draft) => { draft.projectError = message })
          throw error
        }
      },
      editEnvironment: async (envId, networkPool) => {
        assertEnvironmentMutable(envId, 'Environment edit')
        const body: EnvironmentEditRequest = { network_pool: networkPool }
        const project = [...state.tenantProjects, ...state.backingProjects].find((candidate) =>
          candidate.environments?.some((environment) => environment.id === envId),
        )
        if (!project) throw new Error(`Environment ${envId} is not loaded`)
        const key = environmentMutationKey('edit', [envId, networkPool])
        const intent = environmentMutationIntents.current.get(key) ?? {
          key, kind: 'edit' as const, idempotencyKey: `groundplane:${newULID()}`,
          projectId: project.id, environmentId: envId, networkPool,
        }
        persistEnvironmentMutationIntent(intent)
        const generation = nextEnvironmentGeneration(envId, 'edit')
        try {
          const edited = environmentFromAPI(await tenantRequest<EnvironmentEditResponse>(
            `/environments/${encodeURIComponent(envId)}`,
            200,
            { method: 'PATCH', body, idempotencyKey: intent.idempotencyKey },
          ))
          if (edited.id !== envId || edited.projectId !== project.id) throw new Error(`Controller returned Environment ${edited.id} for a different resource`)
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation) {
            clearEnvironmentMutationIntent(key)
            throw new Error(`Environment edit ${envId} was superseded`)
          }
          if (!findEnv(state, envId)) {
            clearEnvironmentMutationIntent(key)
            throw new Error(`Environment edit ${envId} was not applied`)
          }
          update((draft) => {
            if ((environmentGenerations.current.get(envId) ?? 0) !== generation) return
            const environment = findEnv(draft, envId)
            if (!environment) return
            applyAuthoritativeEnvironmentScalars(environment, edited)
          })
          settleEnvironmentMutation(envId, generation, 'succeeded')
          clearEnvironmentMutationIntent(key)
          return edited
        } catch (error) {
          settleEnvironmentMutation(envId, generation, 'failed')
          throw error
        }
      },
      renameEnvironment: async (envId, name) => {
        assertEnvironmentMutable(envId, 'Environment rename')
        const body: EnvironmentRenameRequest = { name }
        const project = [...state.tenantProjects, ...state.backingProjects].find((candidate) =>
          candidate.environments?.some((environment) => environment.id === envId),
        )
        if (!project) throw new Error(`Environment ${envId} is not loaded`)
        const key = environmentMutationKey('rename', [envId, name])
        const intent = environmentMutationIntents.current.get(key) ?? {
          key, kind: 'rename' as const, idempotencyKey: `groundplane:${newULID()}`,
          projectId: project.id, environmentId: envId, name,
        }
        persistEnvironmentMutationIntent(intent)
        const generation = nextEnvironmentGeneration(envId, 'rename')
        try {
          const renamed = environmentFromAPI(await tenantRequest<EnvironmentRenameResponse>(
            `/environments/${encodeURIComponent(envId)}/rename`,
            200,
            { method: 'POST', body, idempotencyKey: intent.idempotencyKey },
          ))
          if (renamed.id !== envId || renamed.projectId !== project.id) throw new Error(`Controller returned Environment ${renamed.id} for a different resource`)
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation) {
            clearEnvironmentMutationIntent(key)
            throw new Error(`Environment rename ${envId} was superseded`)
          }
          if (!findEnv(state, envId)) {
            clearEnvironmentMutationIntent(key)
            throw new Error(`Environment rename ${envId} was not applied`)
          }
          update((draft) => {
            if ((environmentGenerations.current.get(envId) ?? 0) !== generation) return
            const environment = findEnv(draft, envId)
            if (!environment) return
            applyAuthoritativeEnvironmentScalars(environment, renamed)
          })
          settleEnvironmentMutation(envId, generation, 'succeeded')
          clearEnvironmentMutationIntent(key)
          return renamed
        } catch (error) {
          settleEnvironmentMutation(envId, generation, 'failed')
          throw error
        }
      },
      watchTaskEvents: (taskId, onEvent, onMalformed) => {
        const source = new EventSource(`/api/v1/tasks/${encodeURIComponent(taskId)}/events`)
		taskEventSources.current.add(source)
		const close = () => {
			source.close()
			taskEventSources.current.delete(source)
		}
        source.onmessage = (message) => {
          try {
            onEvent(parseTaskEvent(message.data))
          } catch {
			close()
            onMalformed('Task event stream returned malformed data')
          }
        }
		return close
      },
      getTask: async (taskId, signal) => {
		const task = pendingResourceRemovals.current.has(taskId)
			? await waitForRequest(requestResourceRemovalTask(taskId), signal)
			: await tenantRequest<TaskResponse>(`/tasks/${encodeURIComponent(taskId)}`, 200, { signal })
        const pending = pendingZoneRemovals.current.get(taskId)
		if (pending && ['completed', 'failed', 'timed_out', 'aborted'].includes(task.status)) {
          pendingZoneRemovals.current.delete(taskId)
          if (task.status === 'completed') {
            update((draft) => {
              const environment = findEnv(draft, pending.envId)
              if (!environment) return
              const zone = environment.zones.find((candidate) => candidate.id === pending.zoneId)
              if (!zone) return
              environment.zones = environment.zones.filter((candidate) => candidate.id !== pending.zoneId)
              environment.services.forEach((service) => {
                service.zones = service.zones.filter((name) => name !== zone.name)
              })
            })
			}
		}
		if (pendingResourceRemovals.current.has(taskId)) {
			monitorResourceRemoval(taskId)
			void reconcileResourceRemoval(taskId, task).catch(() => undefined)
		}
		await reconcileConnectorRemoval(taskId, task)
		return task
	  },
      abortTask: async (taskId) => {
        const accepted = await tenantRequest<TaskAbortResponse>(
          `/tasks/${encodeURIComponent(taskId)}/abort`,
          202,
          { method: 'POST' },
        )
        if (accepted.task_id !== taskId) throw new Error('Task abort returned a different Task id')
      },
      getBlueprint: async (envId) => tenantRequest<BlueprintDocumentResponse>(
        `/environments/${encodeURIComponent(envId)}/blueprint`,
        200,
      ),
      validateBlueprint: async (envId, request, expectedRevision) => {
        const path = `/environments/${encodeURIComponent(envId)}/blueprint/validate`
        const multipart = await createBlueprintMultipartBody(request)
        const response = await fetch(`/api/v1${path}`, {
          method: 'POST',
          headers: {
            Accept: 'application/json',
            'Content-Type': multipart.contentType,
            'If-Match': `"${expectedRevision}"`,
          },
          body: multipart.body,
        })
        if (response.status !== 200) throw await controllerResponseError(response, 'POST', path)
        return (await response.json()) as BlueprintValidationResponse
      },
      applyBlueprint: async (envId, request, expectedRevision) => {
        assertEnvironmentMutable(envId, 'desired-state apply')
        const path = `/environments/${encodeURIComponent(envId)}/blueprint`
        const revision = expectedRevision ?? (await tenantRequest<BlueprintDocumentResponse>(path, 200)).revision
        const multipart = await createBlueprintMultipartBody(request)
        const response = await fetch(`/api/v1${path}`, {
          method: 'PUT',
          headers: {
            Accept: 'application/json',
            'Content-Type': multipart.contentType,
            'If-Match': `"${revision}"`,
            'Idempotency-Key': newULID(),
          },
          body: multipart.body,
        })
        if (response.status !== 202) throw await controllerResponseError(response, 'PUT', path)
        const accepted = (await response.json()) as BlueprintTaskAccepted
        if (!accepted.task_id) throw new Error('Controller response is missing task_id')
        return accepted
      },
      deleteEnvironment: async (envId) => {
        if (!isEnvironmentDeletionPending(envId)) assertEnvironmentMutable(envId, 'Environment deletion')
        const project = [...state.tenantProjects, ...state.backingProjects].find((candidate) =>
          candidate.environments?.some((environment) => environment.id === envId),
        )
        if (!project) return Promise.reject(new Error(`Environment ${envId} is not loaded`))
		const generation = nextEnvironmentGeneration(envId, 'delete')
		const taskId = await dispatchResourceRemoval({
			kind: 'environment', projectId: project.id, resourceId: envId, generation,
		})
		const task = await waitForResourceRemoval(taskId)
		if (task.status !== 'completed') {
			throw new Error(`Environment deletion Task ${taskId} ${task.status}`)
		}
		return taskId
      },
      addZone: async (envId, input) => {
        assertEnvironmentMutable(envId, 'Zone mutation')
        const body: ZoneCreateRequest = {
          environment_id: envId,
          name: input.name,
          subnet: input.subnet,
          internal: input.internal,
        }
        const zone = zoneFromAPI(await tenantRequest<ZoneCreateResponse>('/zones', 201, {
          method: 'POST', body,
        }))
        update((draft) => {
          findEnv(draft, envId)?.zones.push(zone)
        })
        return zone
      },
      getZone: async (zoneId) => zoneFromAPI(await tenantRequest<ZoneShowResponse>(`/zones/${encodeURIComponent(zoneId)}`, 200, { method: 'GET' })),
      getZoneRemovalImpact: (zoneId) => tenantRequest<ZoneRemovalImpactResponse>(`/zones/${encodeURIComponent(zoneId)}/removal-impact`, 200, { method: 'GET' }),
      removeZone: async (envId, zoneId, impactToken) => {
        assertEnvironmentMutable(envId, 'Zone mutation')
        const accepted = await tenantRequest<ZoneRemoveResponse>(
          `/zones/${encodeURIComponent(zoneId)}${impactToken ? `?impact_token=${encodeURIComponent(impactToken)}` : ''}`,
          202,
          { method: 'DELETE' },
        )
        if (!accepted.task_id) throw new Error('Controller response is missing task_id')
        pendingZoneRemovals.current.set(accepted.task_id, { envId, zoneId })
        return accepted.task_id
      },
      addService: async (envId, input) => {
        assertEnvironmentMutable(envId, 'Service mutation')
        const body: ServiceCreateRequest = { environment_id: envId, name: input.name, ...serviceMutationBody(input) }
        const created = serviceFromAPI(await tenantRequest<ServiceCreateResponse>('/services', 201, { method: 'POST', body }))
        update((draft) => { findEnv(draft, envId)?.services.push(created) })
        return created
      },
      updateService: async (envId, serviceId, input) => {
        assertEnvironmentMutable(envId, 'Service mutation')
        const body: ServiceEditRequest = serviceMutationBody(input)
        const edited = serviceFromAPI(await tenantRequest<ServiceEditResponse>(`/services/${encodeURIComponent(serviceId)}`, 200, { method: 'PATCH', body }))
        update((d) => {
          const e = findEnv(d, envId)
          if (!e) return
          const i = e.services.findIndex((s) => s.id === serviceId)
          if (i >= 0) e.services[i] = edited
        })
        return edited
      },
		getService: async (serviceId) => serviceFromAPI(await tenantRequest<ServiceShowResponse>(
			`/services/${encodeURIComponent(serviceId)}`,
			200,
			{ method: 'GET' },
		)),
		deleteService: (envId, serviceId) => (assertEnvironmentMutable(envId, 'Service mutation'), dispatchResourceRemoval({
			kind: 'service',
			environmentId: envId,
			resourceId: serviceId,
		})),
      runServiceRuntimeAction: async (envId, serviceId, action) => {
        assertEnvironmentMutable(envId, 'Service mutation')
        const accepted = await tenantRequest<ServiceRuntimeTaskAccepted>(
          `/services/${encodeURIComponent(serviceId)}/${action}`,
          202,
          { method: 'POST' },
        )
        const taskId = requireTaskId(accepted, `Service ${action}`)
        const intent: ServiceRuntimeIntent =
          action === 'start' ? 'running' : action === 'stop' ? 'stopped' : 'absent'
        update((d) => {
		  const service = findEnv(d, envId)?.services.find((candidate) => candidate.id === serviceId)
		  if (!service) return
		  service.runtimeIntent = intent
        })
        return taskId
      },
      addRoute: async (envId, route) => {
        assertEnvironmentMutable(envId, 'Route mutation')
        const generation = nextEnvironmentGeneration(envId, 'child')
        const body: RouteCreateRequest = {
          environment_id: envId,
          host: route.host || undefined,
          path: route.path,
          exposure: route.exposure,
          target_service_id: route.targetServiceId,
          target_port: route.targetPort,
        }
        const accepted = await tenantRequest<RouteCreateAccepted>('/routes', 202, {
          method: 'POST',
          body,
        })
        const created = routeFromAPI(accepted.route)
        update((d) => {
          // Public Routes never auto-enable ingress; Component lifecycle is not authored by C07.
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation) return
          findEnv(d, envId)?.routes.push(created)
        })
        return created
      },
      getRoute: async (routeId) => routeFromAPI(await tenantRequest<RouteShowResponse>(`/routes/${encodeURIComponent(routeId)}`, 200, { method: 'GET' })),
      updateRoute: async (envId, routeId, patch) => {
        assertEnvironmentMutable(envId, 'Route mutation')
        const generation = nextEnvironmentGeneration(envId, 'child')
        const body: RouteEditRequest = { exposure: patch.exposure }
        const accepted = await tenantRequest<RouteEditAccepted>(
          `/routes/${encodeURIComponent(routeId)}`,
          202,
          { method: 'PATCH', body },
        )
        const edited = routeFromAPI(accepted.route)
        update((d) => {
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation) return
          const route = findEnv(d, envId)?.routes.find((candidate) => candidate.id === routeId)
          if (!route) return
          Object.assign(route, edited)
        })
        return edited
      },
      removeRoute: (envId, routeId) => (assertEnvironmentMutable(envId, 'Route mutation'), dispatchResourceRemoval({
        kind: 'route',
        environmentId: envId,
        resourceId: routeId,
      })),
      addVolume: async (envId, input) => {
        assertEnvironmentMutable(envId, 'Volume mutation')
        const created = await createVolume(tenantRequest, envId, input)
        update((draft) => {
          findEnv(draft, envId)?.volumes.push(created)
        })
        return created
      },
      getVolume: async (volumeId) => {
        const volume = await getVolume(tenantRequest, volumeId)
        update((draft) => {
          const current = findEnv(draft, volume.environmentId)?.volumes.find((candidate) => candidate.id === volume.id)
          if (current) Object.assign(current, volume)
        })
        return volume
      },
      updateVolume: async (envId, volumeId, patch) => {
        assertEnvironmentMutable(envId, 'Volume mutation')
        const edited = await editVolume(tenantRequest, volumeId, patch.slug)
        update((draft) => {
          const volume = findEnv(draft, envId)?.volumes.find((candidate) => candidate.id === volumeId)
          if (volume) Object.assign(volume, edited)
        })
        return edited
      },
      getVolumeDeletionImpact: (volumeId, cursor = "", limit = 40) =>
        getVolumeDeletionImpact(tenantRequest, volumeId, cursor, limit),
      removeVolume: (envId, volumeId, impactToken, confirmKey) => {
        assertEnvironmentMutable(envId, 'Volume mutation')
        return removeVolume(tenantRequest, volumeId, impactToken, confirmKey)
      },
      addScript: async (envId, script) => {
        assertEnvironmentMutable(envId, 'Script mutation')
        const generation = nextEnvironmentGeneration(envId, 'child')
        const targetService = (await listAllServices(envId)).find((service) => service.name === script.service)
        if (!targetService) throw new Error(`Service ${script.service} was not found in this Environment`)
        const body: ScriptCreateRequest = {
          environment_id: envId,
          slug: script.slug,
          service_id: targetService.id,
          script: script.body,
          when: script.when,
        }
        const created = scriptFromAPI(await tenantRequest<ScriptCreateResponse>('/scripts', 201, {
          method: 'POST',
          body,
        }))
        update((d) => {
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation) return
          findEnv(d, envId)?.scripts.push(created)
        })
        return created
      },
      updateScript: async (envId, scriptId, patch) => {
        assertEnvironmentMutable(envId, 'Script mutation')
        const generation = nextEnvironmentGeneration(envId, 'child')
        const body: ScriptEditRequest = {}
		if (patch.slug !== undefined) body.slug = patch.slug
        if (patch.body !== undefined) body.script = patch.body
        if (patch.when !== undefined) body.when = patch.when
        const edited = scriptFromAPI(await tenantRequest<ScriptEditResponse>(
          `/scripts/${encodeURIComponent(scriptId)}`,
          200,
          { method: 'PATCH', body },
        ))
        update((d) => {
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation) return
          const script = findEnv(d, envId)?.scripts.find((candidate) => candidate.id === scriptId)
          if (script) Object.assign(script, edited)
        })
        return edited
      },
	  runScript: async (scriptId) => {
		const accepted = await tenantRequest<ScriptRunResponse>(`/scripts/${encodeURIComponent(scriptId)}/run`, 202, {
		  method: 'POST',
		  body: undefined,
		})
		return requireTaskId(accepted, 'Script run')
	  },
      removeScript: (envId, scriptId) => (assertEnvironmentMutable(envId, 'Script mutation'), dispatchResourceRemoval({
		kind: 'script',
		environmentId: envId,
		resourceId: scriptId,
	  })),
      addReleaseGroup: async (envId, group) => {
        assertEnvironmentMutable(envId, 'Release group mutation')
        const environment = findEnv(state, envId)
        if (!environment) throw new Error(`Environment ${envId} was not found`)
        const serviceIDs = new Map(environment.services.map((service) => [service.name, service.id]))
        const response = await tenantRequest<ReleaseGroupResponse>('/release-groups', 201, {
          method: 'POST',
          body: {
            environment_id: envId,
            name: group.name,
            service_ids: group.services.map((name) => serviceIDs.get(name) ?? name),
            order: group.order.map((name) => serviceIDs.get(name) ?? name),
            on_failure: group.onFailure,
          },
        })
		const names = new Map(environment.services.map((service) => [service.id, service.name]))
		const created: ReleaseGroup = {
			id: response.id, name: response.name,
			services: (response.service_ids ?? []).map((id) => names.get(id) ?? id),
			order: (response.order ?? []).map((id) => names.get(id) ?? id),
			tag: response.tag,
			onFailure: response.on_failure === 'leave_active' ? 'leave_active' : 'switch_back',
		}
		update((draft) => { findEnv(draft, envId)?.releaseGroups.push(created) })
		return created
      },
      updateReleaseGroup: async (envId, groupId, patch) => {
        assertEnvironmentMutable(envId, 'Release group mutation')
        const environment = findEnv(state, envId)
        if (!environment) throw new Error(`Environment ${envId} was not found`)
        const serviceIDs = new Map(environment.services.map((service) => [service.name, service.id]))
		const response = await tenantRequest<ReleaseGroupResponse>(`/release-groups/${encodeURIComponent(groupId)}`, 200, {
          method: 'PATCH',
          body: {
            name: patch.name,
            service_ids: patch.services.map((name) => serviceIDs.get(name) ?? name),
            order: patch.order.map((name) => serviceIDs.get(name) ?? name),
            on_failure: patch.onFailure,
          },
        })
		const names = new Map(environment.services.map((service) => [service.id, service.name]))
		const edited: ReleaseGroup = {
			id: response.id, name: response.name,
			services: (response.service_ids ?? []).map((id) => names.get(id) ?? id),
			order: (response.order ?? []).map((id) => names.get(id) ?? id),
			tag: response.tag,
			onFailure: response.on_failure === 'leave_active' ? 'leave_active' : 'switch_back',
		}
		update((draft) => {
			const groups = findEnv(draft, envId)?.releaseGroups
			const index = groups?.findIndex((candidate) => candidate.id === groupId) ?? -1
			if (groups && index >= 0) groups[index] = edited
		})
		return edited
      },
      removeReleaseGroup: async (_envId, groupId) => {
        assertEnvironmentMutable(_envId, 'Release group mutation')
        const response = await tenantRequest<ReleaseGroupMutationAccepted>(`/release-groups/${encodeURIComponent(groupId)}`, 202, { method: 'DELETE' })
        return requireTaskId(response, 'Release group removal')
      },
      deployReleaseGroup: async (_envId, groupId, tag) => {
        assertEnvironmentMutable(_envId, 'Release group mutation')
        const body = tag ? { tag } : {}
        const response = await tenantRequest<ReleaseGroupTaskAccepted>(`/release-groups/${encodeURIComponent(groupId)}/deploy`, 202, { method: 'POST', body })
        return requireTaskId(response, 'Release group deploy')
      },
      rollbackReleaseGroup: async (_envId, groupId) => {
        assertEnvironmentMutable(_envId, 'Release group mutation')
        const response = await tenantRequest<ReleaseGroupTaskAccepted>(`/release-groups/${encodeURIComponent(groupId)}/rollback`, 202, { method: 'POST' })
        return requireTaskId(response, 'Release group rollback')
      },
      addAttach: async (_envId, input) => {
        assertEnvironmentMutable(_envId, 'Attach mutation')
        const body: AttachCreateRequest = {
          service_id: input.serviceId,
          backing_service_id: input.backingServiceId,
          name: input.name,
          credential: input.credential.mode === 'new'
            ? { mode: 'new' }
            : { mode: 'existing', attach_id: input.credential.attachId },
          grant_attach_ids: input.credential.mode === 'new' ? input.grantAttachIds : undefined,
        }
        const response = await tenantRequest<AttachTaskAccepted>('/attaches', 202, { method: 'POST', body })
        return requireTaskId(response, 'Attach creation')
      },
      renameAttach: async (envId, attachId, name) => {
        assertEnvironmentMutable(envId, 'Attach mutation')
        const body: AttachRenameRequest = { name }
        const response = await tenantRequest<AttachRenameResponse>(
          `/attaches/${encodeURIComponent(attachId)}/rename`,
          200,
          { method: 'POST', body },
        )
        update((draft) => {
          const attach = findEnv(draft, envId)?.attaches.find((candidate) => candidate.id === attachId)
          if (attach) attach.name = response.name
        })
      },
      removeAttach: async (_envId, attachId) => {
        assertEnvironmentMutable(_envId, 'Attach mutation')
        const response = await tenantRequest<AttachTaskAccepted>(`/attaches/${encodeURIComponent(attachId)}`, 202, {
          method: 'DELETE',
        })
        return requireTaskId(response, 'Attach removal')
      },
      revealAttachFact: (attachId, key, grantAttachId) => revealAttachFactValue(attachId, key, grantAttachId),
      addEntry: async (envId, input) => {
        assertEnvironmentMutable(envId, 'Entry mutation')
        const generation = nextEnvironmentGeneration(envId, 'child')
        const response = await tenantRequest<EntryResponse>('/entries', 201, {
          method: 'POST',
          body: { ...input, environment_id: envId },
        })
        const entry = entryFromAPI(response)
        update((draft) => {
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation) return
          findEnv(draft, envId)?.entries.push(entry)
        })
        return entry
      },
      bulkUpsertEntries: async (envId, input) => {
        assertEnvironmentMutable(envId, 'Entry mutation')
        const generation = nextEnvironmentGeneration(envId, 'child')
        const response = await tenantRequest<EntryBulkUpsertResponse>('/entries/bulk', 202, {
          method: 'POST',
          body: { ...input, environment_id: envId },
        })
        const entries = (response.entries ?? []).map(entryFromAPI)
        update((draft) => {
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation) return
          const environment = findEnv(draft, envId)
          if (!environment) return
          const ids = new Set(entries.map((entry) => entry.id))
          environment.entries = environment.entries.filter((entry) => !ids.has(entry.id))
          environment.entries.push(...entries)
        })
        return response
      },
      updateEntry: async (envId, entryId, input) => {
        assertEnvironmentMutable(envId, 'Entry mutation')
        const generation = nextEnvironmentGeneration(envId, 'child')
        const response = await tenantRequest<EntryResponse>(`/entries/${encodeURIComponent(entryId)}`, 200, {
          method: 'PATCH',
          body: input,
        })
        const entry = entryFromAPI(response)
        update((draft) => {
          if ((environmentGenerations.current.get(envId) ?? 0) !== generation) return
          const environment = findEnv(draft, envId)
          if (!environment) return
          const index = environment.entries.findIndex((candidate) => candidate.id === entryId)
          if (index >= 0) environment.entries[index] = entry
        })
        return entry
      },
		removeEntry: (envId, entryId) => (assertEnvironmentMutable(envId, 'Entry mutation'), dispatchResourceRemoval({
			kind: 'entry',
			environmentId: envId,
			resourceId: entryId,
		})),
      revealEntry: async (entryId) => {
        const value = await tenantRequest<EntryValueResponse>(
          `/entries/${encodeURIComponent(entryId)}/value`,
          200,
        )
        return value.value
      },
      createReusableSecret: async (input) => {
        const body: SecretCreateRequest = {
          key: input.key,
          kind: input.kind === 'env' ? 'env_var' : 'file',
          value: input.value,
          ...(input.kind === 'file' ? { path: input.path } : {}),
          ...(input.projectId !== undefined ? { project_id: input.projectId } : { platform: true }),
        }
        const expectedScope: ExpectedSecretScope = input.projectId !== undefined
          ? { projectId: input.projectId }
          : { platform: true }
        const created = reusableSecretFromAPI(await tenantRequest<SecretResponse>('/secrets', 201, {
          method: 'POST',
          body,
        }), expectedScope)
        update((draft) => {
          draft.reusableSecrets = draft.reusableSecrets.filter((secret) => secret.id !== created.id)
          draft.reusableSecrets.push(created)
          draft.secretError = null
        })
        return created
      },
      removeReusableSecret: async (id) => {
        const secret = state.reusableSecrets.find((candidate) => candidate.id === id)
        if (!secret) throw new Error('Reusable Secret was not found')
        if (secret.scope === 'project' && !state.tenantProjects.some((project) => project.id === secret.projectId)) {
          throw new Error('Reusable Secret project owner was not found')
        }
        try {
          const accepted = await tenantRequest<SecretTaskAccepted>('/secrets/' + encodeURIComponent(id), 202, { method: 'DELETE' })
          if (!accepted.task_id) throw new Error('Controller response is missing task_id')
          update((draft) => { draft.secretError = null })
          return accepted
        } catch (error) {
          update((draft) => {
            draft.secretError = error instanceof Error ? error.message : 'Unable to remove reusable Secret'
          })
          throw error
        }
      },
      refreshReusableSecrets,
      revealReusableSecret: async (id) => {
        const revealed = await tenantRequest<SecretValueResponse>('/secrets/' + encodeURIComponent(id) + '/value', 200)
        return revealed.value
      },
      addConnector: async (connector, intent) => {
        const toAPI = (credential: ConnectorCredentialInput) =>
          credential.kind === 'ref' ? { secret_ref: credential.name } : { value: credential.value }
        const body: ConnectorCreateRequest = {
          name: connector.name,
          kind: connector.kind,
          endpoint: connector.endpoint,
          bucket: connector.bucket,
          prefix: connector.prefix || undefined,
          region: connector.region,
          path_style: connector.pathStyle,
          credentials: {
            access_key: toAPI(connector.credentials.accessKey),
            secret_key: toAPI(connector.credentials.secretKey),
          },
        }
        const query = new URLSearchParams({ environment: connector.scopeRef })
        try {
          const created = connectorFromAPI(await tenantRequest<ConnectorResponse>(`/connectors?${query}`, 201, {
            method: 'POST',
            body,
            idempotencyKey: intent.idempotencyKey,
          }))
          connectorEnvironmentGenerations.current.set(
            connector.scopeRef,
            (connectorEnvironmentGenerations.current.get(connector.scopeRef) ?? 0) + 1,
          )
          update((draft) => {
            draft.connectors = draft.connectors.filter((candidate) => candidate.id !== created.id)
            draft.connectors.push(created)
            draft.connectorError = null
          })
          return created
        } catch (error) {
          update((draft) => {
            draft.connectorError = error instanceof Error ? error.message : 'Unable to create Connector'
          })
          throw error
        }
      },
      removeConnector: async (id, intent) => {
        const pendingTask = [...pendingConnectorRemovals.current.entries()].find(
          ([, pending]) => pending.connectorId === id,
        )
        if (pendingTask) return pendingTask[0]
        const inFlight = connectorRemovalRequests.current.get(id)
        if (inFlight) return inFlight
        const request = (async () => {
          try {
            const connector = state.connectors.find((candidate) => candidate.id === id)
            if (!connector) throw new Error('Connector was not found')
            const accepted = await tenantRequest<ConnectorTaskAccepted>(
              `/connectors/${encodeURIComponent(id)}`,
              202,
              { method: 'DELETE', idempotencyKey: intent.idempotencyKey },
            )
            if (!accepted.task_id) throw new Error('Controller response is missing task_id')
            pendingConnectorRemovals.current.set(accepted.task_id, {
              connectorId: id,
              environmentId: connector.scopeRef,
            })
            persistPendingConnectorRemovals(pendingConnectorRemovals.current)
            update((draft) => {
              draft.connectorError = null
            })
            return accepted.task_id
          } catch (error) {
            update((draft) => {
              draft.connectorError = error instanceof Error
                ? error.message
                : 'Unable to remove Connector'
            })
            throw error
          }
        })()
        connectorRemovalRequests.current.set(id, request)
        try {
          return await request
        } finally {
          if (connectorRemovalRequests.current.get(id) === request) {
            connectorRemovalRequests.current.delete(id)
          }
        }
      },
      refreshRunners,
      createRunner,
      renameRunner,
      retryRunner,
      removeRunner,
		runBackingRuntimeAction: async (id, action) => {
			const accepted = await tenantRequest<BackingRuntimeTaskAccepted>(
				`/backing-services/${encodeURIComponent(id)}/${action}`,
				202,
				{ method: 'POST' },
			)
			const intent: ServiceRuntimeIntent =
				action === 'start' ? 'running' : action === 'stop' ? 'stopped' : 'absent'
			update((d) => {
				const project = d.backingProjects.find((candidate) => candidate.id === id)
				project?.environments?.forEach((environment) => {
					environment.services.forEach((service) => { service.runtimeIntent = intent })
				})
			})
			return requireTaskId(accepted, `Backing service ${action}`)
		},
      addBackingProject: async (input) => {
        const created = await tenantRequest<BackingServiceCreatedResponse>(
          '/backing-services',
          201,
          { method: 'POST', body: input },
        )
        requireTaskId(created, 'Backing service creation')
        const backingProjects = await listAllBackingProjects(
          state.tenantProjects,
          state.tenants,
          new AbortController().signal,
        )
        update((draft) => {
          draft.backingProjects = backingProjects
          draft.backingProjectError = null
        })
        return created
      },
      setComponentEnabled: async (componentId, enabled) => {
        const action = enabled ? 'enable' : 'disable'
        const accepted = await tenantRequest<ComponentTaskAccepted>(
          `/components/${encodeURIComponent(componentId)}/${action}`,
          202,
          { method: 'POST' },
        )
        if (!accepted.task_id) throw new Error(`Controller response is missing Component ${action} task_id`)
        return accepted.task_id
      },
      reconcileEnvironmentComponent: async (componentId) => {
        const accepted = await tenantRequest<ComponentTaskAccepted>(
          `/components/${encodeURIComponent(componentId)}/update`,
          202,
          { method: 'POST' },
        )
        if (!accepted.task_id) throw new Error('Controller response is missing Component update task_id')
        return accepted.task_id
      },
      updateComponentConfig: async (componentId, config) => {
        const result = await tenantRequest<ComponentConfigMutationResponse>(
          `/components/${encodeURIComponent(componentId)}/config`,
          200,
          { method: 'PUT', body: { config } },
        )
        if (!result.reconcile_task_id) throw new Error('Controller response is missing Component config reconcile_task_id')
        return result.reconcile_task_id
      },
    }
  }, [state, update, loadTaskJournal, loadBackupPolicy, replaceBackupPolicy, runBackup, rotateBackupKey, exportBackupKey, refreshPlatformComponents, refreshEnvironmentComponents, refreshEnvironmentReleases, refreshAgents, dispatchResourceRemoval, monitorResourceRemoval, reconcileResourceRemoval, requestResourceRemovalTask, nextEnvironmentGeneration, settleEnvironmentMutation, shouldPreserveEnvironmentOnLoad, getEnvironmentDeletionFailure, refreshEnvironmentDeletion, isEnvironmentDeletionPending, waitForResourceRemoval, retryResourceRemoval, observeEnvironmentDeletionTasks, environmentGenerations, assertEnvironmentMutable])

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>
}
export function useStore() {
  const ctx = useContext(Ctx)
  if (!ctx) throw new Error('useStore must be used within StoreProvider')
  return ctx
}
