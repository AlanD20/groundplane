import type { EnvironmentDeletionFailure, PendingResourceRemoval } from './environment-lifecycle'

export type PendingResourceRemovalIntent = {
  removal: PendingResourceRemoval
  idempotencyKey: string
}

export type ResourceRemovalRetryIntent = {
  removal: PendingResourceRemoval
  idempotencyKey: string
}

export type EnvironmentMutationIntent = {
  key: string
  kind: 'create' | 'edit' | 'rename'
  idempotencyKey: string
  projectId: string
  environmentId?: string
  name?: string
  networkPool?: string
  taskId?: string
}

export function environmentMutationKey(kind: EnvironmentMutationIntent['kind'], values: string[]) {
  return `${kind}:${JSON.stringify(values)}`
}

const pendingResourceStorageKey = 'groundplane-pending-environment-removals'
const pendingResourceIntentStorageKey = 'groundplane-pending-resource-removal-intents'
const retryIntentStorageKey = 'groundplane-resource-removal-retries'
const deletionFailureStorageKey = 'groundplane-environment-deletion-failures'
const mutationIntentStorageKey = 'groundplane-environment-mutation-intents'

export function resourceRemovalKey(removal: PendingResourceRemoval) {
  return `${removal.kind}:${removal.resourceId}`
}

function decodeRemoval(value: unknown): PendingResourceRemoval | undefined {
  if (!value || typeof value !== 'object') return undefined
  const candidate = value as Record<string, unknown>
  if (candidate.kind === 'environment' && typeof candidate.projectId === 'string' && typeof candidate.resourceId === 'string') {
    return {
      kind: 'environment', projectId: candidate.projectId, resourceId: candidate.resourceId,
      generation: typeof candidate.generation === 'number' && Number.isFinite(candidate.generation) ? candidate.generation : 0,
    }
  }
  if (
    (candidate.kind === 'service' || candidate.kind === 'route' || candidate.kind === 'entry' || candidate.kind === 'script') &&
    typeof candidate.environmentId === 'string' && typeof candidate.resourceId === 'string'
  ) {
    return {
      kind: candidate.kind,
      environmentId: candidate.environmentId,
      resourceId: candidate.resourceId,
      generation: typeof candidate.generation === 'number' && Number.isFinite(candidate.generation) ? candidate.generation : undefined,
    }
  }
  return undefined
}

function readEntries(key: string): unknown[] {
  if (typeof window === 'undefined') return []
  try {
    const stored = localStorage.getItem(key)
    if (!stored) return []
    const entries = JSON.parse(stored) as unknown
    return Array.isArray(entries) ? entries : []
  } catch {
    return []
  }
}

function writeEntries(key: string, entries: unknown[]) {
  if (typeof window === 'undefined') return
  try {
    if (entries.length === 0) localStorage.removeItem(key)
    else localStorage.setItem(key, JSON.stringify(entries))
  } catch {
    /* private mode */
  }
}

export function loadPendingResourceRemovals(): Map<string, PendingResourceRemoval> {
  return new Map(readEntries(pendingResourceStorageKey).flatMap((entry) => {
    if (!Array.isArray(entry) || entry.length !== 2) return []
    const [taskId, value] = entry as [unknown, unknown]
    const removal = decodeRemoval(value)
    return typeof taskId === 'string' && removal ? [[taskId, removal]] : []
  }))
}

export function persistPendingResourceRemovals(removals: Map<string, PendingResourceRemoval>) {
  writeEntries(pendingResourceStorageKey, [...removals])
}

export function loadPendingResourceRemovalIntents(): Map<string, PendingResourceRemovalIntent> {
  return new Map(readEntries(pendingResourceIntentStorageKey).flatMap((entry) => {
    if (!Array.isArray(entry) || entry.length !== 2) return []
    const [key, value] = entry as [unknown, unknown]
    if (typeof key !== 'string' || !value || typeof value !== 'object') return []
    const candidate = value as { removal?: unknown; idempotencyKey?: unknown }
    const removal = decodeRemoval(candidate.removal)
    if (!removal || resourceRemovalKey(removal) !== key || typeof candidate.idempotencyKey !== 'string' || candidate.idempotencyKey.length === 0) return []
    return [[key, { removal, idempotencyKey: candidate.idempotencyKey }]]
  }))
}

export function persistPendingResourceRemovalIntents(intents: Map<string, PendingResourceRemovalIntent>) {
  writeEntries(pendingResourceIntentStorageKey, [...intents])
}

export function pendingEnvironmentDeletionReplays(
  intents: Map<string, PendingResourceRemovalIntent>,
  tasks: Map<string, string>,
  pending: Map<string, PendingResourceRemoval>,
) {
  return [...intents.entries()].flatMap(([key, intent]) => {
    if (intent.removal.kind !== 'environment') return []
    const taskId = tasks.get(key)
    return taskId && pending.has(taskId) ? [] : [intent.removal]
  })
}

export function loadResourceRemovalRetryIntents(): Map<string, ResourceRemovalRetryIntent> {
  return new Map(readEntries(retryIntentStorageKey).flatMap((entry) => {
    if (!Array.isArray(entry) || entry.length !== 2) return []
    const [taskId, value] = entry as [unknown, unknown]
    if (typeof taskId !== 'string' || !value || typeof value !== 'object') return []
    const candidate = value as { removal?: unknown; idempotencyKey?: unknown }
    const removal = decodeRemoval(candidate.removal)
    if (!removal || typeof candidate.idempotencyKey !== 'string' || candidate.idempotencyKey.length === 0) return []
    return [[taskId, { removal, idempotencyKey: candidate.idempotencyKey }]]
  }))
}

export function persistResourceRemovalRetryIntents(intents: Map<string, ResourceRemovalRetryIntent>) {
  writeEntries(retryIntentStorageKey, [...intents])
}

export function loadEnvironmentDeletionFailures(): Map<string, EnvironmentDeletionFailure> {
  return new Map(readEntries(deletionFailureStorageKey).flatMap((entry) => {
    if (!Array.isArray(entry) || entry.length !== 2) return []
    const [environmentId, value] = entry as [unknown, unknown]
    if (typeof environmentId !== 'string' || !value || typeof value !== 'object') return []
    const candidate = value as Partial<EnvironmentDeletionFailure>
    if (typeof candidate.taskId !== 'string' || typeof candidate.projectId !== 'string' || typeof candidate.message !== 'string') return []
    return [[environmentId, {
      taskId: candidate.taskId,
      projectId: candidate.projectId,
      message: candidate.message,
      kind: candidate.kind === 'refresh' ? 'refresh' as const : 'task' as const,
    }]]
  }))
}

export function persistEnvironmentDeletionFailures(failures: Map<string, EnvironmentDeletionFailure>) {
  writeEntries(deletionFailureStorageKey, [...failures])
}

function decodeMutationIntent(value: unknown): EnvironmentMutationIntent | undefined {
  if (!value || typeof value !== 'object') return undefined
  const candidate = value as Partial<EnvironmentMutationIntent>
  if (
    typeof candidate.key !== 'string' ||
    (candidate.kind !== 'create' && candidate.kind !== 'edit' && candidate.kind !== 'rename') ||
    typeof candidate.idempotencyKey !== 'string' || candidate.idempotencyKey.length === 0 ||
    typeof candidate.projectId !== 'string'
  ) return undefined
  if (candidate.kind !== 'create' && typeof candidate.environmentId !== 'string') return undefined
  if (candidate.kind === 'create' && (typeof candidate.name !== 'string' || typeof candidate.networkPool !== 'string')) return undefined
  if (candidate.taskId !== undefined && typeof candidate.taskId !== 'string') return undefined
  return candidate as EnvironmentMutationIntent
}

export function loadEnvironmentMutationIntents(): Map<string, EnvironmentMutationIntent> {
  return new Map(readEntries(mutationIntentStorageKey).flatMap((entry) => {
    if (!Array.isArray(entry) || entry.length !== 2) return []
    const [key, value] = entry as [unknown, unknown]
    const intent = decodeMutationIntent(value)
    return typeof key === 'string' && intent?.key === key ? [[key, intent]] : []
  }))
}

export function persistEnvironmentMutationIntents(intents: Map<string, EnvironmentMutationIntent>) {
  writeEntries(mutationIntentStorageKey, [...intents])
}
