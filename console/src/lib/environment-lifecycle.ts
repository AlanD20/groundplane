import { useCallback, useEffect, useRef, type MutableRefObject } from 'react'
import type { operations } from './api.generated'
import type { Environment, EnvironmentEntry, Project, Route, Script, Service } from './types'
import {
  loadEnvironmentDeletionFailures,
  loadPendingResourceRemovalIntents,
  loadPendingResourceRemovals,
  loadResourceRemovalRetryIntents,
  persistEnvironmentDeletionFailures,
  persistPendingResourceRemovalIntents,
  persistPendingResourceRemovals,
  persistResourceRemovalRetryIntents,
  pendingEnvironmentDeletionReplays,
  resourceRemovalKey,
  type PendingResourceRemovalIntent,
  type ResourceRemovalRetryIntent,
} from './environment-storage'
import { newULID } from './utils'
import { isAuthoritativeTaskUnavailable, isDefinitiveRemovalRequestRejection } from './environment-removal-state'

export type TaskResponse = operations['task.show']['responses'][200]['content']['application/json']
export type EnvironmentDeletionResponse = { task_id: string }

export type PendingResourceRemoval =
  | { kind: 'environment'; projectId: string; resourceId: string; generation: number }
  | { kind: 'service' | 'route' | 'entry' | 'script'; environmentId: string; resourceId: string; generation?: number }

export type EnvironmentMutationKind = 'create' | 'edit' | 'rename' | 'delete' | 'child'

export type EnvironmentDeletionFailure = {
  taskId: string
  projectId: string
  message: string
  kind: 'task' | 'refresh'
}

type EnvironmentLifecycleDraft = {
  tenantProjects: Project[]
  backingProjects: Project[]
  projectError: string | null
  environmentDeletionRevision: number
}

type EnvironmentLifecycleOptions<State extends EnvironmentLifecycleDraft> = {
  active: MutableRefObject<boolean>
  update: (fn: (draft: State) => void) => void
  requestTask: RequestTask
  deleteResource: DeleteResource
  retryResource: RetryResource
  listEnvironments: (projectId: string, signal?: AbortSignal) => Promise<Environment[]>
  listServices: (environmentId: string, signal?: AbortSignal) => Promise<Service[]>
  listRoutes: (environmentId: string, signal?: AbortSignal) => Promise<Route[]>
  listEntries: (environmentId: string, signal?: AbortSignal) => Promise<EnvironmentEntry[]>
  listScripts: (environmentId: string, signal?: AbortSignal) => Promise<Script[]>
}

type RequestTask = (taskId: string, signal?: AbortSignal) => Promise<TaskResponse>
type DeleteResource = (resource: 'environments' | 'services' | 'routes' | 'entries' | 'scripts', id: string, idempotencyKey: string) => Promise<EnvironmentDeletionResponse>
type RetryResource = (taskId: string, idempotencyKey: string) => Promise<EnvironmentDeletionResponse>

export type EnvironmentLifecycle<State extends EnvironmentLifecycleDraft> = {
  pendingResourceRemovals: MutableRefObject<Map<string, PendingResourceRemoval>>
  requestResourceRemovalTask: RequestTask
  monitorResourceRemoval: (taskId: string) => void
  reconcileResourceRemoval: (taskId: string, task: TaskResponse) => Promise<void>
  dispatchResourceRemoval: (removal: PendingResourceRemoval) => Promise<string>
  retryResourceRemoval: (taskId: string) => Promise<string> | undefined
  waitForResourceRemoval: (taskId: string) => Promise<TaskResponse>
  nextEnvironmentGeneration: (environmentId: string, kind?: EnvironmentMutationKind) => number
  settleEnvironmentMutation: (environmentId: string, generation: number, outcome: 'succeeded' | 'failed') => void
  shouldPreserveEnvironmentOnLoad: (environmentId: string, snapshotGeneration: number) => boolean
  getEnvironmentDeletionFailure: (environmentId: string) => EnvironmentDeletionFailure | null
  refreshEnvironmentDeletion: (environmentId: string) => Promise<void>
  isEnvironmentDeletionPending: (environmentId: string) => boolean
  observeEnvironmentDeletionTasks: (projects: Project[]) => void
  environmentGenerations: MutableRefObject<Map<string, number>>
}

const resourceRemovalObservationFreshMs = 1_500
const terminalTaskStatuses = new Set(['completed', 'failed', 'timed_out', 'aborted'])
function findEnvironment(draft: EnvironmentLifecycleDraft, environmentId: string) {
  for (const project of [...draft.tenantProjects, ...draft.backingProjects]) {
    const environment = project.environments?.find((candidate) => candidate.id === environmentId)
    if (environment) return environment
  }
  return undefined
}

function environmentDeletionTaskError(taskId: string, task: TaskResponse, removal: PendingResourceRemoval) {
  if (task.id !== taskId) return `Controller returned Task ${task.id} while observing ${taskId}`
  if (task.type !== 'remove') return `Controller returned ${task.type} Task ${taskId} for Environment deletion`
  if (task.target !== removal.resourceId) return `Controller returned deletion Task ${taskId} for a different Environment`
  if (removal.kind === 'environment' && task.project_id !== removal.projectId) return `Controller returned deletion Task ${taskId} for a different Project`
  const environmentId = removal.kind === 'environment' ? removal.resourceId : removal.environmentId
  if (task.environment_id && task.environment_id !== environmentId) return `Controller returned deletion Task ${taskId} for a different Environment`
  return undefined
}
export function useEnvironmentLifecycle<State extends EnvironmentLifecycleDraft>(options: EnvironmentLifecycleOptions<State>): EnvironmentLifecycle<State> {
  const { active, update, requestTask, deleteResource, retryResource, listEnvironments, listServices, listRoutes, listEntries, listScripts } = options
  const pendingResourceRemovals = useRef(loadPendingResourceRemovals())
  const resourceRemovalTasks = useRef(new Map<string, string>(
    [...pendingResourceRemovals.current].map(([taskId, removal]) => [resourceRemovalKey(removal), taskId]),
  ))
  const resourceRemovalDispatches = useRef(new Map<string, Promise<string>>())
  const resourceRemovalRetries = useRef(new Map<string, Promise<string>>())
  const resourceRemovalRetryIntents = useRef<Map<string, ResourceRemovalRetryIntent>>(loadResourceRemovalRetryIntents())
  const resourceRemovalTaskRequests = useRef(new Map<string, Promise<TaskResponse>>())
  const resourceRemovalTaskControllers = useRef(new Map<string, AbortController>())
  const resourceRemovalObservations = useRef(new Map<string, { task: TaskResponse; observedAt: number }>())
  const resourceRemovalReconciliations = useRef(new Map<string, Promise<void>>())
  const resourceRemovalReconciliationControllers = useRef(new Map<string, AbortController>())
  const resourceRemovalMonitors = useRef(new Map<string, () => void>())
  const resourceRemovalErrors = useRef(new Map<string, string>())
  const resourceRemovalIntents = useRef(loadPendingResourceRemovalIntents())
  const environmentDeletionFailures = useRef(loadEnvironmentDeletionFailures())
  const environmentGenerations = useRef(new Map<string, number>(
    [...pendingResourceRemovals.current].flatMap(([, removal]) => (
      removal.kind === 'environment' ? [[removal.resourceId, removal.generation] as [string, number]] : []
    )),
  ))
  const environmentMutationOutcomes = useRef(new Map<string, {
    generation: number
    kind: EnvironmentMutationKind
    outcome: 'pending' | 'succeeded' | 'failed'
  }>(
    [...pendingResourceRemovals.current].flatMap(([, removal]) => (
      removal.kind === 'environment'
        ? [[removal.resourceId, { generation: removal.generation, kind: 'delete' as const, outcome: 'pending' as const }]]
        : []
    )),
  ))
  const successfulEnvironmentDeletions = useRef(new Map<string, string>())
  const nextEnvironmentGeneration = useCallback((environmentId: string, kind: EnvironmentMutationKind = 'child') => {
    const generation = (environmentGenerations.current.get(environmentId) ?? 0) + 1
    environmentGenerations.current.set(environmentId, generation)
    environmentMutationOutcomes.current.set(environmentId, { generation, kind, outcome: 'pending' })
    if (kind === 'create' || kind === 'delete') successfulEnvironmentDeletions.current.delete(environmentId)
    return generation
  }, [])
  const settleEnvironmentMutation = useCallback((environmentId: string, generation: number, outcome: 'succeeded' | 'failed') => {
    const current = environmentMutationOutcomes.current.get(environmentId)
    if (current?.generation === generation) current.outcome = outcome
  }, [])

  const shouldPreserveEnvironmentOnLoad = useCallback((environmentId: string, snapshotGeneration: number) => {
    if (successfulEnvironmentDeletions.current.has(environmentId)) return false
    const currentGeneration = environmentGenerations.current.get(environmentId) ?? 0
    const mutation = environmentMutationOutcomes.current.get(environmentId)
    if (mutation?.generation === currentGeneration && mutation.kind === 'delete' && mutation.outcome === 'succeeded') return false
    return currentGeneration === snapshotGeneration || mutation?.generation === currentGeneration
  }, [])

  const getEnvironmentDeletionFailure = useCallback((environmentId: string) => {
    return environmentDeletionFailures.current.get(environmentId) ?? null
  }, [])

  const isEnvironmentDeletionPending = useCallback((environmentId: string) => {
    return [...pendingResourceRemovals.current.values()].some((removal) => (
      removal.kind === 'environment' && removal.resourceId === environmentId
    )) || [...resourceRemovalIntents.current.values()].some(({ removal }) => (
      removal.kind === 'environment' && removal.resourceId === environmentId
    ))
  }, [])

  const forgetResourceRemoval = useCallback((taskId: string, removal: PendingResourceRemoval) => {
    pendingResourceRemovals.current.delete(taskId)
    resourceRemovalObservations.current.delete(taskId)
    resourceRemovalErrors.current.delete(taskId)
    if (removal.kind === 'environment' && environmentDeletionFailures.current.get(removal.resourceId)?.taskId === taskId) {
      environmentDeletionFailures.current.delete(removal.resourceId)
      persistEnvironmentDeletionFailures(environmentDeletionFailures.current)
    }
    const key = resourceRemovalKey(removal)
    if (resourceRemovalTasks.current.get(key) === taskId) resourceRemovalTasks.current.delete(key)
    if (resourceRemovalIntents.current.get(key)?.removal.resourceId === removal.resourceId) {
      resourceRemovalIntents.current.delete(key)
      persistPendingResourceRemovalIntents(resourceRemovalIntents.current)
    }
    persistPendingResourceRemovals(pendingResourceRemovals.current)
    update((draft) => {
      draft.environmentDeletionRevision += 1
      if (removal.kind === 'environment') {
        const environment = findEnvironment(draft, removal.resourceId)
        if (environment?.deletionTaskId === taskId) environment.deletionTaskId = null
      }
    })
  }, [update])

  const recordResourceRemovalError = useCallback((taskId: string, removal: PendingResourceRemoval, message: string, kind: 'task' | 'refresh' = 'task') => {
    resourceRemovalErrors.current.set(taskId, message)
    if (removal.kind === 'environment') {
      environmentDeletionFailures.current.set(removal.resourceId, {
        taskId, projectId: removal.projectId, message, kind,
      })
      persistEnvironmentDeletionFailures(environmentDeletionFailures.current)
      settleEnvironmentMutation(removal.resourceId, removal.generation, 'failed')
      update((draft) => { draft.projectError = message })
    }
  }, [settleEnvironmentMutation, update])

  const requestResourceRemovalTask = useCallback((taskId: string) => {
    const current = resourceRemovalTaskRequests.current.get(taskId)
    if (current) return current
    const removal = pendingResourceRemovals.current.get(taskId)
    const controller = new AbortController()
    resourceRemovalTaskControllers.current.set(taskId, controller)
    const request = requestTask(taskId, controller.signal).then((task) => {
      const validationError = removal ? environmentDeletionTaskError(taskId, task, removal) : undefined
      if (validationError) {
        recordResourceRemovalError(taskId, removal!, validationError, 'refresh')
        throw new Error(validationError)
      }
      if (active.current && !controller.signal.aborted && removal && pendingResourceRemovals.current.get(taskId) === removal) {
        resourceRemovalObservations.current.set(taskId, { task, observedAt: Date.now() })
      }
      return task
    }).catch((error) => {
      if (removal && isAuthoritativeTaskUnavailable(error)) {
        recordResourceRemovalError(
          taskId,
          removal,
          `Unable to observe deletion Task ${taskId}: Controller no longer has this Task; refresh the Environment to recover`,
          'refresh',
        )
      }
      throw error
    }).finally(() => {
      if (resourceRemovalTaskRequests.current.get(taskId) === request) resourceRemovalTaskRequests.current.delete(taskId)
      if (resourceRemovalTaskControllers.current.get(taskId) === controller) resourceRemovalTaskControllers.current.delete(taskId)
    })
    resourceRemovalTaskRequests.current.set(taskId, request)
    return request
  }, [active, recordResourceRemovalError, requestTask])

  const reconcileResourceRemoval = useCallback((taskId: string, task: TaskResponse) => {
    const removal = pendingResourceRemovals.current.get(taskId)
    if (!removal || !terminalTaskStatuses.has(task.status)) return Promise.resolve()
    const validationError = environmentDeletionTaskError(taskId, task, removal)
    if (validationError) {
      recordResourceRemovalError(taskId, removal, validationError, 'refresh')
      return Promise.reject(new Error(validationError))
    }
    if (task.status !== 'completed') {
      const label = removal.kind[0].toUpperCase() + removal.kind.slice(1)
      const message = `${label} deletion task ${taskId} ${task.status}`
      recordResourceRemovalError(taskId, removal, message)
      return Promise.resolve()
    }
    const current = resourceRemovalReconciliations.current.get(taskId)
    if (current) return current
    const controller = new AbortController()
    resourceRemovalReconciliationControllers.current.set(taskId, controller)
    const reconciliation = (async () => {
      try {
        const refreshed = removal.kind === 'environment'
          ? { kind: 'environment' as const, projectId: removal.projectId, resources: await listEnvironments(removal.projectId, controller.signal) }
          : removal.kind === 'service'
            ? { kind: 'service' as const, environmentId: removal.environmentId, resources: await listServices(removal.environmentId, controller.signal) }
            : removal.kind === 'route'
            ? { kind: 'route' as const, environmentId: removal.environmentId, resources: await listRoutes(removal.environmentId, controller.signal) }
            : removal.kind === 'entry'
              ? { kind: 'entry' as const, environmentId: removal.environmentId, resources: await listEntries(removal.environmentId, controller.signal) }
              : { kind: 'script' as const, environmentId: removal.environmentId, resources: await listScripts(removal.environmentId, controller.signal) }
        if (!active.current || controller.signal.aborted) return
        if (refreshed.kind === 'environment' && refreshed.resources.some((candidate) => candidate.id === removal.resourceId)) {
          throw new Error(`Authoritative Environment list still contains ${removal.resourceId} after deletion`)
        }
        const currentGeneration = removal.kind === 'environment'
          ? environmentGenerations.current.get(removal.resourceId) ?? 0
          : environmentGenerations.current.get(removal.environmentId) ?? 0
        const generationChanged = removal.generation !== undefined && currentGeneration !== removal.generation
        const relatedTaskIds = [...pendingResourceRemovals.current.entries()]
          .filter(([, candidate]) => candidate === removal)
          .map(([relatedTaskId]) => relatedTaskId)
        const clearedErrors = relatedTaskIds
          .map((relatedTaskId) => resourceRemovalErrors.current.get(relatedTaskId))
          .filter((message): message is string => !!message)
        for (const relatedTaskId of relatedTaskIds) forgetResourceRemoval(relatedTaskId, removal)
        if (removal.kind === 'environment') {
          successfulEnvironmentDeletions.current.set(removal.resourceId, taskId)
          settleEnvironmentMutation(removal.resourceId, removal.generation, 'succeeded')
        }
        update((draft) => {
          switch (refreshed.kind) {
            case 'environment': {
              const project = [...draft.tenantProjects, ...draft.backingProjects]
                .find((candidate) => candidate.id === refreshed.projectId)
              if (project) {
                project.environments = generationChanged
                  ? (project.environments ?? []).filter((candidate) => candidate.id !== removal.resourceId)
                  : refreshed.resources
              }
              break
            }
            case 'route': {
              const environment = findEnvironment(draft, refreshed.environmentId)
              if (environment && !generationChanged) environment.routes = refreshed.resources
              break
            }
            case 'service': {
              const environment = findEnvironment(draft, refreshed.environmentId)
              if (environment && !generationChanged) environment.services = refreshed.resources
              break
            }
            case 'entry': {
              const environment = findEnvironment(draft, refreshed.environmentId)
              if (environment && !generationChanged) environment.entries = refreshed.resources
              break
            }
            case 'script': {
              const environment = findEnvironment(draft, refreshed.environmentId)
              if (environment && !generationChanged) environment.scripts = refreshed.resources
              break
            }
          }
          if (draft.projectError && clearedErrors.includes(draft.projectError)) {
            draft.projectError = [...resourceRemovalErrors.current.values()].at(-1) ?? null
          }
        })
      } catch (error) {
        if (!active.current || controller.signal.aborted) throw error
        const label = removal.kind[0].toUpperCase() + removal.kind.slice(1)
        const detail = error instanceof Error ? error.message : `Unable to refresh ${label}s`
        const message = `${label} removal completed, but authoritative refresh failed: ${detail}`
        recordResourceRemovalError(taskId, removal, message, 'refresh')
        throw error
      }
    })()
    resourceRemovalReconciliations.current.set(taskId, reconciliation)
    void reconciliation.finally(() => {
      if (resourceRemovalReconciliations.current.get(taskId) === reconciliation) resourceRemovalReconciliations.current.delete(taskId)
      if (resourceRemovalReconciliationControllers.current.get(taskId) === controller) resourceRemovalReconciliationControllers.current.delete(taskId)
    }).catch(() => undefined)
    return reconciliation
  }, [active, forgetResourceRemoval, listEntries, listEnvironments, listRoutes, listScripts, listServices, recordResourceRemovalError, settleEnvironmentMutation, update])

  const monitorResourceRemoval = useCallback((taskId: string) => {
    if (!pendingResourceRemovals.current.has(taskId) || resourceRemovalMonitors.current.has(taskId)) return
    let stopped = false
    let polling = false
    let timer: ReturnType<typeof setTimeout> | undefined
    const stop = () => {
      if (stopped) return
      stopped = true
      if (timer) clearTimeout(timer)
      if (resourceRemovalMonitors.current.get(taskId) === stop) resourceRemovalMonitors.current.delete(taskId)
    }
    const schedule = () => {
      if (!stopped) timer = setTimeout(() => void poll(), 1_000)
    }
    const poll = async () => {
      if (stopped || polling || !active.current) return
      if (!pendingResourceRemovals.current.has(taskId)) return stop()
      polling = true
      try {
        const observation = resourceRemovalObservations.current.get(taskId)
        if (observation && terminalTaskStatuses.has(observation.task.status)) {
          await reconcileResourceRemoval(taskId, observation.task)
          return stop()
        }
        if (observation && Date.now() - observation.observedAt < resourceRemovalObservationFreshMs) return schedule()
        const task = await requestResourceRemovalTask(taskId)
        if (!terminalTaskStatuses.has(task.status)) return schedule()
        await reconcileResourceRemoval(taskId, task)
        stop()
      } catch {
        if (resourceRemovalErrors.current.has(taskId)) stop()
        else schedule()
      } finally {
        polling = false
      }
    }
    resourceRemovalMonitors.current.set(taskId, stop)
    void poll()
  }, [active, reconcileResourceRemoval, requestResourceRemovalTask])

  const dispatchResourceRemoval = useCallback((removal: PendingResourceRemoval) => {
    if (removal.kind !== 'environment' && isEnvironmentDeletionPending(removal.environmentId)) {
      return Promise.reject(new Error(`Environment ${removal.environmentId} deletion is in progress; ${removal.kind} mutation is disabled`))
    }
    const key = resourceRemovalKey(removal)
    const existingTaskId = resourceRemovalTasks.current.get(key)
    if (existingTaskId && pendingResourceRemovals.current.has(existingTaskId)) {
      monitorResourceRemoval(existingTaskId)
      return Promise.resolve(existingTaskId)
    }
    const inFlight = resourceRemovalDispatches.current.get(key)
    if (inFlight) return inFlight
    const trackedRemoval: PendingResourceRemoval = removal.kind === 'environment'
      ? removal
      : { ...removal, generation: nextEnvironmentGeneration(removal.environmentId, 'child') }
    const resource = removal.kind === 'environment' ? 'environments' : removal.kind === 'route' ? 'routes' : removal.kind === 'entry' ? 'entries' : 'scripts'
    const intent = resourceRemovalIntents.current.get(key) ?? {
      removal: trackedRemoval,
      idempotencyKey: `groundplane:${newULID()}`,
    }
    resourceRemovalIntents.current.set(key, intent)
    persistPendingResourceRemovalIntents(resourceRemovalIntents.current)
    update((draft) => { draft.environmentDeletionRevision += 1 })
    const request = deleteResource(resource, trackedRemoval.resourceId, intent.idempotencyKey).then((accepted) => {
      if (!accepted.task_id) throw new Error('Controller response is missing task_id')
      pendingResourceRemovals.current.set(accepted.task_id, trackedRemoval)
      resourceRemovalTasks.current.set(key, accepted.task_id)
      persistPendingResourceRemovals(pendingResourceRemovals.current)
      update((draft) => {
        draft.environmentDeletionRevision += 1
        if (trackedRemoval.kind === 'environment') {
          const environment = findEnvironment(draft, trackedRemoval.resourceId)
          if (environment) environment.deletionTaskId = accepted.task_id
        }
      })
      monitorResourceRemoval(accepted.task_id)
      return accepted.task_id
    }).catch((error) => {
      if (isDefinitiveRemovalRequestRejection(error)) {
        resourceRemovalIntents.current.delete(key)
        const taskId = resourceRemovalTasks.current.get(key)
        if (taskId && !pendingResourceRemovals.current.has(taskId)) resourceRemovalTasks.current.delete(key)
        persistPendingResourceRemovalIntents(resourceRemovalIntents.current)
        update((draft) => {
          draft.environmentDeletionRevision += 1
          if (trackedRemoval.kind === 'environment') {
            const environment = findEnvironment(draft, trackedRemoval.resourceId)
            if (environment) environment.deletionTaskId = null
          }
        })
      }
      throw error
    }).finally(() => {
      if (resourceRemovalDispatches.current.get(key) === request) resourceRemovalDispatches.current.delete(key)
    })
    resourceRemovalDispatches.current.set(key, request)
    return request
  }, [deleteResource, isEnvironmentDeletionPending, monitorResourceRemoval, nextEnvironmentGeneration, update])

  const retryResourceRemoval = useCallback((taskId: string) => {
    let removal = pendingResourceRemovals.current.get(taskId)
    if (!removal) {
      const failure = [...environmentDeletionFailures.current.entries()].find(([, candidate]) => candidate.taskId === taskId)
      if (failure) {
        removal = {
          kind: 'environment',
          projectId: failure[1].projectId,
          resourceId: failure[0],
          generation: environmentGenerations.current.get(failure[0]) ?? 0,
        }
        pendingResourceRemovals.current.set(taskId, removal)
        resourceRemovalTasks.current.set(resourceRemovalKey(removal), taskId)
        persistPendingResourceRemovals(pendingResourceRemovals.current)
      }
    }
    if (!removal) return undefined
    if (removal.kind === 'environment') {
      const currentGeneration = environmentGenerations.current.get(removal.resourceId) ?? removal.generation
      if (currentGeneration !== removal.generation) {
        removal = { ...removal, generation: currentGeneration }
        pendingResourceRemovals.current.set(taskId, removal)
        persistPendingResourceRemovals(pendingResourceRemovals.current)
      }
    }
    const previousFailure = removal.kind === 'environment'
      ? environmentDeletionFailures.current.get(removal.resourceId)
      : undefined
    const inFlight = resourceRemovalRetries.current.get(taskId)
    if (inFlight) return inFlight
    const intent = resourceRemovalRetryIntents.current.get(taskId) ?? {
      removal,
      idempotencyKey: `groundplane:${newULID()}`,
    }
    resourceRemovalRetryIntents.current.set(taskId, intent)
    persistResourceRemovalRetryIntents(resourceRemovalRetryIntents.current)
    const request = retryResource(taskId, intent.idempotencyKey).then((accepted) => {
      if (!accepted.task_id) throw new Error('Controller response is missing task_id')
      forgetResourceRemoval(taskId, removal)
      const currentGeneration = removal.kind === 'environment'
        ? environmentGenerations.current.get(removal.resourceId) ?? removal.generation
        : environmentGenerations.current.get(removal.environmentId) ?? removal.generation ?? 0
      const acceptedGeneration = currentGeneration ?? 0
      const acceptedRemoval: PendingResourceRemoval = { ...removal, generation: acceptedGeneration }
      if (previousFailure) update((draft) => {
        if (draft.projectError === previousFailure.message) draft.projectError = null
      })
      if (removal.kind === 'environment') {
        environmentMutationOutcomes.current.set(removal.resourceId, {
          generation: acceptedGeneration, kind: 'delete', outcome: 'pending',
        })
      }
      pendingResourceRemovals.current.set(accepted.task_id, acceptedRemoval)
      resourceRemovalTasks.current.set(resourceRemovalKey(acceptedRemoval), accepted.task_id)
      persistPendingResourceRemovals(pendingResourceRemovals.current)
      update((draft) => {
        draft.environmentDeletionRevision += 1
        if (acceptedRemoval.kind === 'environment') {
          const environment = findEnvironment(draft, acceptedRemoval.resourceId)
          if (environment) environment.deletionTaskId = accepted.task_id
        }
      })
      resourceRemovalRetryIntents.current.delete(taskId)
      persistResourceRemovalRetryIntents(resourceRemovalRetryIntents.current)
      monitorResourceRemoval(accepted.task_id)
      return accepted.task_id
    }).finally(() => {
      if (resourceRemovalRetries.current.get(taskId) === request) resourceRemovalRetries.current.delete(taskId)
    })
    resourceRemovalRetries.current.set(taskId, request)
    return request
  }, [forgetResourceRemoval, monitorResourceRemoval, retryResource, update])

  const waitForResourceRemoval = useCallback(async (taskId: string) => {
    let delayMs = 250
    while (active.current) {
      const task = await requestResourceRemovalTask(taskId)
      const removal = pendingResourceRemovals.current.get(taskId)
      if (removal?.kind === 'environment' && task.target !== removal.resourceId) {
        throw new Error(`Controller returned deletion Task ${taskId} for a different Environment`)
      }
      if (terminalTaskStatuses.has(task.status)) {
        await reconcileResourceRemoval(taskId, task)
        if (task.status !== 'completed') throw new Error(`Environment deletion Task ${taskId} ${task.status}`)
        return task
      }
      await new Promise((resolve) => setTimeout(resolve, delayMs))
      delayMs = Math.min(delayMs * 2, 2_000)
    }
    throw new Error('Environment deletion observation stopped')
  }, [active, reconcileResourceRemoval, requestResourceRemovalTask])

  const refreshEnvironmentDeletion = useCallback(async (environmentId: string) => {
    let taskId = environmentDeletionFailures.current.get(environmentId)?.taskId
    let removal = taskId ? pendingResourceRemovals.current.get(taskId) : undefined
    if (!removal) {
      const pending = [...pendingResourceRemovals.current.entries()].find(([, candidate]) => (
        candidate.kind === 'environment' && candidate.resourceId === environmentId
      ))
      if (pending) {
        taskId = pending[0]
        removal = pending[1]
      }
    }
    if (!taskId || !removal || removal.kind !== 'environment') throw new Error(`Environment deletion ${environmentId} has no observable Task`)
    const task = await requestResourceRemovalTask(taskId)
    await reconcileResourceRemoval(taskId, task)
    if (task.status !== 'completed') throw new Error(`Environment deletion Task ${taskId} ${task.status}`)
  }, [reconcileResourceRemoval, requestResourceRemovalTask])

  const observeEnvironmentDeletionTasks = useCallback((projects: Project[]) => {
    for (const project of projects) {
      for (const environment of project.environments ?? []) {
        if (!environment.deletionTaskId) continue
        if (successfulEnvironmentDeletions.current.has(environment.id)) continue
        for (const [taskId, removal] of pendingResourceRemovals.current) {
          if (removal.kind === 'environment' && removal.resourceId === environment.id && taskId !== environment.deletionTaskId) {
            pendingResourceRemovals.current.delete(taskId)
          }
        }
        const generation = environmentGenerations.current.get(environment.id) ?? 0
        environmentMutationOutcomes.current.set(environment.id, {
          generation, kind: 'delete', outcome: 'pending',
        })
        pendingResourceRemovals.current.set(environment.deletionTaskId, {
          kind: 'environment', projectId: project.id, resourceId: environment.id, generation,
        })
        resourceRemovalTasks.current.set(`environment:${environment.id}`, environment.deletionTaskId)
        persistPendingResourceRemovals(pendingResourceRemovals.current)
        monitorResourceRemoval(environment.deletionTaskId)
      }
    }
  }, [monitorResourceRemoval])

  useEffect(() => {
    active.current = true
    for (const removal of pendingEnvironmentDeletionReplays(resourceRemovalIntents.current, resourceRemovalTasks.current, pendingResourceRemovals.current)) {
      void dispatchResourceRemoval(removal).catch(() => undefined)
    }
    for (const taskId of pendingResourceRemovals.current.keys()) monitorResourceRemoval(taskId)
    return () => {
      active.current = false
      for (const stop of resourceRemovalMonitors.current.values()) stop()
      for (const controller of resourceRemovalTaskControllers.current.values()) controller.abort()
      for (const controller of resourceRemovalReconciliationControllers.current.values()) controller.abort()
    }
  }, [active, dispatchResourceRemoval, monitorResourceRemoval])

  useEffect(() => {
    const failure = [...environmentDeletionFailures.current.values()][0]
    if (failure) update((draft) => {
      if (!draft.projectError) draft.projectError = failure.message
    })
  }, [update])

  return {
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
  }
}
