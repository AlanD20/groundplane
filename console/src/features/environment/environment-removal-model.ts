import type { operations } from '@/lib/api.generated'
import type { Project } from '@/lib/types'

export type TaskResponse = operations['task.show']['responses'][200]['content']['application/json']

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

export type EnvironmentRemovalDraft = {
  tenantProjects: Project[]
  backingProjects: Project[]
}

export const terminalTaskStatuses: ReadonlySet<string> = new Set(['completed', 'failed', 'timed_out', 'aborted'])

export function findEnvironment(draft: EnvironmentRemovalDraft, environmentId: string) {
  for (const project of [...draft.tenantProjects, ...draft.backingProjects]) {
    const environment = project.environments?.find((candidate) => candidate.id === environmentId)
    if (environment) return environment
  }
  return undefined
}

export function environmentDeletionTaskError(
  taskId: string,
  task: TaskResponse,
  removal: PendingResourceRemoval,
) {
  if (task.id !== taskId) return `Controller returned Task ${task.id} while observing ${taskId}`
  if (task.type !== 'remove') return `Controller returned ${task.type} Task ${taskId} for Environment deletion`
  if (task.target !== removal.resourceId) return `Controller returned deletion Task ${taskId} for a different Environment`
  if (removal.kind === 'environment' && task.project_id !== removal.projectId) {
    return `Controller returned deletion Task ${taskId} for a different Project`
  }
  const environmentId = removal.kind === 'environment' ? removal.resourceId : removal.environmentId
  if (task.environment_id && task.environment_id !== environmentId) {
    return `Controller returned deletion Task ${taskId} for a different Environment`
  }
  return undefined
}
