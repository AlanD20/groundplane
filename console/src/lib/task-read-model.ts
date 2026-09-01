import type { TaskType } from './types.ts'

const taskTypes = new Set<TaskType>([
  'deploy',
  'rollback',
  'backup',
  'backup_prune',
  'restore',
  'attach',
  'detach',
  'run',
  'script',
  'provision',
  'create',
  'update',
  'remove',
  'start',
  'stop',
  'destroy',
  'rotate',
])

export function taskTypeFromAPI(value: string, actor: string): TaskType {
  if (!taskTypes.has(value as TaskType)) {
    throw new Error(`Controller returned unknown Task type ${value}`)
  }
  if (value === 'backup_prune' && actor !== 'system') {
    throw new Error('Controller returned backup_prune Task with non-system actor')
  }
  return value as TaskType
}
