import type { ActivityEntry } from './types.ts'

export type TaskDetailActions = {
  abort: boolean
  cancel: boolean
  retry: boolean
}

export function taskDetailActions(
  task: (Pick<ActivityEntry, 'type' | 'status'> & Partial<Pick<ActivityEntry, 'target'>>) | null,
): TaskDetailActions {
  if (task === null || task.type === 'backup_prune') {
    return { abort: false, cancel: false, retry: false }
  }
  return {
    abort: task.status === 'running',
    cancel: task.status === 'pending',
    retry: !(task.type === 'update' && task.target === 'controller') && (
      task.status === 'failed' ||
      task.status === 'timed_out' ||
      task.status === 'aborted'),
  }
}
