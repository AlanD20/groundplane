import { useSyncExternalStore } from 'react'

let accepted: string[] = []
const listeners = new Set<() => void>()

// Keep only Task identities, never response bodies or operator inputs.
export function reportAcceptedTasks(response: unknown) {
  if (!response || typeof response !== 'object') return
  const ids: string[] = []
  for (const field of ['task_id', 'create_task_id', 'reconcile_task_id', 'deletion_task_id']) {
    if (field in response) {
      const value = (response as Record<string, unknown>)[field]
      if (typeof value === 'string' && /^task_[0-9A-Z]{26}$/.test(value)) ids.push(value)
    }
  }
  if (!ids.length) return
  accepted = [...new Set([...ids, ...accepted])].slice(0, 5)
  listeners.forEach(listener => listener())
}

export function dismissAcceptedTask(id: string) {
  accepted = accepted.filter(value => value !== id)
  listeners.forEach(listener => listener())
}

function subscribe(listener: () => void) {
  listeners.add(listener)
  return () => { listeners.delete(listener) }
}

export function useAcceptedTasks() {
  return useSyncExternalStore(subscribe, () => accepted)
}
