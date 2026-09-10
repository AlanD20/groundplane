import { useCallback, useEffect, useState } from 'react'
import { newULID } from '@/lib/utils'
import type { HostInfo } from '../platform-host/host-model'
import type { ControllerRequest, ControllerTask, ControllerUpdateAccepted } from './api'
import { ControllerUpdateIntent } from './update-intent'

const terminal = (status: string) => ['completed', 'failed', 'timed_out', 'aborted'].includes(status)

export function useControllerUpdate(
  request: ControllerRequest,
  rejected: (error: unknown) => boolean,
  host: HostInfo | null,
  refreshHost: (signal?: AbortSignal) => Promise<HostInfo>,
) {
  const [intent] = useState(() => new ControllerUpdateIntent({
    getItem: (key) => sessionStorage.getItem(key),
    setItem: (key, value) => sessionStorage.setItem(key, value),
    removeItem: (key) => sessionStorage.removeItem(key),
  }))
  const [pendingControllerUpdate, setPending] = useState(intent.current)
  const [controllerUpdateTask, setTask] = useState<ControllerTask | null>(null)
  const [trackedTaskID, setTrackedTaskID] = useState<string | null>(null)
  const [controllerUpdatePublishing, setPublishing] = useState(false)
  const [controllerUpdateError, setError] = useState<string | null>(null)
  const [controllerUpdateConnectionError, setConnectionError] = useState<string | null>(null)
  const latest = host?.controller.update.last_update
  const controllerUpdateTaskID = pendingControllerUpdate && !pendingControllerUpdate.taskId ? null
    : pendingControllerUpdate?.taskId ?? trackedTaskID ?? latest?.task_id ?? null

  const publish = useCallback(async () => {
    setPublishing(true)
    setError(null)
    try {
      const taskID = await intent.publish((release, idempotencyKey) => request<ControllerUpdateAccepted>(
        '/controller/update', 202, { method: 'POST', body: { release }, idempotencyKey },
      ), rejected)
      setTrackedTaskID(taskID)
      return taskID
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Unable to publish Controller update')
      throw error
    } finally {
      setPending(intent.current)
      setPublishing(false)
    }
  }, [intent, request, rejected])

  const updateController = useCallback(async (release: string) => {
    try {
      intent.begin(release, newULID())
      setPending(intent.current)
      setTask(null)
      setTrackedTaskID(null)
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Unable to retain Controller update request')
      throw error
    }
    return publish()
  }, [intent, publish])

  // Reads may disconnect while the native service restarts. Keep the exact ID,
  // never dispatch from this loop, and keep trying until the same Task settles.
  useEffect(() => {
    if (!controllerUpdateTaskID) return
    const controller = new AbortController()
    let timer: ReturnType<typeof setTimeout> | undefined
    async function poll() {
      try {
        const task = await request<ControllerTask>(`/tasks/${encodeURIComponent(controllerUpdateTaskID!)}`, 200, {
          signal: controller.signal,
        })
        if (controller.signal.aborted) return
        if (task.id !== controllerUpdateTaskID || task.type !== 'update' || task.target !== 'controller') {
          throw new Error('Controller returned a different update Task')
        }
        setTask(task)
        const observedHost = await refreshHost(controller.signal)
        if (controller.signal.aborted) return
        setConnectionError(null)
        if (terminal(task.status)) {
          intent.settle(task.id)
          setPending(intent.current)
          if (observedHost.controller.update.last_update) setTrackedTaskID(null)
          return
        }
      } catch (error) {
        if (controller.signal.aborted) return
        setConnectionError(error instanceof Error ? error.message : 'Controller is temporarily unavailable')
      }
      timer = setTimeout(() => void poll(), 2000)
    }
    void poll()
    return () => { controller.abort(); clearTimeout(timer) }
  }, [controllerUpdateTaskID, intent, request, refreshHost])

  const status = controllerUpdateTask?.id === controllerUpdateTaskID ? controllerUpdateTask.status : latest?.status
  const controllerUpdateActive = pendingControllerUpdate !== null || status === 'pending' || status === 'running'
  return {
    pendingControllerUpdate, controllerUpdateTask, controllerUpdateTaskID, controllerUpdatePublishing,
    controllerUpdateError, controllerUpdateConnectionError, controllerUpdateActive,
    updateController, resolveControllerUpdate: publish,
  }
}
