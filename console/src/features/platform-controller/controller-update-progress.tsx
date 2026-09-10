import { useState } from 'react'
import { Search } from 'lucide-react'
import { TaskDetailDrawer } from '@/components/common/task-detail-drawer'
import { StatusBadge } from '@/components/common/status-badge'
import { Button } from '@/components/ui/button'
import { useStore } from '@/lib/store'
import type { ActivityEntry } from '@/lib/types'

export function ControllerUpdateProgress() {
  const {
    host, controllerUpdateTaskID: taskID, controllerUpdateTask: task,
    controllerUpdateConnectionError, getTaskJournalDetail,
  } = useStore()
  const [inspected, setInspected] = useState<ActivityEntry | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  if (!taskID) return null
  const summary = host?.controller.update.last_update
  const status = task?.id === taskID ? task.status : summary?.task_id === taskID ? summary.status : 'pending'
  const phase = summary?.task_id === taskID ? summary.phase : ''

  async function inspect() {
    if (!taskID) return
    setLoading(true)
    setError(null)
    try { setInspected(await getTaskJournalDetail(taskID)) }
    catch (cause) { setError(cause instanceof Error ? cause.message : 'Unable to inspect update Task') }
    finally { setLoading(false) }
  }

  return (
    <section aria-label="Controller update Task" className="flex flex-col gap-3 rounded-lg border border-border p-3">
      <div className="flex flex-wrap items-center justify-between gap-2" aria-live="polite">
        <h3 className="text-sm font-medium">Latest update</h3>
        <StatusBadge status={status} label={status === 'aborted' ? 'Aborted' : undefined} />
      </div>
      <code className="break-all text-xs text-muted-foreground">{taskID}</code>
      {phase ? <p className="text-xs">Recovery phase: <span className="font-mono">{phase}</span></p> : null}
      {['failed', 'timed_out', 'aborted'].includes(status) ? (
        <p className="text-xs text-muted-foreground">
          This update did not complete. Restoring the previous runtime does not count as update success.
          Inspect the Task before starting a new explicit update.
        </p>
      ) : null}
      {status === 'pending' || status === 'running' ? (
        <p className="text-xs text-muted-foreground">
          Following this Task through the Controller restart. Abort is available only before activation.
        </p>
      ) : null}
      {controllerUpdateConnectionError ? (
        <p role="status" className="text-xs text-warning">
          Reconnecting to the same Task. Last read: {controllerUpdateConnectionError}
        </p>
      ) : null}
      {error ? <p role="alert" className="text-xs text-destructive">{error}</p> : null}
      <Button size="sm" variant="outline" className="self-start" disabled={loading} onClick={() => void inspect()}>
        <Search className="size-4" /> {loading ? 'Loading Task…' : 'Inspect Task'}
      </Button>
      {inspected ? (
        <TaskDetailDrawer entry={inspected} scope={{ kind: 'workspace', workspace: 'platform' }} surface="activity"
          onOpenChange={(open) => { if (!open) setInspected(null) }} />
      ) : null}
    </section>
  )
}
