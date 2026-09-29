'use client'

import { useEffect, useState } from 'react'
import { RefreshCw, X } from 'lucide-react'
import { useStore } from '@/lib/store'
import { ActivityIcon } from '@/components/common/activity-icon'
import { StatusBadge } from '@/components/common/status-badge'
import { TaskJournalMetadata } from '@/components/common/task-journal-metadata'
import { Button } from '@/components/ui/button'
import { DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import { taskDetailActions } from '@/lib/task-detail-actions'
import { requestTask } from '@/features/task/api'
import { taskFromAPI } from '@/features/task/journal-model'
import { ImageFetchTaskDetails } from '@/features/image-delivery/image-fetch-task-details'
import type { ActivityEntry, TaskJournalScope, TaskJournalSurface } from '@/lib/types'

export function TaskDetailDrawer({
  entry,
  taskId,
  scope,
  surface,
  onOpenChange,
}: {
  scope: TaskJournalScope
  surface: TaskJournalSurface
  onOpenChange: (open: boolean) => void
} & ({ entry: ActivityEntry; taskId?: never } | { taskId: string; entry?: never })) {
  const store = useStore()
  const [detail, setDetail] = useState<ActivityEntry | null>(null)
  const [detailLoading, setDetailLoading] = useState(true)
  const [detailError, setDetailError] = useState<string | null>(null)
  const [actionError, setActionError] = useState<string | null>(null)
  const [actionPending, setActionPending] = useState(false)
  const [reload, setReload] = useState(0)
  const id = taskId ?? entry!.id
  const task = detail ?? entry

  useEffect(() => {
    const controller = new AbortController()
    setActionError(null)
    setActionPending(false)
    setDetailLoading(true)
    setDetailError(null)
    setDetail(null)
    let timer: ReturnType<typeof setTimeout> | undefined
    const poll = async () => {
      let terminal = false
      try {
        const loaded = taskFromAPI(await requestTask(id, controller.signal))
        if (controller.signal.aborted) return
        setDetail(loaded)
        setDetailError(null)
        terminal = ['completed', 'failed', 'timed_out', 'aborted'].includes(loaded.status)
      } catch (error) {
        if (controller.signal.aborted) return
        setDetailError(error instanceof Error ? error.message : 'Unable to load Task details')
      } finally {
        if (!controller.signal.aborted) setDetailLoading(false)
      }
      if (!terminal && !controller.signal.aborted) timer = setTimeout(() => void poll(), 1000)
    }
    void poll()
    return () => { controller.abort(); if (timer) clearTimeout(timer) }
  }, [id, reload])

  const actions = taskDetailActions(detail)

  async function runAction(action: 'abort' | 'retry') {
    setActionError(null)
    setActionPending(true)
    try {
      if (action === 'abort') { await store.abortTask(id); setReload(value => value + 1) }
      else { await store.retryTask(id); onOpenChange(false) }
      setActionPending(false)
      void store.loadTaskJournal(surface, scope).catch(() => undefined)
    } catch (error) {
      setActionError(error instanceof Error ? error.message : `Unable to ${action} Task`)
      setActionPending(false)
    }
  }

  return (
    <Drawer open onOpenChange={onOpenChange}>
      <DrawerContent>
        <DialogHeader>
          <DialogTitle className="flex min-w-0 items-start gap-2 break-all">
            {task && <ActivityIcon type={task.type} status={task.status} />}
            {task?.title ?? `Task ${id}`}
          </DialogTitle>
        </DialogHeader>
        <div className="flex flex-col gap-4">
          {detailLoading && (
            <p role="status" className="rounded-lg border border-border bg-surface px-3 py-2 text-xs text-muted-foreground">
              Loading authoritative Task details…
            </p>
          )}
          {detailError && (
            <div role="alert" className="flex items-center justify-between gap-3 rounded-lg border border-destructive/40 bg-destructive/5 px-3 py-2 text-xs text-destructive">
              <span>{detailError}</span>
              <Button variant="outline" size="sm" onClick={() => setReload((value) => value + 1)}>Retry</Button>
            </div>
          )}
          {detail && task && (
            <>
              {task.imageFetch ? <><StatusBadge status={task.status} /><ImageFetchTaskDetails task={task} /></> : <>
              <div className="flex flex-wrap gap-1.5">
                <span className="rounded-full border border-border bg-surface px-2.5 py-1 font-mono text-[11px] text-muted-foreground">
                  {task.id}
                </span>
                <span className="rounded-full border border-border bg-surface px-2.5 py-1 font-mono text-[11px] text-muted-foreground">
                  {task.type}
                </span>
                <StatusBadge status={task.status} />
              </div>
              <div className="flex flex-col gap-1.5 text-sm">
                <TaskDetailRow label="Target" value={task.target} />
                {task.operationId && <TaskDetailRow label="Operation" value={task.operationId} />}
                {task.retryOf && <TaskDetailRow label="Retry of" value={task.retryOf} />}
              </div>
              <TaskJournalMetadata entry={task} />
              {task.note && (
                <p className="rounded-lg border border-border bg-surface px-3 py-2 text-xs text-muted-foreground">
                  {task.note}
                </p>
              )}
              <div className="flex flex-col gap-1">
                <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground/70">
                  Procedure · {task.steps?.length ?? 0} steps
                </span>
                {task.steps && task.steps.length > 0 ? (
                  <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface px-3 py-2">
                    {task.steps.map((step, index) => (
                      <div key={`${step.label}:${index}`} className="flex items-center gap-2 text-[11px]">
                        <span className={
                          step.state === 'done'
                            ? 'size-1.5 shrink-0 rounded-full bg-success'
                            : step.state === 'running'
                              ? 'size-1.5 shrink-0 animate-pulse rounded-full bg-primary'
                              : step.state === 'failed'
                                ? 'size-1.5 shrink-0 rounded-full bg-destructive'
                                : 'size-1.5 shrink-0 rounded-full bg-muted-foreground/30'
                        } />
                        <span className="font-mono text-muted-foreground">{step.label}</span>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="text-xs text-muted-foreground">no steps recorded</p>
                )}
              </div>
              </>}
            </>
          )}
          {actionError && (
            <p role="alert" className="rounded-lg border border-destructive/40 bg-destructive/5 px-3 py-2 text-xs text-destructive">
              {actionError}
            </p>
          )}
          {actionPending && <p role="status" className="text-xs text-muted-foreground">Submitting Task action…</p>}
          <div className="flex flex-wrap justify-end gap-2 border-t border-border pt-4">
            {actions.abort && (
              <Button variant="destructive" size="sm" disabled={actionPending} onClick={() => void runAction('abort')}>
                <X className="size-4" /> Abort
              </Button>
            )}
            {actions.cancel && (
              <Button variant="destructive" size="sm" disabled={actionPending} onClick={() => void runAction('abort')}>
                <X className="size-4" /> Cancel
              </Button>
            )}
            {actions.retry && (
              <Button size="sm" disabled={actionPending} onClick={() => void runAction('retry')}>
                <RefreshCw className="size-4" /> Retry
              </Button>
            )}
            <Button variant="outline" size="sm" disabled={actionPending} onClick={() => onOpenChange(false)}>Close</Button>
          </div>
        </div>
      </DrawerContent>
    </Drawer>
  )
}

function TaskDetailRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between gap-3">
      <span className="text-muted-foreground">{label}</span>
      <span className="min-w-0 truncate font-mono text-xs" title={value}>{value}</span>
    </div>
  )
}
