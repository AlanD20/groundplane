'use client'

import { useEffect, useRef, useState } from 'react'
import { CheckCircle2, Circle, Loader2, XCircle } from 'lucide-react'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import { CopyButton } from '@/components/common/copy-button'
import { useStore } from '@/lib/store'
import type { TaskStep, TaskType } from '@/lib/types'
import { cn } from '@/lib/utils'

type Phase = 'review' | 'running' | 'done' | 'dispatched' | 'failed'

export type TaskStepState = 'pending' | 'running' | 'done' | 'failed'

export function TaskRunnerDialog({
  open,
  onOpenChange,
  title,
  description,
  type,
  target,
  workspace,
  steps,
  review,
  executionCopy = 'Controller will sequence the steps and the Agent will apply them:',
  startLabel = 'Run task',
  startDisabled = false,
  confirmText,
  destructive,
  onCommit,
  onDispatch,
  variant = 'dialog',
}: {
  open: boolean
  onOpenChange: (v: boolean) => void
  title: string
  description?: React.ReactNode
  type: TaskType
  target: string
  workspace: string
  steps: TaskStep[]
  review?: React.ReactNode
  executionCopy?: React.ReactNode
  startLabel?: string
  startDisabled?: boolean
  confirmText?: string
  destructive?: boolean
  onCommit?: () => void
  onDispatch: () => Promise<string | null>
  // "drawer" for anything that takes inputs (deploy, rollback), "dialog"
  // for plain confirmations (restore, run, start/stop/destroy).
  variant?: 'drawer' | 'dialog'
}) {
  const { abortTask, getTask, watchTaskEvents } = useStore()
  const [phase, setPhase] = useState<Phase>('review')
  const [stepStates, setStepStates] = useState<TaskStepState[]>([])
  const [typed, setTyped] = useState('')
  const [taskId, setTaskId] = useState('')
  const [taskStatus, setTaskStatus] = useState('')
  const [failure, setFailure] = useState('')
  const [statusFailure, setStatusFailure] = useState('')
	const [aborting, setAborting] = useState(false)
	const committedTaskId = useRef('')
	const onCommitRef = useRef(onCommit)

	useEffect(() => {
		onCommitRef.current = onCommit
	}, [onCommit])

  // reset when reopened
  useEffect(() => {
    if (open) {
      setPhase('review')
      setStepStates(steps.map(() => 'pending'))
      setTyped('')
      setTaskId('')
      setTaskStatus('')
      setFailure('')
      setStatusFailure('')
		setAborting(false)
		committedTaskId.current = ''
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open])

  useEffect(() => {
    if (!open || !taskId) return
    const controller = new AbortController()
    let timer: ReturnType<typeof setTimeout> | undefined
    let polling = false
    let stopEvents = () => {}
    const poll = async () => {
      if (polling || controller.signal.aborted) return
      polling = true
      try {
        const task = await getTask(taskId, controller.signal)
        if (controller.signal.aborted) return
        setTaskStatus(task.status)
        setStatusFailure('')
        setStepStates(
          steps.map((_, index) => {
            const status = task.steps?.[index]?.status
            if (status === 'completed') return 'done'
            if (status === 'failed' || status === 'timed_out' || status === 'aborted') return 'failed'
            if (status === 'running') return 'running'
            return 'pending'
          }),
        )
		if (task.status === 'completed' && committedTaskId.current !== task.id) {
			committedTaskId.current = task.id
			setPhase('done')
			onCommitRef.current?.()
		}
		if (['completed', 'failed', 'timed_out', 'aborted'].includes(task.status)) {
          stopEvents()
          return
        }
      } catch (cause: unknown) {
        if (controller.signal.aborted) return
        setStatusFailure(cause instanceof Error ? cause.message : 'Task status is temporarily unavailable')
      } finally {
        polling = false
      }
      timer = setTimeout(() => void poll(), 1000)
    }
    stopEvents = watchTaskEvents(
      taskId,
      () => void poll(),
      (message) => setStatusFailure(message),
    )
    void poll()
    return () => {
      controller.abort()
      stopEvents()
      if (timer) clearTimeout(timer)
    }
  }, [getTask, onDispatch, open, steps, taskId, watchTaskEvents])

  function start() {
    setPhase('running')
    setFailure('')
    void onDispatch().then(
      (id) => {
        if (!id) {
          setTaskStatus('not_required')
          setPhase('dispatched')
          onCommitRef.current?.()
          return
        }
        setTaskId(id)
        setPhase('dispatched')
      },
      (cause: unknown) => {
        setFailure(cause instanceof Error ? cause.message : 'The Controller rejected the request')
        setPhase('failed')
      },
    )
  }

  function abort() {
    if (!taskId || aborting) return
    setAborting(true)
    setStatusFailure('')
    void abortTask(taskId)
      .then(
        () => setTaskStatus('aborted'),
        (cause: unknown) => {
          setStatusFailure(cause instanceof Error ? cause.message : 'The Controller rejected the abort')
        },
      )
      .finally(() => setAborting(false))
  }

  const canStart = confirmText ? typed === confirmText : true
  const taskTerminalFailure = ['failed', 'timed_out', 'aborted'].includes(taskStatus)
  const taskInFlight = ['pending', 'running'].includes(taskStatus)
  const taskStatusLabel = taskStatus === 'not_required'
    ? 'No task required'
    : taskStatus
      ? `Task ${taskStatus.replaceAll('_', ' ')}`
      : 'Task dispatched'

  const Body = (
    <>
      <DialogHeader>
        <div className="flex items-center gap-2">
          <DialogTitle>{title}</DialogTitle>
          <Badge variant="outline" className="font-mono">
            {type}
          </Badge>
        </div>
        {description && <DialogDescription>{description}</DialogDescription>}
      </DialogHeader>

      <div className="flex items-center gap-2 rounded-lg border border-border bg-surface px-3 py-2 text-xs">
        <span className="text-muted-foreground">Target</span>
        <code className="font-mono text-foreground">{target}</code>
      </div>

      {phase === 'review' && (
        <div className="flex flex-col gap-3">
          {review}
          <div className="rounded-lg border border-border bg-surface/50 p-3">
            <p className="mb-2 text-xs font-medium text-muted-foreground">
              {executionCopy}
            </p>
            <ol className="flex flex-col gap-1.5">
              {steps.map((s, i) => (
                <li key={i} className="flex items-center gap-2 text-sm">
                  <span className="font-mono text-xs text-muted-foreground">{String(i + 1).padStart(2, '0')}</span>
                  <span>{s.label}</span>
                </li>
              ))}
            </ol>
          </div>
          {confirmText && (
            <div className="flex flex-col gap-1.5">
              <div className="flex items-center justify-between gap-2">
                <Label htmlFor="task-confirm">Type the required value to confirm</Label>
                <CopyButton value={confirmText} label="copy required value" />
              </div>
              <code className="select-all rounded-md border border-border bg-surface px-2.5 py-1.5 font-mono text-xs text-foreground">
                {confirmText}
              </code>
              <Input id="task-confirm" value={typed} onChange={(e) => setTyped(e.target.value)} autoFocus placeholder={confirmText} />
            </div>
          )}
        </div>
      )}

      {phase !== 'review' && (
        <div
          role={phase === 'failed' || taskTerminalFailure || statusFailure ? 'alert' : 'status'}
          className="flex items-start gap-2 rounded-lg border border-border bg-surface p-3 text-sm"
        >
          {phase === 'running' && <Loader2 className="mt-0.5 size-4 animate-spin text-info" />}
          {phase === 'dispatched' && taskInFlight && <Loader2 className="mt-0.5 size-4 animate-spin text-info" />}
          {phase === 'dispatched' && !taskInFlight && !taskTerminalFailure && <CheckCircle2 className="mt-0.5 size-4 text-success" />}
          {phase === 'dispatched' && taskTerminalFailure && <XCircle className="mt-0.5 size-4 text-destructive" />}
          {phase === 'failed' && <XCircle className="mt-0.5 size-4 text-destructive" />}
          <div className="min-w-0">
            <p className={cn('font-medium', (phase === 'failed' || taskTerminalFailure) && 'text-destructive')}>
              {phase === 'running' && 'Dispatching durable task'}
              {phase === 'dispatched' && taskStatusLabel}
              {phase === 'failed' && 'Request failed'}
            </p>
            {taskId && <code className="mt-1 block break-all text-xs text-muted-foreground">{taskId}</code>}
            {failure && <p className="mt-1 text-xs text-destructive">{failure}</p>}
            {statusFailure && (
              <p className="mt-1 text-xs text-warning">Task dispatched; status unavailable: {statusFailure}</p>
            )}
          </div>
        </div>
      )}

      {phase !== 'review' && taskId && (
        <div className="flex flex-col gap-1 rounded-lg border border-border bg-[oklch(0.14_0.01_260)] p-3 font-mono text-xs">
          {steps.map((step, index) => {
            const state = stepStates[index] ?? 'pending'
            return (
              <div key={step.label} className="flex items-center gap-2 py-0.5">
                {state === 'done' ? (
                  <CheckCircle2 className="size-3.5 text-success" />
                ) : state === 'running' ? (
                  <Loader2 className="size-3.5 animate-spin text-info" />
                ) : state === 'failed' ? (
                  <XCircle className="size-3.5 text-destructive" />
                ) : (
                  <Circle className="size-3.5 text-muted-foreground/50" />
                )}
                <span
                  className={cn(
                    state === 'done' && 'text-foreground',
                    state === 'running' && 'text-info',
                    state === 'pending' && 'text-muted-foreground/60',
                  )}
                >
                  {step.label}
                </span>
                {state === 'done' && <span className="ml-auto text-[10px] text-success">acked</span>}
              </div>
            )
          })}
        </div>
      )}

      <DialogFooter>
        {phase === 'review' && (
          <>
            <Button variant="outline" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
            <Button variant={destructive ? 'destructive' : 'default'} disabled={!canStart || startDisabled} onClick={start}>
              {startLabel}
            </Button>
          </>
        )}
        {phase === 'running' && (
          <Button disabled>
            <Loader2 className="size-4 animate-spin" />
            Dispatching…
          </Button>
        )}
        {phase === 'done' && (
          <div className="flex w-full items-center justify-between">
            <span className="flex items-center gap-1.5 text-sm text-success">
              <CheckCircle2 className="size-4" />
              Task completed
            </span>
            <Button onClick={() => onOpenChange(false)}>Done</Button>
          </div>
        )}
        {phase === 'dispatched' && (
          <div className="flex w-full items-center justify-between">
            <span className={cn('flex items-center gap-1.5 text-sm', taskTerminalFailure ? 'text-destructive' : taskInFlight ? 'text-info' : 'text-success')}>
              {taskInFlight ? <Loader2 className="size-4 animate-spin" /> : taskTerminalFailure ? <XCircle className="size-4" /> : <CheckCircle2 className="size-4" />}
              {taskStatusLabel}
            </span>
            <div className="flex gap-2">
              {taskInFlight && (
                <Button variant="destructive" disabled={aborting} onClick={abort}>
                  {aborting && <Loader2 className="size-4 animate-spin" />}
                  Abort task
                </Button>
              )}
              <Button onClick={() => onOpenChange(false)}>{taskStatus === 'completed' ? 'Done' : 'Close'}</Button>
            </div>
          </div>
        )}
        {phase === 'failed' && (
          <div className="flex w-full justify-end gap-2">
            <Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
            <Button onClick={() => setPhase('review')}>Back to review</Button>
          </div>
        )}
      </DialogFooter>
    </>
  )

  if (variant === 'drawer') {
    return (
      <Drawer open={open} onOpenChange={onOpenChange}>
        <DrawerContent>{Body}</DrawerContent>
      </Drawer>
    )
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg">{Body}</DialogContent>
    </Dialog>
  )
}
