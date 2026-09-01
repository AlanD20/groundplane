'use client'

import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'
import type { TaskStep } from '@/lib/types'

export function HierarchyDeleteDialog({
  open,
  onOpenChange,
  kind,
  name,
  id,
  workspace,
  description,
  steps,
  onDispatch,
  onCommit,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  kind: 'tenant' | 'project' | 'environment'
  name: string
  id: string
  workspace: string
  description: string
  steps: TaskStep[]
  onDispatch: () => Promise<string>
  onCommit?: () => void
}) {
  return (
    <TaskRunnerDialog
      open={open}
      onOpenChange={onOpenChange}
      title={`Delete ${kind} · ${name}`}
      description={description}
      type="remove"
      target={id}
      workspace={workspace}
      destructive
      confirmText={name}
      startLabel={`Delete ${kind}`}
      steps={steps}
      onDispatch={onDispatch}
      onCommit={onCommit}
    />
  )
}
