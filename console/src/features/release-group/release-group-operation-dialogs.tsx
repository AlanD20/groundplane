'use client'

import { useEffect, useMemo, useState } from 'react'
import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { useRequiredParams } from '@/lib/router'
import { useStore } from '@/lib/store'
import type { Environment, ReleaseGroup, TaskStep } from '@/lib/types'
import { releaseGroupPolicyLabel, resolveReleaseGroupRollbackTargets } from './release-group-projections'

type OperationDialogProps = {
  env: Environment
  group: ReleaseGroup
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function DeployReleaseGroupDialog({ env, group, open, onOpenChange }: OperationDialogProps) {
  const store = useStore()
  const params = useRequiredParams('tenant')
  const [tag, setTag] = useState(group.tag ?? '')

  useEffect(() => {
    if (open) setTag(group.tag ?? '')
  }, [group.tag, open])

  const steps = useMemo<TaskStep[]>(() => [
    { label: 'Acquire the environment release-group task lock', state: 'pending' },
    ...group.order.map((service) => ({ label: `Deploy ${service} · ${tag || '<tag>'}`, state: 'pending' as const })),
    { label: 'Record member results and release the task lock', state: 'pending' },
  ], [group.order, tag])

  return (
    <TaskRunnerDialog
      open={open}
      onOpenChange={onOpenChange}
      variant="drawer"
      title={`Deploy ${group.name} · ${env.name}`}
      description="Deploy one immutable tag to every member in the stored order under one task lock. Each member retains its own release record."
      type="deploy"
      target={group.id}
      workspace={params.tenant}
      startLabel="Deploy group"
      startDisabled={!tag.trim()}
      review={
        <div className="flex flex-col gap-3">
          <div className="flex flex-col gap-1">
            <Label htmlFor="release-group-tag">Image tag</Label>
            <Input id="release-group-tag" value={tag} onChange={(event) => setTag(event.target.value)} placeholder="sha-…" />
          </div>
          <div className="rounded-lg border border-border bg-surface p-3 text-xs text-muted-foreground">
            <span className="font-medium text-foreground">Failure behavior:</span> {releaseGroupPolicyLabel(group.onFailure)}
          </div>
        </div>
      }
      steps={steps}
      onDispatch={() => store.deployReleaseGroup(env.id, group.id, tag.trim())}
      onCommit={() => { void store.refreshEnvironmentReleases(env.id).catch(() => undefined) }}
    />
  )
}

export function RollbackReleaseGroupDialog({ env, group, open, onOpenChange }: OperationDialogProps) {
  const store = useStore()
  const params = useRequiredParams('tenant')
  const [targets, setTargets] = useState(() => resolveReleaseGroupRollbackTargets(env, group))

  useEffect(() => {
    if (open) setTargets(resolveReleaseGroupRollbackTargets(env, group))
    // Freeze the reviewed member targets for the full task run. Store updates
    // at completion must not rewrite the approved or completed step labels.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, group.id])

  const missing = targets.filter((target) => !target.tag).map((target) => target.service)

  return (
    <TaskRunnerDialog
      open={open}
      onOpenChange={onOpenChange}
      title={`Roll back ${group.name} · ${env.name}`}
      description="Resolve each member's previous successful release, then roll back in the stored order under one task lock. Migrations are never reversed."
      type="rollback"
      target={group.id}
      workspace={params.tenant}
      startLabel="Roll back group"
      startDisabled={missing.length > 0}
      review={
        <div className="flex flex-col gap-2 rounded-lg border border-border bg-surface p-3 text-xs text-muted-foreground">
          <p><span className="font-medium text-foreground">Failure behavior:</span> {releaseGroupPolicyLabel(group.onFailure)}</p>
          {targets.map((target) => (
            <div key={target.service} className="flex items-center justify-between gap-3 font-mono">
              <span>{target.service}</span>
              <span className={target.tag ? 'text-foreground' : 'text-destructive'}>{target.tag ?? 'no prior successful release'}</span>
            </div>
          ))}
          {missing.length > 0 && <p className="text-destructive">Rollback is unavailable until every member has a prior successful release.</p>}
        </div>
      }
      steps={[
        { label: 'Acquire the environment release-group task lock', state: 'pending' },
        ...targets.map((target) => ({ label: `Roll back ${target.service} · ${target.tag ?? 'unavailable'}`, state: 'pending' as const })),
        { label: 'Record member results and release the task lock', state: 'pending' },
      ]}
      onDispatch={() => store.rollbackReleaseGroup(env.id, group.id)}
      onCommit={() => { void store.refreshEnvironmentReleases(env.id).catch(() => undefined) }}
    />
  )
}
