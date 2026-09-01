'use client'

import { Ban, CirclePlay, CircleStop, Trash2 } from 'lucide-react'
import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { useStore } from '@/lib/store'
import { LogViewer } from '@/features/logs/log-viewer'
import type { Environment, Service } from '@/lib/types'

export type ServiceOperation = 'start' | 'stop' | 'destroy' | 'remove'

export function ServiceStateBadges({ service, compact = false }: { service: Service; compact?: boolean }) {
  return (
    <span className="flex flex-wrap items-center gap-1">
      <Badge variant="outline" className={compact ? 'px-1 py-0 font-mono text-[10px]' : 'font-mono'}>
        observed {service.status}
      </Badge>
      <Badge
        variant={service.runtimeIntent === 'running' ? 'success' : service.runtimeIntent === 'stopped' ? 'warning' : 'muted'}
        className={compact ? 'px-1 py-0 font-mono text-[10px]' : 'font-mono'}
      >
        intent {service.runtimeIntent}
      </Badge>
    </span>
  )
}

export function ServiceRuntimeActions({
  service,
  onAction,
}: {
  service: Service
  onAction: (action: ServiceOperation) => void
}) {
  return (
    <div className="rounded-xl border border-border bg-surface/50 p-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-col gap-1">
          <p className="text-xs font-medium">Runtime intent</p>
          <ServiceStateBadges service={service} />
        </div>
        <div className="flex flex-wrap gap-2">
          <LogViewer target={{ kind: 'service', id: service.id }} label="Service logs" />
          <Button size="sm" onClick={() => onAction('start')} disabled={service.runtimeIntent === 'running'}>
            <CirclePlay className="size-3.5" /> Start
          </Button>
          <Button size="sm" variant="outline" onClick={() => onAction('stop')} disabled={service.runtimeIntent === 'stopped'}>
            <CircleStop className="size-3.5" /> Stop
          </Button>
          <Button size="sm" variant="destructive" onClick={() => onAction('destroy')} disabled={service.runtimeIntent === 'absent'}>
            <Ban className="size-3.5" /> Destroy runtime
          </Button>
        </div>
      </div>
      <p className="mt-2 text-xs text-muted-foreground">
        Start, Stop, and Destroy preserve desired state. Reconciliation follows this Controller-owned intent.
      </p>
    </div>
  )
}

export function ServiceOperationDialog({
  env,
  service,
  operation,
  workspace,
  onOpenChange,
  onRemoved,
}: {
  env: Environment
  service: Service
  operation: ServiceOperation | null
  workspace: string
  onOpenChange: (open: boolean) => void
  onRemoved: () => void
}) {
  const store = useStore()
  if (!operation) return null

  const runtimeIntent = operation === 'start' ? 'running' : operation === 'stop' ? 'stopped' : 'absent'
  const removing = operation === 'remove'
  const title = removing ? `Remove desired service · ${service.name}` : `${operation[0].toUpperCase()}${operation.slice(1)} ${service.name}`
  const description = removing
    ? 'Delete the desired service and its Controller-owned runtime intent. This is not the same as destroying its runtime.'
    : operation === 'destroy'
      ? 'Remove the service runtime while retaining its desired service record. A later Start recreates it.'
      : `${operation === 'start' ? 'Run' : 'Stop'} the service through durable Controller-owned runtime intent.`
  const steps = removing
    ? [
        { label: 'Validate desired-state references', state: 'pending' as const },
        { label: 'Remove desired service and runtime-intent record', state: 'pending' as const },
        { label: 'Reconcile the environment without the service', state: 'pending' as const },
      ]
    : [
        { label: `Persist runtime intent ${runtimeIntent}`, state: 'pending' as const },
        { label: operation === 'start' ? 'Reconcile the service runtime' : operation === 'stop' ? 'Stop service containers' : 'Remove service containers', state: 'pending' as const },
        { label: 'Record observed runtime state', state: 'pending' as const },
      ]

  return (
    <TaskRunnerDialog
      open
      onOpenChange={onOpenChange}
      title={title}
      description={description}
      type={operation}
      target={service.id}
      workspace={workspace}
      destructive={operation === 'destroy' || removing}
      confirmText={operation === 'destroy' || removing ? service.name : undefined}
      startLabel={removing ? 'Remove desired service' : operation === 'destroy' ? 'Destroy runtime' : `${operation[0].toUpperCase()}${operation.slice(1)} service`}
      steps={steps}
      {...(removing
		? {
			onDispatch: () => store.deleteService(env.id, service.id),
			onCommit: onRemoved,
		  }
        : {
            onDispatch: () => store.runServiceRuntimeAction(env.id, service.id, operation),
          })}
    />
  )
}

export function RemoveDesiredServiceButton({ onClick }: { onClick: () => void }) {
  return (
    <Button variant="destructive" onClick={onClick}>
      <Trash2 className="size-4" /> Remove desired service
    </Button>
  )
}
