'use client'

import { Ban, CirclePlay, CircleStop, Trash2 } from 'lucide-react'
import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { useStore } from '@/lib/store'
import { LogViewer } from '@/features/logs/log-viewer'
import type { Environment, Service } from '@/lib/types'
import { currentServiceObservation, replicaTotal, type ServiceObservationState } from './service-observation'

export type ServiceOperation = 'start' | 'stop' | 'destroy' | 'remove'

function observationBadgeVariant(state: ServiceObservationState): 'success' | 'warning' | 'danger' | 'muted' | 'primary' {
  if (state === 'healthy') return 'success'
  if (state === 'failed') return 'danger'
  if (state === 'degraded' || state === 'starting') return 'warning'
  if (state === 'running') return 'primary'
  return 'muted'
}

export function ServiceStateBadges({
  service,
  compact = false,
  now = Date.now(),
}: {
  service: Service
  compact?: boolean
  now?: number
}) {
  const observation = currentServiceObservation(service.observation, now)
  return (
    <span className="flex flex-wrap items-center gap-1">
      <Badge variant={observationBadgeVariant(observation.state)} className={compact ? 'px-1 py-0 font-mono text-[10px]' : 'font-mono'}>
        runtime {observation.state}
      </Badge>
      {observation.state !== 'unavailable' && (
        <Badge variant="outline" className={compact ? 'px-1 py-0 font-mono text-[10px]' : 'font-mono'}>
          serving {replicaTotal(observation.replicas)}/{observation.expectedReplicas}
        </Badge>
      )}
      <Badge
        variant={service.runtimeIntent === 'running' ? 'success' : service.runtimeIntent === 'stopped' ? 'warning' : 'muted'}
        className={compact ? 'px-1 py-0 font-mono text-[10px]' : 'font-mono'}
      >
        intent {service.runtimeIntent}
      </Badge>
    </span>
  )
}

export function ServiceObservationDetails({ service, now = Date.now() }: { service: Service; now?: number }) {
  const observation = currentServiceObservation(service.observation, now)
  if (observation.state === 'unavailable') {
    return (
      <section aria-labelledby="service-observation-heading" className="rounded-lg border border-border bg-surface/50 p-3">
        <h3 id="service-observation-heading" className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
          Current serving workload
        </h3>
        <p className="mt-2 text-sm text-muted-foreground">
          Runtime evidence is unavailable or expired. Desired state and previous Tasks do not imply health.
        </p>
      </section>
    )
  }
  const counts: [string, number][] = [
    ['Running without healthcheck', observation.replicas.running],
    ['Healthy', observation.replicas.healthy],
    ['Healthcheck starting', observation.replicas.starting],
    ['Unhealthy', observation.replicas.unhealthy],
    ['Transitional', observation.replicas.transitional],
    ['Stopped', observation.replicas.stopped],
    ['Failed', observation.replicas.failed],
  ]
  return (
    <section aria-labelledby="service-observation-heading" className="rounded-lg border border-border bg-surface/50 p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 id="service-observation-heading" className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
          Current serving workload
        </h3>
        <Badge variant={observationBadgeVariant(observation.state)} className="font-mono">{observation.state}</Badge>
      </div>
      <dl className="mt-3 grid gap-x-6 gap-y-2 text-xs sm:grid-cols-2">
        <div><dt className="text-muted-foreground">Serving Release</dt><dd className="break-all font-mono">{observation.servingReleaseId}</dd></div>
        <div><dt className="text-muted-foreground">Observed replicas</dt><dd className="font-mono">{replicaTotal(observation.replicas)} / {observation.expectedReplicas} expected by serving Release</dd></div>
        <div><dt className="text-muted-foreground">Observed at</dt><dd className="font-mono">{new Date(observation.observedAt).toLocaleString()}</dd></div>
        <div><dt className="text-muted-foreground">Expires at</dt><dd className="font-mono">{new Date(observation.expiresAt).toLocaleString()}</dd></div>
      </dl>
      <dl className="mt-3 grid grid-cols-2 gap-2 border-t border-border pt-3 text-xs sm:grid-cols-4">
        {counts.map(([label, value]) => (
          <div key={label}>
            <dt className="text-muted-foreground">{label}</dt>
            <dd className="font-mono text-sm font-medium tabular-nums">{value}</dd>
          </div>
        ))}
      </dl>
      <p className="mt-3 text-xs text-muted-foreground">
        Process and healthcheck evidence only; this does not assert route or application reachability.
      </p>
    </section>
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
