'use client'

import { useState } from 'react'
import { Link } from 'react-router-dom'
import { Boxes, ChevronRight, Plug, RefreshCw } from 'lucide-react'
import { useStore } from '@/lib/store'
import type { Environment, Service } from '@/lib/types'
import { StatusDot } from '@/components/common/status-badge'
import { EmptyState } from '@/components/common/empty-state'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { ServiceStateBadges } from '@/features/service/service-runtime-actions'
import { serviceObservationState } from '@/features/service/service-observation'
import { cn } from '@/lib/utils'
import { ServiceDetailsDrawer } from './service-details-drawer'

export function ServicesList({ env, now = Date.now() }: { env: Environment; now?: number }) {
  const store = useStore()
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const selected = env.services.find((service) => service.id === selectedId)

  return (
    <>
      <ul aria-label="Environment services" className="divide-y divide-border overflow-hidden rounded-lg border border-border bg-card">
        {env.services.map((service) => {
          const attached = env.attaches.filter((attach) => attach.service === service.name)
          return (
            <li key={service.id} className="min-w-0">
              <button
                type="button"
                aria-label={`View ${service.name} details`}
                aria-haspopup="dialog"
                onClick={() => setSelectedId(service.id)}
                className="flex w-full items-start gap-3 px-4 py-3 text-left transition-colors hover:bg-muted/50 focus-visible:bg-muted/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
              >
                <StatusDot status={serviceObservationState(service.observation, now)} className="mt-1.5 shrink-0" />
                <span className="grid min-w-0 flex-1 gap-3 md:grid-cols-3 md:items-start">
                  <span className="flex min-w-0 flex-col gap-1">
                    <span className="break-all font-mono text-sm font-medium">{service.name}</span>
                    <ServiceStateBadges service={service} compact now={now} />
                    {service.role && <span className="break-words text-xs text-muted-foreground">{service.role}</span>}
                  </span>
                  <span className="flex min-w-0 flex-col gap-1 text-xs">
                    <span className="break-all font-mono text-muted-foreground">{service.image}</span>
                    <span className="break-words text-muted-foreground">Zones: {service.zones.join(', ') || 'none'}</span>
                  </span>
                  <span className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground">
                    <span>Desired: {service.replicas} {service.replicas === 1 ? 'replica' : 'replicas'} · {service.strategy}</span>
                    <span>{service.resources.mem} · {service.resources.cpus} cpu</span>
                    {service.healthcheck && (
                      <span className="break-all font-mono">
                        {service.healthcheck.kind === 'http' ? 'HTTP' : service.healthcheck.kind === 'tcp' ? 'TCP' : 'pgrep'} {service.healthcheck.target}
                      </span>
                    )}
                  </span>
                </span>
                <ChevronRight aria-hidden="true" className="mt-1 size-4 shrink-0 text-muted-foreground" />
              </button>
              {attached.length > 0 && (
                <div className="flex flex-wrap gap-1.5 px-4 pb-3" aria-label={`${service.name} backing service connections`}>
                  {attached.map((attach) => {
                    const project = store.getBackingProject(attach.projectId)
                    return (
                      <Link
                        key={attach.id}
                        to={`/platform/backing-services/${attach.projectId}`}
                        className="inline-flex max-w-full items-center gap-1 rounded-full bg-primary/10 px-2 py-0.5 font-mono text-[11px] text-primary hover:bg-primary/20 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                      >
                        <Plug aria-hidden="true" className="size-3 shrink-0" />
                        <span className="break-all">
                          {project?.environments?.[0]?.services[0]?.serviceName ?? attach.projectId}
                          {attach.database !== '—' ? ` · ${attach.database}` : ''}
                        </span>
                      </Link>
                    )
                  })}
                </div>
              )}
            </li>
          )
        })}
      </ul>
      {selected && (
        <ServiceDetailsDrawer
          key={selected.id}
          env={env}
          service={selected}
          now={now}
          open
          onOpenChange={(open) => { if (!open) setSelectedId(null) }}
        />
      )}
    </>
  )
}

export function ServiceCard({ service, env }: { service: Service; env: Environment }) {
  const store = useStore()
  const attached = env.attaches.filter((attach) => attach.service === service.name)
  const [open, setOpen] = useState(false)
  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        title={`Edit ${service.name}`}
        className="flex items-start gap-2 rounded-lg border border-border bg-surface px-2 py-1.5 text-left transition-colors hover:border-ring"
      >
        <StatusDot status={serviceObservationState(service.observation)} className="mt-1.5" />
        <div className="flex min-w-0 flex-col">
          <span className="font-mono text-xs font-medium">{service.name}</span>
          <ServiceStateBadges service={service} compact />
          <span className="truncate text-[11px] text-muted-foreground">{service.role}</span>
          <span className="truncate font-mono text-[10px] text-muted-foreground/60">{service.image}</span>
          <span className="truncate text-[10px] text-muted-foreground/70">zones: {service.zones.join(', ') || 'none'}</span>
          <div className="mt-1 flex flex-wrap gap-1">
            <span className="rounded-full bg-primary/10 px-1.5 py-0.5 font-mono text-[10px] text-primary">
              {service.resources.mem} · {service.resources.cpus} cpu
            </span>
            {service.healthcheck && (
              <span className="rounded-full bg-secondary px-1.5 py-0.5 font-mono text-[10px] text-secondary-foreground">
                {service.healthcheck.kind === 'http'
                  ? `hc ${service.healthcheck.target}`
                  : service.healthcheck.kind === 'tcp'
                    ? `tcp ${service.healthcheck.target}`
                    : `pgrep ${service.healthcheck.target}`}
              </span>
            )}
            {service.strategy !== 'recreate' && (
              <span className="rounded-full bg-warning/10 px-1.5 py-0.5 font-mono text-[10px] text-warning">{service.strategy}</span>
            )}
            {service.replicas > 1 && (
              <span className="rounded-full bg-secondary px-1.5 py-0.5 font-mono text-[10px] text-secondary-foreground">×{service.replicas}</span>
            )}
            {attached.map((attach) => {
              const backing = store.getBackingProject(attach.projectId)
              return (
                <Link
                  key={attach.id}
                  to={`/platform/backing-services/${attach.projectId}`}
                  className="flex items-center gap-1 rounded-full bg-primary/10 px-1.5 py-0.5 font-mono text-[10px] text-primary transition-colors hover:bg-primary/20"
                >
                  <Plug className="size-2.5" />
                  {backing?.environments?.[0]?.services[0]?.serviceName ?? attach.projectId}
                  {attach.database !== '—' ? ` · ${attach.database}` : ''}
                </Link>
              )
            })}
          </div>
        </div>
      </button>
      <ServiceDetailsDrawer env={env} service={service} open={open} onOpenChange={setOpen} />
    </>
  )
}

export function ServicesPanel({
  env,
  now,
  refreshing,
  onRefresh,
  createAction,
}: {
  env: Environment
  now: number
  refreshing: boolean
  onRefresh: () => void
  createAction: React.ReactNode
}) {
  const count = env.services.length
  return (
    <section aria-labelledby="environment-services-heading" className="flex flex-col gap-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 id="environment-services-heading" className="flex items-center gap-2 text-sm font-semibold">
            <Boxes className="size-4 text-muted-foreground" /> Services
          </h2>
          <p className="mt-1 text-xs text-muted-foreground">
            Inspect desired configuration separately from current serving-workload evidence.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" disabled={refreshing} onClick={onRefresh}>
            <RefreshCw className={cn('size-3.5', refreshing && 'animate-spin')} />
            {refreshing ? 'Refreshing runtime' : 'Refresh runtime'}
          </Button>
          <Badge variant="outline" className="font-mono">{count} {count === 1 ? 'service' : 'services'}</Badge>
          {createAction}
        </div>
      </div>
      {count === 0 ? (
        <EmptyState
          icon={<Boxes />}
          title="No services yet"
          description="Add a service to start building this environment's workload."
          action={createAction}
        />
      ) : (
        <ServicesList env={env} now={now} />
      )}
    </section>
  )
}
