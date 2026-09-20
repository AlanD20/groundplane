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
import { Input } from '@/components/ui/input'
import { Select } from '@/components/ui/select'
import { ServiceStateBadges } from '@/features/service/service-runtime-actions'
import { serviceObservationState } from '@/features/service/service-observation'
import { cn } from '@/lib/utils'
import { LogViewer } from '@/features/logs/log-viewer'
import type { ZoneSelection } from './zone-map'
import { ServiceDetailsDrawer } from './service-details-drawer'

export function ServicesList({ env, now = Date.now() }: { env: Environment; now?: number }) {
  const store = useStore()
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [query, setQuery] = useState('')
  const [status, setStatus] = useState('all')
  const [ascending, setAscending] = useState(true)
  const selected = env.services.find((service) => service.id === selectedId)
  const visible = env.services.filter(service =>
    `${service.name} ${service.image} ${service.zones.join(' ')}`.toLowerCase().includes(query.trim().toLowerCase()) &&
    (status === 'all' || serviceObservationState(service.observation, now) === status),
  ).sort((a, b) => (ascending ? 1 : -1) * a.name.localeCompare(b.name))

  return (
    <>
      <div className="flex flex-wrap items-center gap-3">
        <Input type="search" aria-label="Filter services" placeholder="Filter by name, image or zone…" value={query} onChange={event => setQuery(event.target.value)} className="min-w-48 flex-1" />
        <Select aria-label="Runtime status" className="w-auto min-w-44" value={status} onValueChange={setStatus} options={['all', 'healthy', 'running', 'degraded', 'starting', 'stopped', 'failed', 'absent', 'unavailable'].map(value => ({ value, label: value === 'all' ? 'All runtime states' : value }))} />
        <Button variant="outline" onClick={() => setAscending(!ascending)} aria-label={`Name sorted ${ascending ? 'ascending' : 'descending'}, reverse sort`}>Name {ascending ? '↑' : '↓'}</Button>
        <span className="text-xs text-muted-foreground" role="status">{visible.length} of {env.services.length}</span>
      </div>
      <ul aria-label="Environment services" className="divide-y divide-border overflow-hidden rounded-lg border border-border bg-card">
        {visible.map((service) => {
          const attached = env.attaches.filter((attach) => attach.service === service.name)
          return (
            <li key={service.id} className="min-w-0">
              <Button variant="ghost" size="content"
                type="button"
                aria-label={`View ${service.name} details`}
                aria-haspopup="dialog"
                onClick={() => setSelectedId(service.id)}
                className="flex w-full items-start gap-3 px-4 py-3 text-left transition-colors hover:bg-muted/50 focus-visible:bg-muted/50 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-ring"
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
              </Button>
              {attached.length > 0 && (
                <div className="flex flex-wrap gap-1.5 px-4 pb-3" aria-label={`${service.name} backing service connections`}>
                  {attached.map((attach) => {
                    const project = store.getBackingProject(attach.projectId)
                    return (
                      <Link
                        key={attach.id}
                        to={`/platform/backing-services/${attach.projectId}`}
                        className="inline-flex max-w-full items-center gap-1 rounded-full bg-primary/10 px-2 py-0.5 font-mono text-[11px] text-primary hover:bg-primary/20 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
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
              <div className="flex justify-end px-4 pb-3"><LogViewer target={{ kind: 'service', id: service.id }} label="Service logs" /></div>
            </li>
          )
        })}
      </ul>
      {visible.length === 0 && <p className="py-6 text-center text-sm text-muted-foreground">No services match these filters.</p>}
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

export function ServiceCard({ service, env, selection }: { service: Service; env: Environment; selection: ZoneSelection }) {
  const store = useStore()
  const attached = env.attaches.filter(attach => attach.service === service.name)
  const [open, setOpen] = useState(false)
  const selected = selection.selectedId === service.id
  return <>
    <article className={cn('overflow-hidden rounded-xl border bg-background transition-[opacity,border-color] duration-150', selected ? 'border-primary' : 'border-border', selection.selectedId && !selected && 'opacity-50')}>
      <Button variant="ghost" aria-pressed={selected} onClick={() => selection.onSelect(service.id)} className="h-auto w-full flex-col items-stretch gap-3 whitespace-normal rounded-none p-4 text-left">
        <span className="flex items-center justify-between gap-2"><strong className="text-sm">{service.name}</strong>{service.zones.length > 1 && <Badge variant="outline">{service.zones.length} zones</Badge>}</span>
        <ServiceStateBadges service={service} compact />
        <span className="break-all font-mono text-[11px] text-muted-foreground">{service.image}</span>
        {service.role && <span className="text-xs text-muted-foreground">{service.role}</span>}
        <span className="text-[11px] text-muted-foreground">{service.resources.mem}, {service.resources.cpus} CPU</span>
      </Button>
      {attached.length > 0 && <div className="flex flex-wrap gap-1 border-t border-dashed border-border px-3 py-2">{attached.map(attach => {
        const backing = store.getBackingProject(attach.projectId)
        return <Link key={attach.id} to={`/platform/backing-services/${attach.projectId}`} className="inline-flex items-center gap-1 rounded-md bg-accent px-2 py-1 text-[10px] text-primary">
          <Plug className="size-3" />{attach.name}, {backing?.name ?? attach.projectId}
        </Link>
      })}</div>}
      <div className="flex justify-end gap-1 border-t border-border p-2"><LogViewer target={{ kind: 'service', id: service.id }} /><Button size="xs" variant="ghost" onClick={() => setOpen(true)}>Details</Button></div>
    </article>
    <ServiceDetailsDrawer env={env} service={service} open={open} onOpenChange={setOpen} />
  </>
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
