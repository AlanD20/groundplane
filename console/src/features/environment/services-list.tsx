'use client'

import { useState } from 'react'
import { Link } from 'react-router-dom'
import { ChevronRight, Plug } from 'lucide-react'
import { useStore } from '@/lib/store'
import type { Environment, Service } from '@/lib/types'
import { StatusDot } from '@/components/common/status-badge'
import { ServiceStateBadges } from '@/features/service/service-runtime-actions'
import { ServiceDetailsDrawer } from './service-details-drawer'

export function ServicesList({ env }: { env: Environment }) {
  const store = useStore()
  const [selected, setSelected] = useState<Service | null>(null)

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
                onClick={() => setSelected(service)}
                className="flex w-full items-start gap-3 px-4 py-3 text-left transition-colors hover:bg-muted/50 focus-visible:bg-muted/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
              >
                <StatusDot status={service.status} className="mt-1.5 shrink-0" />
                <span className="grid min-w-0 flex-1 gap-3 md:grid-cols-3 md:items-start">
                  <span className="flex min-w-0 flex-col gap-1">
                    <span className="break-all font-mono text-sm font-medium">{service.name}</span>
                    <ServiceStateBadges service={service} compact />
                    {service.role && <span className="break-words text-xs text-muted-foreground">{service.role}</span>}
                  </span>
                  <span className="flex min-w-0 flex-col gap-1 text-xs">
                    <span className="break-all font-mono text-muted-foreground">{service.image}</span>
                    <span className="break-words text-muted-foreground">Zones: {service.zones.join(', ') || 'none'}</span>
                  </span>
                  <span className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground">
                    <span>{service.replicas} {service.replicas === 1 ? 'replica' : 'replicas'} · {service.strategy}</span>
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
          open
          onOpenChange={(open) => { if (!open) setSelected(null) }}
        />
      )}
    </>
  )
}
