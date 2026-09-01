'use client'

import { Link } from 'react-router-dom'
import { ArrowRight, Blocks, Boxes, Layers } from 'lucide-react'
import { useStore } from '@/lib/store'
import { PageHeader } from '@/components/common/page-header'
import { StatusDot } from '@/components/common/status-badge'
import { MetaPill } from '@/components/common/meta-pill'
import { EmptyState } from '@/components/common/empty-state'

export default function PlatformProjectsPage() {
  const { tenants, tenantProjects, projectsLoading, projectError } = useStore()

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Platform"
        title="Projects"
        description="Every tenant project on this control plane — each one contains environments (staging, production) built from services."
        icon={<Boxes />}
        meta={<MetaPill icon={<Boxes />}>{tenantProjects.length} projects</MetaPill>}
      />

      {projectsLoading ? (
        <EmptyState icon={<Boxes />} title="Loading projects" />
      ) : projectError ? (
        <EmptyState icon={<Boxes />} title="Unable to load projects" description={projectError} />
      ) : tenantProjects.length === 0 ? (
        <EmptyState
          icon={<Boxes />}
          title="No projects yet"
          description="Create a project inside a tenant to get started."
        />
      ) : (
        <div className="grid gap-4 md:grid-cols-2">
          {tenantProjects.map((p) => {
            const envs = p.environments ?? []
            const svcCount = envs.reduce((n, e) => n + e.services.length, 0)
            const tenant = tenants.find((t) => t.id === p.tenantId)
            return (
              <Link
                key={p.id}
                to={`/t/${tenant?.slug ?? p.tenantId}/${p.slug}`}
                className="group flex flex-col rounded-xl border border-border bg-card p-4 transition-colors hover:border-ring/50"
              >
                <div className="flex items-start justify-between">
                  <div className="flex items-center gap-3">
                    <span className="flex size-10 shrink-0 items-center justify-center rounded-lg bg-primary/10 text-primary [&_svg]:size-5">
                      <Boxes />
                    </span>
                    <div className="flex flex-col">
                      <div className="flex items-center gap-2">
                        <span className="text-base font-semibold">{p.name}</span>
                        {p.status && <StatusDot status={p.status} />}
                      </div>
                      {tenant && <span className="font-mono text-xs text-muted-foreground">{tenant.slug}</span>}
                    </div>
                  </div>
                  <ArrowRight className="size-4 text-muted-foreground/40 transition-transform group-hover:translate-x-0.5 group-hover:text-foreground" />
                </div>
                <p className="mt-2 flex-1 text-sm text-muted-foreground">{p.description}</p>
                <div className="mt-3 flex flex-wrap items-center gap-x-6 gap-y-2 border-t border-border pt-3">
                  <MiniStat icon={<Layers />} value={envs.length} label="environments" />
                  <MiniStat icon={<Blocks />} value={svcCount} label="services" />
                </div>
                {envs.length > 0 && (
                  <div className="mt-3 flex flex-wrap items-center gap-1.5">
                    {envs.map((e) => (
                      <span
                        key={e.id}
                        className="flex items-center gap-1.5 rounded-full border border-border bg-surface px-2 py-0.5 text-[11px] text-muted-foreground"
                      >
                        <StatusDot status={e.status} />
                        {e.name}
                      </span>
                    ))}
                  </div>
                )}
              </Link>
            )
          })}
        </div>
      )}
    </div>
  )
}

function MiniStat({ icon, value, label }: { icon: React.ReactNode; value: React.ReactNode; label: string }) {
  return (
    <span className="flex items-center gap-1.5 text-xs text-muted-foreground [&_svg]:size-3.5">
      {icon}
      <span className="font-mono text-sm font-semibold text-foreground">{value}</span>
      <span className="text-muted-foreground/70">{label}</span>
    </span>
  )
}
