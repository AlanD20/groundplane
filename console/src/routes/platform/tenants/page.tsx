'use client'

import { Link } from 'react-router-dom'
import { ArrowRight, Blocks, Boxes, Building2, Cpu, Layers } from 'lucide-react'
import { useStore } from '@/lib/store'
import { PageHeader } from '@/components/common/page-header'
import { MetaPill } from '@/components/common/meta-pill'
import { EmptyState } from '@/components/common/empty-state'

export default function PlatformTenantsPage() {
  const { tenants, tenantsLoading, tenantError, tenantProjects, runners } = useStore()

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Platform"
        title="Tenants"
        description="Every isolation boundary on this control plane. Projects, runners, and secrets live under a tenant."
        icon={<Building2 />}
        meta={<MetaPill icon={<Building2 />}>{tenants.length} tenants</MetaPill>}
      />

      {tenantsLoading ? (
        <EmptyState icon={<Building2 />} title="Loading tenants" />
      ) : tenantError ? (
        <EmptyState icon={<Building2 />} title="Unable to load tenants" description={tenantError} />
      ) : tenants.length === 0 ? (
        <EmptyState
          icon={<Building2 />}
          title="No tenants yet"
          description="Create a tenant from the workspace switcher to start."
        />
      ) : (
        <div className="grid gap-4 md:grid-cols-2">
          {tenants.map((t) => {
            const projects = tenantProjects.filter((p) => p.tenantId === t.id)
            const envs = projects.reduce((n, p) => n + (p.environments?.length ?? 0), 0)
            const svcs = projects.reduce(
              (n, p) => n + (p.environments ?? []).reduce((m, e) => m + e.services.length, 0),
              0,
            )
            const rs = runners.filter((r) => r.tenantId === t.id)
            return (
              <Link
                key={t.id}
                to={`/t/${t.slug}`}
                className="group rounded-xl border border-border bg-card p-4 transition-colors hover:border-ring/50"
              >
                <div className="flex items-start justify-between">
                  <div className="flex items-center gap-3">
                    <span className="flex size-10 shrink-0 items-center justify-center rounded-lg bg-primary/10 text-primary [&_svg]:size-5">
                      <Building2 />
                    </span>
                    <div className="flex flex-col">
                      <span className="text-base font-semibold">{t.name}</span>
                      <span className="font-mono text-xs text-muted-foreground">{t.slug}</span>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="rounded-full bg-success/10 px-2 py-0.5 text-xs text-success">isolated</span>
                    <ArrowRight className="size-4 text-muted-foreground/40 transition-transform group-hover:translate-x-0.5 group-hover:text-foreground" />
                  </div>
                </div>
                <p className="mt-2 text-sm text-muted-foreground">{t.description}</p>
                <div className="mt-3 flex flex-wrap items-center gap-x-6 gap-y-2 border-t border-border pt-3">
                  <MiniStat icon={<Boxes />} value={projects.length} label="projects" />
                  <MiniStat icon={<Layers />} value={envs} label="environments" />
                  <MiniStat icon={<Blocks />} value={svcs} label="services" />
                  <MiniStat icon={<Cpu />} value={rs.length} label="runners" />
                </div>
              </Link>
            )
          })}
        </div>
      )}
    </div>
  )
}

function MiniStat({ icon, value, label }: { icon: React.ReactNode; value: number; label: string }) {
  return (
    <span className="flex items-center gap-1.5 text-xs text-muted-foreground [&_svg]:size-3.5">
      {icon}
      <span className="font-mono text-sm font-semibold text-foreground">{value}</span>
      <span className="text-muted-foreground/70">{label}</span>
    </span>
  )
}
