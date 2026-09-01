'use client'

import { useState } from 'react'
import { Link } from 'react-router-dom'
import { Blocks, Boxes, Building2, Cpu, ExternalLink, Layers, Settings, Trash2 } from 'lucide-react'
import { useStore } from '@/lib/store'
import { PageHeader } from '@/components/common/page-header'
import { MetaPill } from '@/components/common/meta-pill'
import { EmptyState } from '@/components/common/empty-state'
import { ResourceActionMenu } from '@/components/common/resource-action-menu'
import { HierarchyDeleteDialog } from '@/components/common/hierarchy-delete-dialog'

export default function PlatformTenantsPage() {
  const store = useStore()
  const { tenants, tenantsLoading, tenantError, tenantProjects, runners } = store
  const [removing, setRemoving] = useState<(typeof tenants)[number] | null>(null)

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
              <div key={t.id} className="relative rounded-xl border border-border bg-card transition-colors hover:border-ring/50">
                <Link to={`/t/${t.slug}`} className="group block p-4 pr-14">
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
                <div className="absolute right-3 top-3">
                  <ResourceActionMenu
                    label={`Actions for ${t.name}`}
                    actions={[
                      { label: 'Open tenant', icon: <ExternalLink />, href: `/t/${t.slug}` },
                      { label: 'Tenant settings', icon: <Settings />, href: `/t/${t.slug}/settings` },
                      { label: 'Delete tenant', icon: <Trash2 />, destructive: true, disabled: t.deletionTaskId !== null, onSelect: () => setRemoving(t) },
                    ]}
                  />
                </div>
              </div>
            )
          })}
        </div>
      )}

      {removing ? (
        <HierarchyDeleteDialog
          open
          onOpenChange={(open) => !open && setRemoving(null)}
          kind="tenant"
          name={removing.slug}
          id={removing.id}
          workspace={removing.slug}
          description="Permanently removes this Tenant and every Project, Environment, workload, volume, Secret, Connector, and Runner it owns. Backing services survive."
          steps={[
            { label: 'Freeze Tenant membership', state: 'pending' },
            { label: 'Remove child runtime and storage', state: 'pending' },
            { label: 'Remove Projects and Environment state', state: 'pending' },
            { label: 'Remove the Tenant', state: 'pending' },
          ]}
          onDispatch={() => store.removeTenant(removing.id)}
          onCommit={() => setRemoving(null)}
        />
      ) : null}
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
