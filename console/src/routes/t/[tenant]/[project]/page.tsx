'use client'

import { useState } from 'react'
import { Link } from 'react-router-dom'
import { useRequiredParams } from '@/lib/router'
import { ArrowLeft, Boxes, Building2, ExternalLink, KeyRound, Layers, Plug, Plus, Settings, Trash2, Router as RouterIcon } from 'lucide-react'
import { useStore } from '@/lib/store'
import { PageHeader } from '@/components/common/page-header'
import { StatusBadge, StatusDot } from '@/components/common/status-badge'
import { MetaPill } from '@/components/common/meta-pill'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import { EmptyState } from '@/components/common/empty-state'
import { ResourceActionMenu } from '@/components/common/resource-action-menu'
import { HierarchyDeleteDialog } from '@/components/common/hierarchy-delete-dialog'
import type { Environment } from '@/lib/types'

export default function TenantProjectPage() {
  const params = useRequiredParams('tenant', 'project')
  const store = useStore()
  const tenant = store.getTenant(params.tenant)
  const project = store.getProject(params.tenant, params.project)
  const [open, setOpen] = useState(false)
  const [name, setName] = useState('')
  const [networkPool, setNetworkPool] = useState('')
  const [removingEnvironment, setRemovingEnvironment] = useState<Environment | null>(null)

  if (!tenant || !project || project.tenantId !== tenant.id) {
    return (
      <EmptyState
        icon={<Boxes />}
        title="Project not found"
        description={`${params.tenant}/${params.project} does not exist.`}
        action={
          <Link to={`/t/${params.tenant}`}>
            <Button variant="outline">
              <ArrowLeft className="size-4" /> Back to tenant
            </Button>
          </Link>
        }
      />
    )
  }

  const envs = project.environments ?? []
  const svcCount = envs.reduce((n, e) => n + e.services.length, 0)
  const repoRunner = store.runners.find((r) => r.tenantId === tenant.id && r.projectId === project.id)

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title={
          <>
            {project.name}
            {repoRunner && (
              <span className="ml-2 inline-flex items-center gap-1.5 rounded-full border border-border bg-surface px-3 py-1 align-middle text-xs text-muted-foreground">
                <StatusDot status={repoRunner.online ? 'healthy' : 'stopped'} />
                Runner · {repoRunner.labels.join(', ') || repoRunner.id} ·{' '}
                <span className={repoRunner.online ? 'text-success' : ''}>{repoRunner.online ? 'online' : 'offline'}</span>
              </span>
            )}
          </>
        }
        description={project.description}
        icon={<Boxes />}
        meta={
          <>
            <MetaPill icon={<Building2 />}>{tenant.id}</MetaPill>
            <MetaPill icon={<Layers />}>
              {envs.length} environment{envs.length === 1 ? '' : 's'}
            </MetaPill>
            <MetaPill icon={<Boxes />}>{svcCount} services</MetaPill>
          </>
        }
        actions={
          <>
            <Link to={`/t/${tenant.slug}/${project.slug}/secrets`}>
              <Button variant="outline">
                <KeyRound className="size-4" /> Secrets
              </Button>
            </Link>
            <Button onClick={() => setOpen(true)}>
              <Plus className="size-4" /> New environment
            </Button>
          </>
        }
      />

      {/* Environment selector */}
      {envs.length === 0 ? (
        <EmptyState
          icon={<Boxes />}
          title="No environments yet"
          description="An environment is one deployable instance of this project — staging, production, anything."
          action={
            <Button onClick={() => setOpen(true)}>
              <Plus className="size-4" /> New environment
            </Button>
          }
        />
      ) : (
        <div className="flex flex-col gap-3">
          {envs.map((e) => (
            <div key={e.id} className="relative rounded-xl border border-border bg-card transition-colors hover:border-ring/50">
              <Link
                to={`/t/${tenant.slug}/${project.slug}/${e.name}`}
                className="group flex flex-col gap-4 p-4 pr-14 md:flex-row md:items-center md:justify-between"
              >
              <div className="flex min-w-0 items-center gap-3">
                <div className="flex size-10 shrink-0 items-center justify-center rounded-lg border border-border bg-surface text-primary [&_svg]:size-5">
                  <Layers />
                </div>
                <div className="flex min-w-0 flex-col gap-0.5">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-base font-semibold">{e.name}</span>
                    <StatusBadge status={e.status} />
                  </div>
                  <span className="truncate font-mono text-xs text-muted-foreground">
                    release {e.release} · {e.services.length} services
                  </span>
                </div>
              </div>

              <div className="flex flex-wrap items-center gap-x-8 gap-y-3">
                <EnvStat icon={<Boxes />} value={e.services.length} label="services" />
                <EnvStat icon={<RouterIcon />} value={e.routes.length} label="routes" />
                <EnvStat icon={<Layers />} value={e.zones.length} label="zones" />
                <EnvStat icon={<Plug />} value={e.attaches.length} label="attached" />
              </div>

              <div className="flex items-center gap-3 md:shrink-0">
                <span className="hidden text-xs text-muted-foreground lg:block">deployed {e.lastDeployAt}</span>
              </div>
              </Link>
              <div className="absolute right-3 top-3">
                <ResourceActionMenu
                  label={`Actions for ${e.name}`}
                  actions={[
                    { label: 'Open environment', icon: <ExternalLink />, href: `/t/${tenant.slug}/${project.slug}/${e.name}` },
                    { label: 'Environment settings', icon: <Settings />, href: `/t/${tenant.slug}/${project.slug}/${e.name}?tab=settings` },
                    { label: 'Delete environment', icon: <Trash2 />, destructive: true, disabled: e.deletionTaskId !== null, onSelect: () => setRemovingEnvironment(e) },
                  ]}
                />
              </div>
            </div>
          ))}
        </div>
      )}

      <Drawer open={open} onOpenChange={setOpen}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>New environment · {project.name}</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="e-name">Name</Label>
              <Input id="e-name" value={name} onChange={(e) => setName(e.target.value)} placeholder="production" autoFocus />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="e-network-pool">Network pool</Label>
              <Input
                id="e-network-pool"
                value={networkPool}
                onChange={(e) => setNetworkPool(e.target.value)}
                placeholder="10.200.0.0/16"
              />
            </div>
            <p className="text-xs text-muted-foreground">
              One deployable instance of the project. Public routes need the ingress components (Caddy + Cloudflare Tunnel),
              enabled on the environment&apos;s Router tab — never auto-deployed.
            </p>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button
              disabled={!name.trim() || !networkPool.trim()}
              onClick={async () => {
                const slug = name.trim().toLowerCase().replace(/[^a-z0-9-]/g, '')
                await store.addEnvironment(project.id, slug, networkPool.trim())
                setOpen(false)
                setName('')
                setNetworkPool('')
              }}
            >
              Create environment
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>

      {removingEnvironment ? (
        <HierarchyDeleteDialog
          open
          onOpenChange={(open) => !open && setRemovingEnvironment(null)}
          kind="environment"
          name={removingEnvironment.name}
          id={removingEnvironment.id}
          workspace={tenant.slug}
          description="Permanently removes this Environment and every workload, volume, Entry, Attach, backup, recovery point, and Component state it owns."
          steps={[
            { label: 'Freeze Environment membership', state: 'pending' },
            { label: 'Remove workload runtime and storage', state: 'pending' },
            { label: 'Remove Environment-scoped state', state: 'pending' },
            { label: 'Remove the Environment', state: 'pending' },
          ]}
          onDispatch={() => store.deleteEnvironment(removingEnvironment.id)}
          onCommit={() => setRemovingEnvironment(null)}
        />
      ) : null}
    </div>
  )
}

function EnvStat({ icon, value, label }: { icon: React.ReactNode; value: number; label: string }) {
  return (
    <div className="flex flex-col items-center gap-1">
      <span className="flex items-center gap-1.5 text-muted-foreground [&_svg]:size-3.5">
        {icon}
        <span className="font-mono text-base font-semibold leading-none tabular-nums text-foreground">{value}</span>
      </span>
      <span className="text-[11px] uppercase tracking-wide text-muted-foreground/70">{label}</span>
    </div>
  )
}
