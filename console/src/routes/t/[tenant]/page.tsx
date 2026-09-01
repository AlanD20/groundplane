'use client'

import { useState } from 'react'
import { Link } from 'react-router-dom'
import { useRequiredParams } from '@/lib/router'
import { Blocks, Boxes, CalendarDays, Cpu, ExternalLink, Layers, Plus, Settings, ShieldCheck, Trash2 } from 'lucide-react'
import { useStore } from '@/lib/store'
import { useLinkedSlug } from '@/lib/use-linked-slug'
import { PageHeader } from '@/components/common/page-header'
import { StatusDot } from '@/components/common/status-badge'
import { MetaPill } from '@/components/common/meta-pill'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import { EmptyState } from '@/components/common/empty-state'
import { ResourceActionMenu } from '@/components/common/resource-action-menu'
import { HierarchyDeleteDialog } from '@/components/common/hierarchy-delete-dialog'

export default function TenantPage() {
  const params = useRequiredParams('tenant')
  const store = useStore()
  const tenant = store.getTenant(params.tenant)
  const projects = store.tenantProjects.filter((p) => p.tenantId === tenant?.id)
  const runners = store.runners.filter((r) => r.tenantId === tenant?.id)

  const [open, setOpen] = useState(false)
  const { name, slug, setName, setSlug, reset: resetProjectIdentity } = useLinkedSlug()
  const [description, setDescription] = useState('')
  const [projectError, setProjectError] = useState<string | null>(null)
  const [creatingProject, setCreatingProject] = useState(false)
  const [removingProject, setRemovingProject] = useState<(typeof projects)[number] | null>(null)

  if (!tenant) {
    if (store.tenantsLoading) return <EmptyState icon={<Boxes />} title="Loading tenant" />
    return <EmptyState icon={<Boxes />} title="Tenant not found" description={store.tenantError ?? `${params.tenant} does not exist.`} />
  }

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title={
          <>
            {tenant.name}
            {runners.map((r) => (
              <MetaPill key={r.id} icon={<StatusDot status={r.online ? 'healthy' : 'stopped'} />} className="ml-2 align-middle">
                {r.labels.join(', ') || r.id} · {r.projectId ? `project ${r.projectId}` : `tenant ${r.tenantId}`} ·{' '}
                <span className={r.online ? 'text-success' : ''}>{r.online ? 'online' : 'offline'}</span>
              </MetaPill>
            ))}
          </>
        }
        description={tenant.description}
        icon={<Boxes />}
        meta={
          <>
            <MetaPill icon={<Boxes />}>{projects.length} projects</MetaPill>
            <MetaPill icon={<Cpu />}>
              {runners.length} runners · {runners.filter((r) => r.online).length} online
            </MetaPill>
            <MetaPill tone="success" icon={<ShieldCheck />}>
              isolated
            </MetaPill>
          </>
        }
        actions={
          <Button onClick={() => setOpen(true)}>
            <Plus className="size-4" /> New project
          </Button>
        }
      />

      {projects.length === 0 ? (
        <EmptyState
          icon={<Boxes />}
          title="No projects yet"
          description="A project is an application — it may be a microservice architecture with many services."
          action={
            <Button onClick={() => setOpen(true)}>
              <Plus className="size-4" /> New project
            </Button>
          }
        />
      ) : (
        <div className="grid gap-4 md:grid-cols-2">
          {projects.map((p) => {
            const envs = p.environments ?? []
            const svcCount = envs.reduce((n, e) => n + e.services.length, 0)
            return (
              <div key={p.id} className="relative rounded-xl border border-border bg-card transition-colors hover:border-ring/50">
                <Link to={`/t/${tenant.slug}/${p.slug}`} className="group flex h-full flex-col p-4 pr-14">
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
                      {p.createdAt && <span className="font-mono text-xs text-muted-foreground">created {p.createdAt}</span>}
                    </div>
                  </div>
                </div>
                <p className="mt-2 flex-1 text-sm text-muted-foreground">{p.description}</p>
                <div className="mt-3 flex flex-wrap items-center gap-x-6 gap-y-2 border-t border-border pt-3">
                  <MiniStat icon={<Layers />} value={envs.length} label="environments" />
                  <MiniStat icon={<Blocks />} value={svcCount} label="services" />
                  <MiniStat icon={<Cpu />} value={store.runners.filter((r) => r.tenantId === tenant.id && r.projectId === p.id).length} label="runners" />
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
                <div className="absolute right-3 top-3">
                  <ResourceActionMenu
                    label={`Actions for ${p.name}`}
                    actions={[
                      { label: 'Open project', icon: <ExternalLink />, href: `/t/${tenant.slug}/${p.slug}` },
                      { label: 'Project settings', icon: <Settings />, href: `/t/${tenant.slug}/${p.slug}/settings` },
                      { label: 'Delete project', icon: <Trash2 />, destructive: true, disabled: p.deletionTaskId !== null, onSelect: () => setRemovingProject(p) },
                    ]}
                  />
                </div>
              </div>
            )
          })}
        </div>
      )}

      <Drawer open={open} onOpenChange={setOpen}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>New project · {tenant.name}</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="p-name">Name</Label>
              <Input id="p-name" value={name} onChange={(e) => setName(e.target.value)} placeholder="checkout" autoFocus />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="p-slug">Slug</Label>
              <Input id="p-slug" value={slug} onChange={(e) => setSlug(e.target.value)} placeholder="checkout" />
              <p className="text-xs text-muted-foreground">Follows Name until edited.</p>
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="p-desc">Description</Label>
              <Input id="p-desc" value={description} onChange={(e) => setDescription(e.target.value)} placeholder="What this project is" />
            </div>
            <p className="text-xs text-muted-foreground">
              A project may be a microservice architecture. Environments (staging, production) are created inside it, and each
              environment attaches backing services.
            </p>
            {projectError && <p role="alert" className="text-xs text-destructive">{projectError}</p>}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button
              disabled={creatingProject || !name.trim() || !slug.trim()}
              onClick={() => void (async () => {
                setCreatingProject(true)
                setProjectError(null)
                try {
                  await store.addProject({
                    tenantId: tenant.id, slug, name: name.trim(), description: description.trim(),
                  })
                  setOpen(false)
                  resetProjectIdentity()
                  setDescription('')
                } catch (error) {
                  setProjectError(error instanceof Error ? error.message : 'Unable to create project')
                } finally {
                  setCreatingProject(false)
                }
              })()}
            >
              {creatingProject ? 'Creating…' : 'Create project'}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>

      {removingProject ? (
        <HierarchyDeleteDialog
          open
          onOpenChange={(open) => !open && setRemovingProject(null)}
          kind="project"
          name={removingProject.slug}
          id={removingProject.id}
          workspace={tenant.slug}
          description="Permanently removes this Project and every Environment, workload, volume, Secret, Connector, and project Runner it owns."
          steps={[
            { label: 'Freeze Project membership', state: 'pending' },
            { label: 'Remove child runtime and storage', state: 'pending' },
            { label: 'Remove Environment state', state: 'pending' },
            { label: 'Remove the Project', state: 'pending' },
          ]}
          onDispatch={() => store.deleteProject(removingProject.id)}
          onCommit={() => setRemovingProject(null)}
        />
      ) : null}
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
