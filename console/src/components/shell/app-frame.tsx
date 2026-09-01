'use client'

import { useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { Link } from 'react-router-dom'
import { Menu, Ship, X } from 'lucide-react'
import { Sidebar } from './sidebar'
import { Topbar } from './topbar'
import { WorkspaceSwitcher } from './workspace-switcher'
import { TooltipProvider } from '@/components/ui/tooltip'
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { useStore } from '@/lib/store'
import { useLinkedSlug } from '@/lib/use-linked-slug'
import { cn } from '@/lib/utils'

function useWorkspace(pathname: string): { kind: 'platform' | 'tenant'; slug: string } {
  const parts = pathname.split('/').filter(Boolean)
  if (parts[0] === 't' && parts[1]) return { kind: 'tenant', slug: parts[1] }
  return { kind: 'platform', slug: 'platform' }
}

// The project the current route lives under, if any (/t/<tenant>/<project>/…).
// parts[2] is only a project when it actually names one — tenant-level
// sections (runners, activity, settings) must never trigger the project nav.
function useProjectSlug(pathname: string, projectSlugs: string[]): string | undefined {
  const parts = pathname.split('/').filter(Boolean)
  if (parts[0] === 't' && parts[1] && parts[2] && projectSlugs.includes(parts[2])) return parts[2]
  return undefined
}

export function AppFrame({ children }: { children: React.ReactNode }) {
  const { pathname } = useLocation()
  const workspace = useWorkspace(pathname)
  const navigate = useNavigate()
  const { addTenant, tenantProjects } = useStore()
  const projectSlug = useProjectSlug(
    pathname,
    tenantProjects.map((p) => p.slug),
  )

  const [mobileOpen, setMobileOpen] = useState(false)
  const [newTenantOpen, setNewTenantOpen] = useState(false)
  const { name, slug, setName, setSlug, reset: resetTenantIdentity } = useLinkedSlug()
  const [description, setDescription] = useState('')
  const [createError, setCreateError] = useState<string | null>(null)
  const [creating, setCreating] = useState(false)

  async function createTenant() {
    const s = slug.trim()
    if (!s) return
    setCreating(true)
    setCreateError(null)
    try {
      const tenant = await addTenant({ slug: s, name: name.trim() || s, description: description.trim() })
      setNewTenantOpen(false)
      resetTenantIdentity()
      setDescription('')
      navigate(`/t/${tenant.slug}`)
    } catch (error) {
      setCreateError(error instanceof Error ? error.message : 'Unable to create tenant')
    } finally {
      setCreating(false)
    }
  }

  return (
    <TooltipProvider>
      <div className="flex min-h-screen w-full">
        {/* Desktop sidebar */}
        <aside className="fixed inset-y-0 left-0 z-40 hidden w-64 flex-col border-r border-border bg-sidebar lg:flex">
          <div className="flex h-14 items-center gap-2 border-b border-border px-4">
            <Link to="/platform/overview" className="flex items-center gap-2 outline-none">
              <span className="flex size-7 items-center justify-center rounded-md bg-primary text-primary-foreground">
                <Ship className="size-4" />
              </span>
              <span className="text-sm font-semibold tracking-tight">groundplane</span>
            </Link>
          </div>
          <div className="px-3 py-3">
            <WorkspaceSwitcher workspace={workspace} onNewTenant={() => setNewTenantOpen(true)} />
          </div>
          <div className="flex-1 overflow-y-auto">
            <Sidebar workspace={workspace} projectSlug={projectSlug} pathname={pathname} />
          </div>
        </aside>

        {/* Mobile drawer */}
        <div className={cn('fixed inset-0 z-50 lg:hidden', mobileOpen ? 'pointer-events-auto' : 'pointer-events-none')}>
          <div
            className={cn(
              'absolute inset-0 bg-black/60 transition-opacity',
              mobileOpen ? 'opacity-100' : 'opacity-0',
            )}
            onClick={() => setMobileOpen(false)}
          />
          <aside
            className={cn(
              'absolute inset-y-0 left-0 flex w-72 flex-col border-r border-border bg-sidebar transition-transform',
              mobileOpen ? 'translate-x-0' : '-translate-x-full',
            )}
          >
            <div className="flex h-14 items-center justify-between border-b border-border px-4">
              <span className="flex items-center gap-2">
                <span className="flex size-7 items-center justify-center rounded-md bg-primary text-primary-foreground">
                  <Ship className="size-4" />
                </span>
                <span className="text-sm font-semibold tracking-tight">groundplane</span>
              </span>
              <button onClick={() => setMobileOpen(false)} className="rounded-md p-1 text-muted-foreground hover:text-foreground">
                <X className="size-5" />
                <span className="sr-only">Close menu</span>
              </button>
            </div>
            <div className="px-3 py-3" onClick={() => setMobileOpen(false)}>
              <WorkspaceSwitcher workspace={workspace} onNewTenant={() => setNewTenantOpen(true)} />
            </div>
            <div className="flex-1 overflow-y-auto" onClick={() => setMobileOpen(false)}>
              <Sidebar workspace={workspace} projectSlug={projectSlug} pathname={pathname} />
            </div>
          </aside>
        </div>

        {/* Main column */}
        <div className="flex min-w-0 flex-1 flex-col lg:pl-64">
          <div className="flex items-center gap-2 lg:block">
            <button
              onClick={() => setMobileOpen(true)}
              className="ml-3 mt-3 flex items-center justify-center rounded-md border border-border bg-surface p-2 text-muted-foreground lg:hidden"
            >
              <Menu className="size-5" />
              <span className="sr-only">Open menu</span>
            </button>
            <div className="min-w-0 flex-1">
              <Topbar pathname={pathname} />
            </div>
          </div>
          <main className="mx-auto w-full max-w-[1400px] flex-1 px-4 py-6 sm:px-6 lg:px-8">{children}</main>
        </div>
      </div>

      <Drawer open={newTenantOpen} onOpenChange={setNewTenantOpen}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>New tenant</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="t-name">Display name</Label>
              <Input id="t-name" value={name} onChange={(e) => setName(e.target.value)} placeholder="Acme Inc." autoFocus />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="t-slug">Slug</Label>
              <Input
                id="t-slug"
                value={slug}
                onChange={(e) => setSlug(e.target.value)}
                placeholder="acme"
              />
              <p className="text-xs text-muted-foreground">
                Follows Display name until edited. Projects, runners, and secrets live under this isolation boundary.
              </p>
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="t-desc">Description</Label>
              <Input id="t-desc" value={description} onChange={(e) => setDescription(e.target.value)} placeholder="What this tenant hosts" />
            </div>
            {createError && <p role="alert" className="text-xs text-destructive">{createError}</p>}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setNewTenantOpen(false)}>
              Cancel
            </Button>
            <Button onClick={() => void createTenant()} disabled={creating || !name.trim() || !slug.trim()}>
              {creating ? 'Creating…' : 'Create tenant'}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>
    </TooltipProvider>
  )
}
