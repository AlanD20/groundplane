'use client'

import { Brand } from './brand'
import { ThemeToggle } from '@/components/common/theme-toggle'

import { useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { Menu } from 'lucide-react'
import { Sidebar } from './sidebar'
import { Topbar } from './topbar'
import { WorkspaceSwitcher } from './workspace-switcher'
import { TooltipProvider } from '@/components/ui/tooltip'
import { DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { useStore } from '@/lib/store'
import { useLinkedSlug } from '@/lib/use-linked-slug'

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
      <header className="fixed inset-x-0 top-0 z-40 flex h-[74px] items-center justify-between border-b border-border bg-background px-5 sm:px-8"><Brand /><ThemeToggle /></header>
      <div className="flex min-h-screen w-full pt-[74px]">
        {/* Desktop sidebar */}
        <aside className="fixed bottom-0 left-0 top-[74px] z-30 hidden w-[216px] flex-col border-r border-border bg-sidebar lg:flex">
          <div className="px-3 py-3">
            <WorkspaceSwitcher workspace={workspace} onNewTenant={() => setNewTenantOpen(true)} />
          </div>
          <div className="flex-1 overflow-y-auto">
            <Sidebar workspace={workspace} projectSlug={projectSlug} pathname={pathname} />
          </div>
        </aside>

        <Drawer open={mobileOpen} onOpenChange={setMobileOpen}>
          <DrawerContent className="left-0 right-auto max-w-72 border-l-0 border-r p-0 data-[ending-style]:-translate-x-full data-[starting-style]:-translate-x-full" onClick={(event) => {
            if (event.target instanceof Element && event.target.closest('a')) setMobileOpen(false)
          }}>
            <DialogTitle className="sr-only">Navigation</DialogTitle>
            <div className="border-b border-border p-4"><Brand /></div>
            <div className="px-3">
              <WorkspaceSwitcher workspace={workspace} onNewTenant={() => { setMobileOpen(false); setNewTenantOpen(true) }} />
            </div>
            <div className="flex-1 overflow-y-auto">
              <Sidebar workspace={workspace} projectSlug={projectSlug} pathname={pathname} />
            </div>
          </DrawerContent>
        </Drawer>

        {/* Main column */}
        <div className="flex min-w-0 flex-1 flex-col lg:pl-[216px]">
          <div className="flex items-center gap-2 lg:block">
            <Button variant="outline" size="icon"
              onClick={() => setMobileOpen(true)}
              className="ml-3 mt-3 flex items-center justify-center rounded-md border border-border bg-surface p-2 text-muted-foreground lg:hidden"
            >
              <Menu className="size-5" />
              <span className="sr-only">Open menu</span>
            </Button>
            <div className="min-w-0 flex-1">
              <Topbar pathname={pathname} />
            </div>
          </div>
          <main className="mx-auto w-full max-w-[1600px] flex-1 px-4 py-7 sm:px-6 lg:px-9 console-content"><div className="console-page" key={pathname}>{children}</div></main>
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
