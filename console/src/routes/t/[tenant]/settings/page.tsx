'use client'

import { useNavigate } from 'react-router-dom'
import { useRequiredParams } from '@/lib/router'
import { useState } from 'react'
import { Boxes, Building2, Fingerprint, Pencil, Save, Trash2 } from 'lucide-react'
import { PageHeader } from '@/components/common/page-header'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { MetaPill } from '@/components/common/meta-pill'
import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'
import { EmptyState } from '@/components/common/empty-state'
import { useStore } from '@/lib/store'
import type { TaskStep } from '@/lib/types'

export default function TenantSettingsPage() {
  const params = useRequiredParams('tenant')
  const navigate = useNavigate()
  const store = useStore()
  const tenant = store.getTenant(params.tenant)
  const [name, setName] = useState(tenant?.name ?? '')
  const [description, setDescription] = useState(tenant?.description ?? '')
  const [slug, setSlug] = useState(tenant?.slug ?? '')
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [mutationError, setMutationError] = useState<string | null>(null)
  const [mutating, setMutating] = useState(false)

  if (!tenant) {
    if (store.tenantsLoading) return <EmptyState icon={<Building2 />} title="Loading tenant" />
    return <EmptyState icon={<Building2 />} title="Tenant not found" description={store.tenantError ?? undefined} />
  }

  const projects = store.tenantProjects.filter((p) => p.tenantId === tenant.id)
  const runners = store.runners.filter((r) => r.tenantId === tenant.id)
  const environments = projects.flatMap((p) => p.environments ?? [])
  const destroySteps: TaskStep[] = [
    { label: `Detach all ${projects.flatMap((p) => (p.environments ?? []).flatMap((e) => e.attaches)).length} backing attaches`, state: 'pending' },
    { label: `Destroy ${environments.length} environments (volumes + data)`, state: 'pending' },
    { label: `Delete ${projects.length} projects (secrets, connectors, runners)`, state: 'pending' },
    { label: `Remove tenant ${tenant.name} from etcd`, state: 'pending' },
  ]

  return (
	<div className={`flex flex-col gap-6 ${tenant.deletionTaskId ? 'pointer-events-none opacity-60' : ''}`}>
      <PageHeader
        eyebrow={`Tenant · ${tenant.slug}`}
        title="Tenant settings"
        description="The tenant is a strict isolation boundary — organizational, not access-control. Rename, describe, or delete it here."
        icon={<Building2 />}
        meta={
          <>
			<MetaPill icon={<Fingerprint />}>{tenant.id}</MetaPill>
			<MetaPill icon={<Boxes />}>{projects.length} projects · {environments.length} environments</MetaPill>
			{tenant.deletionTaskId && <MetaPill icon={<Trash2 />}>Deletion task {tenant.deletionTaskId}</MetaPill>}
          </>
        }
      />

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Pencil className="size-4 text-muted-foreground" /> Identity
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="t-id">Tenant id</Label>
            <Input id="t-id" value={tenant.id} readOnly className="font-mono text-muted-foreground" />
            <p className="text-xs text-muted-foreground">
              The stable id — every tenant-scoped reference keys off it. The slug is only the URL label, renamable
              whenever you like; nothing breaks, because nothing references the slug.
            </p>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="t-slug">Slug (URL label, hyphenated)</Label>
              <Input
                id="t-slug"
                value={slug}
                onChange={(e) => setSlug(e.target.value.toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-'))}
                className="font-mono"
                placeholder="my-tenant"
              />
              <div>
                <Button
                  size="sm"
                  variant="outline"
                  disabled={mutating || !slug.trim() || slug.trim() === tenant.slug}
                  onClick={() => void (async () => {
                    const nextSlug = slug.trim()
                    setMutating(true)
                    setMutationError(null)
                    try {
                      const renamed = await store.renameTenant(tenant.slug, nextSlug)
                      navigate(`/t/${renamed.slug}/settings`, { replace: true })
                    } catch (error) {
                      setMutationError(error instanceof Error ? error.message : 'Unable to rename tenant')
                    } finally {
                      setMutating(false)
                    }
                  })()}
                >
                  Rename slug
                </Button>
              </div>
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="t-name">Name</Label>
              <Input id="t-name" value={name} onChange={(e) => setName(e.target.value)} placeholder="Acme" />
            </div>
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="t-desc">Description</Label>
            <Input
              id="t-desc"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="What runs inside this isolation boundary"
            />
          </div>
          <div className="flex items-center justify-between">
            <p className="text-xs text-muted-foreground">
              {projects.length} projects · {environments.length} environments · {runners.length} runners inside this
              tenant.
            </p>
            <Button
              size="sm"
              disabled={mutating || (name.trim() === tenant.name && description === tenant.description)}
              onClick={() => void (async () => {
                setMutating(true)
                setMutationError(null)
                try {
                  const updated = await store.updateTenant(tenant.slug, { name: name.trim(), description })
                } catch (error) {
                  setMutationError(error instanceof Error ? error.message : 'Unable to update tenant')
                } finally {
                  setMutating(false)
                }
              })()}
            >
              <Save className="size-4" /> Save profile
            </Button>
          </div>
          {mutationError && <p role="alert" className="text-xs text-destructive">{mutationError}</p>}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-destructive">
            <Trash2 className="size-4" /> Danger zone
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <p className="max-w-3xl text-xs text-muted-foreground">
            Deleting the tenant removes its isolation boundary and everything inside it:{' '}
            <span className="font-medium text-foreground">{projects.length} projects</span> ({environments.length}{' '}
            environments), their volumes, secrets, connectors, and {runners.length} runners. Backing services are not
            tenant-owned and survive. This cannot be undone.
          </p>
          <div>
			<Button variant="destructive" size="sm" disabled={tenant.deletionTaskId !== null} onClick={() => setConfirmOpen(true)}>
              <Trash2 className="size-4" /> Delete tenant
            </Button>
          </div>
        </CardContent>
      </Card>

      <TaskRunnerDialog
        open={confirmOpen}
        onOpenChange={setConfirmOpen}
        title={`Delete tenant ${tenant.name}`}
        description={`This permanently removes the tenant and everything inside it — ${projects.length} projects, ${environments.length} environments with volumes and data, secrets, connectors, and runners. Backing services survive.`}
		type="remove"
        target={tenant.id}
        workspace={tenant.slug}
        destructive
        confirmText={tenant.slug}
        startLabel="Delete tenant"
        steps={destroySteps}
		onDispatch={() => store.removeTenant(tenant.id)}
		onCommit={() => navigate('/platform/overview', { replace: true })}
      />
    </div>
  )
}
