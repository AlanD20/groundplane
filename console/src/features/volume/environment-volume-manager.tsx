'use client'

import { useEffect, useState } from 'react'
import { Database, Eye, Pencil, Plus, Trash2 } from 'lucide-react'
import { useRequiredParams } from '@/lib/router'
import { useStore } from '@/lib/store'
import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import type { Environment, VolumeDeletionImpactPage } from '@/lib/types'

export function EnvironmentVolumeManager({ env }: { env: Environment }) {
  const store = useStore()
  const params = useRequiredParams('tenant')
  const [open, setOpen] = useState(false)
  const [slug, setSlug] = useState('')
  const [key, setKey] = useState('')
  const [saveError, setSaveError] = useState<string | null>(null)
  const [editing, setEditing] = useState<Environment['volumes'][number] | null>(null)
  const [editSlug, setEditSlug] = useState('')
  const [viewing, setViewing] = useState<Environment['volumes'][number] | null>(null)
  const [removing, setRemoving] = useState<Environment['volumes'][number] | null>(null)
  const [impactPages, setImpactPages] = useState<VolumeDeletionImpactPage[]>([])
  const [impactError, setImpactError] = useState<string | null>(null)
  const [impactLoading, setImpactLoading] = useState(false)

  useEffect(() => {
    if (!removing) {
      setImpactPages([])
      setImpactError(null)
      setImpactLoading(false)
      return
    }
    let cancelled = false
    setImpactPages([])
    setImpactError(null)
    setImpactLoading(true)
    void (async () => {
      try {
        const pages: VolumeDeletionImpactPage[] = []
        const seen = new Set<string>()
        let runningCount = 0
        let cursor = ''
        let identity = ''
        let revision = 0
        let head = ''
        for (let pageNumber = 0; pageNumber < 1024; pageNumber += 1) {
          const page = await store.getVolumeDeletionImpact(removing.id, cursor, 40)
          if (
            page.volume_id !== removing.id ||
            page.environment_id !== removing.environmentId ||
            page.slug !== removing.slug ||
            page.key !== removing.key ||
            page.rolling_digest === '' ||
            page.data_handling !== 'recursive_destroy'
          )
            throw new Error('Controller returned an invalid volume deletion-impact page')
          runningCount += page.items.length
          if (page.item_count !== runningCount) throw new Error('Volume deletion impact count does not join')
          for (const item of page.items) {
            if (seen.has(item.id)) throw new Error('Volume deletion impact item repeated')
            seen.add(item.id)
          }
          if (!identity) {
            identity = page.volume_id
            revision = page.revision
            head = page.environment_head
          } else if (page.volume_id !== identity || page.revision !== revision || page.environment_head !== head) {
            throw new Error('Volume deletion impact changed while it was being reviewed')
          }
          if (page.complete) {
            if (page.next_cursor) throw new Error('Final impact page unexpectedly has a cursor')
            if (!page.impact_token) throw new Error('Final impact page did not include an impact token')
            pages.push(page)
            const itemCount = pages.reduce((total, itemPage) => total + itemPage.items.length, 0)
            if (itemCount !== page.item_count) throw new Error('Volume deletion impact count is inconsistent')
            break
          }
          if (!page.next_cursor || page.impact_token) throw new Error('Incomplete impact page has invalid pagination')
          pages.push(page)
          cursor = page.next_cursor
          if (pageNumber === 1023) throw new Error('Volume deletion impact exceeded the page limit')
        }
        if (!pages.at(-1)?.complete) throw new Error('Volume deletion impact did not reach a final page')
        if (!cancelled) setImpactPages(pages)
      } catch (cause: unknown) {
        if (!cancelled) setImpactError(cause instanceof Error ? cause.message : 'Unable to load deletion impact')
      } finally {
        if (!cancelled) setImpactLoading(false)
      }
    })()
    return () => {
      cancelled = true
    }
  }, [removing, store])

  const finalImpact = impactPages.at(-1)
  const impactItems = impactPages.flatMap((page) => page.items)
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <Database className="size-4 text-muted-foreground" /> Named volumes
        </CardTitle>
        <Button variant="outline" size="sm" onClick={() => setOpen(true)}>
          <Plus className="size-3.5" /> Volume
        </Button>
      </CardHeader>
      <CardContent className="flex flex-col gap-1.5">
        {env.volumes.map((v) => (
          <div key={v.id} className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
            <div className="flex min-w-0 flex-col">
              <span className="font-mono text-sm">{v.slug}</span>
              <span className="font-mono text-xs text-muted-foreground">key: {v.key}</span>
            </div>
            <div className="flex items-center gap-3">
              <span className="text-xs text-muted-foreground">{v.state ?? 'unknown'}</span>
              {v.path && <span className="max-w-56 truncate font-mono text-xs text-muted-foreground">{v.path}</span>}
              <Button
                variant="ghost"
                size="icon-xs"
                title="Show volume"
                onClick={async () => {
                  try {
                    setViewing(await store.getVolume(v.id))
                  } catch (cause: unknown) {
                    setSaveError(cause instanceof Error ? cause.message : 'Unable to load volume')
                  }
                }}
              >
                <Eye className="size-3.5" />
              </Button>
              <Button
                variant="ghost"
                size="icon-xs"
                title="Edit volume slug"
                onClick={() => {
                  setEditSlug(v.slug)
                  setEditing(v)
                }}
              >
                <Pencil className="size-3.5" />
              </Button>
              <Button
                variant="ghost"
                size="icon-xs"
                className="text-muted-foreground hover:text-destructive"
                title="Remove volume"
                onClick={() => setRemoving(v)}
              >
                <Trash2 className="size-3.5" />
              </Button>
            </div>
          </div>
        ))}
        {env.volumes.length === 0 && <div className="text-xs text-muted-foreground">no volumes yet</div>}
        <p className="mt-2 text-xs text-muted-foreground">
          Volumes live inside the environment&apos;s controller-managed folder ({env.volumeDir}). No traversal outside it; no host ports.
        </p>
      </CardContent>

      <Drawer open={open} onOpenChange={setOpen}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>Add volume · {env.name}</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="v-slug">Slug</Label>
              <Input id="v-slug" value={slug} onChange={(e) => setSlug(e.target.value)} placeholder="app-data" autoFocus />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="v-key">Compose key (optional)</Label>
              <Input id="v-key" value={key} onChange={(e) => setKey(e.target.value)} placeholder="app_data" />
            </div>
            <p className="text-xs text-muted-foreground">
              Slugs are operator-facing labels. The immutable Compose key is used in desired state; the Controller derives the managed path.
            </p>
            {saveError && <p className="text-xs text-destructive">{saveError}</p>}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button
              disabled={!slug.trim()}
              onClick={async () => {
                setSaveError(null)
                try {
                  await store.addVolume(env.id, {
                    slug: slug.trim(),
                    key: key.trim() || undefined,
                  })
                  setOpen(false)
                  setSlug('')
                  setKey('')
                } catch (cause: unknown) {
                  setSaveError(cause instanceof Error ? cause.message : 'Unable to add volume')
                }
              }}
            >
              Add volume
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>
      <Dialog open={!!viewing} onOpenChange={(next) => !next && setViewing(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Volume · {viewing?.slug}</DialogTitle>
          </DialogHeader>
          {viewing && (
            <div className="grid grid-cols-2 gap-2 text-xs">
              <VolumeDetailCell label="ID" value={viewing.id} />
              <VolumeDetailCell label="Environment" value={viewing.environmentId} />
              <VolumeDetailCell label="Slug" value={viewing.slug} />
              <VolumeDetailCell label="Immutable key" value={viewing.key} />
              <VolumeDetailCell label="State" value={viewing.state ?? 'unknown'} />
              <VolumeDetailCell label="Path" value={viewing.path ?? 'not assigned'} />
              {viewing.currentTaskId && <VolumeDetailCell label="Current task" value={viewing.currentTaskId} />}
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setViewing(null)}>
              Close
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <Drawer open={!!editing} onOpenChange={(next) => !next && setEditing(null)}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>Edit volume · {editing?.slug}</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="volume-edit-slug">Slug</Label>
              <Input id="volume-edit-slug" value={editSlug} onChange={(event) => setEditSlug(event.target.value)} autoFocus />
            </div>
            <p className="text-xs text-muted-foreground">Changing the slug does not change the immutable Compose key or the derived path.</p>
            {saveError && <p className="text-xs text-destructive">{saveError}</p>}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setEditing(null)}>
              Cancel
            </Button>
            <Button
              disabled={!editSlug.trim()}
              onClick={async () => {
                if (!editing) return
                setSaveError(null)
                try {
                  const edited = await store.updateVolume(env.id, editing.id, {
                    slug: editSlug.trim(),
                  })
                  setEditing(null)
                } catch (cause: unknown) {
                  setSaveError(cause instanceof Error ? cause.message : 'Unable to edit volume')
                }
              }}
            >
              Save slug
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>
      <TaskRunnerDialog
        open={!!removing}
        onOpenChange={(next) => !next && setRemoving(null)}
        title={`Remove volume · ${removing?.slug ?? ''}`}
        description="Review every affected mount, backup source, policy, and recovery point before permanently removing this volume."
        type="destroy"
        target={removing?.id ?? env.id}
        workspace={params.tenant}
        destructive
        confirmText={removing?.key ?? ''}
        startDisabled={impactLoading || Boolean(impactError) || !finalImpact?.impact_token}
        startLabel="Remove volume"
        steps={[
          { label: 'Intent sealed', state: 'pending' },
          { label: 'Revision staged', state: 'pending' },
          { label: 'Desired state published', state: 'pending' },
          { label: 'Consumers detached', state: 'pending' },
          { label: 'Directory absent', state: 'pending' },
          { label: 'Runtime finalized', state: 'pending' },
        ]}
        review={
          <div className="flex flex-col gap-2">
            {impactLoading && <p className="text-sm text-muted-foreground">Loading all deletion consequences…</p>}
            {impactError && <p className="text-sm text-destructive">{impactError}</p>}
            {!impactLoading && !impactError && (
              <>
                <p className="text-sm">
                  {impactItems.length} consequence
                  {impactItems.length === 1 ? '' : 's'} at revision {finalImpact?.revision}.
                </p>
                <p className="text-xs text-warning">Data handling: recursive destroy. The managed volume directory and its contents will be removed.</p>
                <div className="max-h-64 overflow-y-auto rounded-lg border border-border bg-surface p-2">
                  {impactItems.length === 0 && <p className="p-2 text-xs text-muted-foreground">No dependent records were found.</p>}
                  {impactItems.map((item) => (
                    <div key={item.id} className="border-b border-border px-2 py-1.5 text-xs last:border-0">
                      <span className="font-mono">{item.kind}</span>
                      {item.service_name && <span className="text-muted-foreground"> · {item.service_name}</span>}
                      {item.mount_target && <span className="text-muted-foreground"> · {item.mount_target}</span>}
                      {item.source_id && <span className="text-muted-foreground"> · source {item.source_id}</span>}
                      {item.recovery_point_count !== undefined && <span className="text-muted-foreground"> · {item.recovery_point_count} recovery points</span>}
                    </div>
                  ))}
                </div>
              </>
            )}
          </div>
        }
        onDispatch={() => {
          if (!removing || !finalImpact?.impact_token) return Promise.reject(new Error('Deletion impact is not ready'))
          return store.removeVolume(env.id, removing.id, finalImpact.impact_token, removing.key)
        }}
      />
    </Card>
  )
}

function VolumeDetailCell({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface px-3 py-2">
      <span className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</span>
      <span className="break-all font-mono text-xs">{value}</span>
    </div>
  )
}
