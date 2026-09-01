'use client'

import { useEffect, useState } from 'react'
import { ArrowDown, ArrowUp } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select } from '@/components/ui/select'
import { useStore } from '@/lib/store'
import type { Environment, ReleaseGroup, ReleaseGroupOnFailure } from '@/lib/types'

type GroupDraft = {
  name: string
  services: string[]
  order: string[]
  onFailure: ReleaseGroupOnFailure
}

const emptyDraft = (): GroupDraft => ({ name: '', services: [], order: [], onFailure: 'switch_back' })

export function ReleaseGroupFormDrawer({
  env,
  group,
  open,
  onOpenChange,
}: {
  env: Environment
  group?: ReleaseGroup
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  const store = useStore()
  const [draft, setDraft] = useState<GroupDraft>(emptyDraft)
	const [saving, setSaving] = useState(false)

  useEffect(() => {
    if (open) {
      setDraft(group
        ? { name: group.name, services: [...group.services], order: [...group.order], onFailure: group.onFailure }
        : emptyDraft())
    }
  }, [group, open])

  const duplicateName = env.releaseGroups.some(
    (candidate) => candidate.id !== group?.id && candidate.name === draft.name.trim(),
  )
	const valid = draft.name === draft.name.trim() && draft.name.length > 0 && !duplicateName && draft.services.length >= 2 && draft.services.length <= 32 &&
    draft.order.length === draft.services.length

  function toggleService(service: string) {
    setDraft((current) => current.services.includes(service)
      ? {
          ...current,
          services: current.services.filter((name) => name !== service),
          order: current.order.filter((name) => name !== service),
        }
      : { ...current, services: [...current.services, service], order: [...current.order, service] })
  }

  function move(service: string, offset: -1 | 1) {
    setDraft((current) => {
      const index = current.order.indexOf(service)
      const nextIndex = index + offset
      if (index < 0 || nextIndex < 0 || nextIndex >= current.order.length) return current
      const order = [...current.order]
      ;[order[index], order[nextIndex]] = [order[nextIndex], order[index]]
      return { ...current, order }
    })
  }

	async function save() {
    if (!valid) return
		setSaving(true)
		try {
    if (group) {
		await store.updateReleaseGroup(env.id, group.id, {
		name: draft.name,
        services: draft.services,
        order: draft.order,
        onFailure: draft.onFailure,
      })
    } else {
		await store.addReleaseGroup(env.id, {
		id: '',
		name: draft.name,
        services: draft.services,
        order: draft.order,
        onFailure: draft.onFailure,
      })
    }
    onOpenChange(false)
		} finally {
			setSaving(false)
		}
  }

  return (
    <Drawer open={open} onOpenChange={onOpenChange}>
      <DrawerContent>
        <DialogHeader>
          <DialogTitle>{group ? `Edit ${group.name}` : 'Add release group'}</DialogTitle>
          <DialogDescription>Choose at least two services and set their exact release order. Membership is never inferred.</DialogDescription>
        </DialogHeader>
        <div className="flex flex-col gap-5">
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="release-group-name">Name</Label>
			<Input id="release-group-name" value={draft.name} onChange={(event) => setDraft((current) => ({ ...current, name: event.target.value }))} placeholder="realtime" />
            {duplicateName && <p className="text-xs text-destructive">A release group with this name already exists.</p>}
			{draft.name !== draft.name.trim() && <p className="text-xs text-destructive">Leading or trailing whitespace is not allowed.</p>}
          </div>
          <fieldset className="flex flex-col gap-2">
            <legend className="mb-1 text-xs font-medium text-muted-foreground">Member services</legend>
            <div className="grid gap-2 sm:grid-cols-2">
              {env.services.map((service) => {
                const selected = draft.services.includes(service.name)
                return (
                  <button key={service.id} type="button" aria-pressed={selected} onClick={() => toggleService(service.name)} className={`flex items-center justify-between rounded-lg border p-3 text-left transition-colors ${selected ? 'border-primary/50 bg-primary/10' : 'border-border bg-surface hover:bg-muted/40'}`}>
                    <span><span className="block font-mono text-sm">{service.name}</span><span className="block text-xs text-muted-foreground">{service.role}</span></span>
                    <Badge variant={selected ? 'primary' : 'outline'}>{selected ? 'Selected' : 'Add'}</Badge>
                  </button>
                )
              })}
            </div>
            {draft.services.length < 2 && <p className="text-xs text-warning">Select at least two services.</p>}
          </fieldset>
          <div className="flex flex-col gap-2">
            <Label>Release order</Label>
            {draft.order.length === 0 ? (
              <p className="rounded-lg border border-dashed border-border p-4 text-center text-xs text-muted-foreground">Selected services appear here.</p>
            ) : draft.order.map((service, index) => (
              <div key={service} className="flex items-center gap-2 rounded-lg border border-border bg-surface p-2">
                <span className="flex size-6 items-center justify-center rounded-full bg-primary/10 font-mono text-xs text-primary">{index + 1}</span>
                <span className="flex-1 font-mono text-sm">{service}</span>
                <Button type="button" variant="ghost" size="icon-sm" aria-label={`Move ${service} up`} disabled={index === 0} onClick={() => move(service, -1)}><ArrowUp /></Button>
                <Button type="button" variant="ghost" size="icon-sm" aria-label={`Move ${service} down`} disabled={index === draft.order.length - 1} onClick={() => move(service, 1)}><ArrowDown /></Button>
              </div>
            ))}
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="release-group-failure">On failure</Label>
            <Select id="release-group-failure" value={draft.onFailure} onValueChange={(value) => setDraft((current) => ({ ...current, onFailure: value as ReleaseGroupOnFailure }))} options={[{ value: 'switch_back', label: 'Switch back completed members' }, { value: 'leave_active', label: 'Leave completed members active' }]} />
            <p className="text-xs text-muted-foreground">Default: switch back every member already advanced by this task.</p>
          </div>
        </div>
		<DialogFooter><Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button><Button onClick={() => void save()} disabled={!valid || saving}>{saving ? 'Dispatching…' : group ? 'Save changes' : 'Add group'}</Button></DialogFooter>
      </DrawerContent>
    </Drawer>
  )
}
