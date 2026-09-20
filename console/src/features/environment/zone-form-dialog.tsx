import { useState } from 'react'
import { useStore } from '@/lib/store'
import type { Environment } from '@/lib/types'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import { DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { FormSection } from '@/components/ui/form-section'
import { NetworkRange } from '@/components/common/network-range'

export function ZoneFormDialog({
  env,
  open,
  onOpenChange,
}: {
  env: Environment
  open: boolean
  onOpenChange: (v: boolean) => void
}) {
  const store = useStore()
  const [name, setName] = useState('')
  const [subnet, setSubnet] = useState('')
  const [internal, setInternal] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [submitError, setSubmitError] = useState<string | null>(null)
  const normalizedName = name.trim()
  const nameError =
    normalizedName !== '' && !/^[A-Za-z0-9._-]+$/.test(normalizedName)
      ? 'Use only letters, numbers, dot, underscore, and hyphen.'
      : null
  const subnetError = subnet.trim() ? null : 'Subnet is required.'
  const submit = async () => {
    setSubmitting(true)
    setSubmitError(null)
    try {
      await store.addZone(env.id, {
        name: normalizedName,
        subnet: subnet.trim(),
        internal,
      })
      onOpenChange(false)
      setName('')
      setSubnet('')
      setInternal(false)
    } catch (error) {
      setSubmitError(error instanceof Error ? error.message : 'Unable to create Zone')
    } finally {
      setSubmitting(false)
    }
  }
  return (
    <Drawer open={open} onOpenChange={onOpenChange}>
      <DrawerContent>
        <DialogHeader>
          <DialogTitle>Add zone · {env.name}</DialogTitle>
        </DialogHeader>
        <FormSection title="Zone details" description="Choose a name and a subnet within the Environment pool.">
          <NetworkRange cidr={env.networkPool} label="Environment pool" />
          {env.zones.length > 0 && (
            <p className="text-xs text-muted-foreground sm:col-span-2">
              Existing zones: {env.zones.map((zone) => `${zone.name} (${zone.subnet})`).join(', ')}
            </p>
          )}
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="z-name">Zone name</Label>
            <Input id="z-name" value={name} onChange={(e) => setName(e.target.value)} placeholder="backend" autoFocus />
            <p className={nameError ? 'text-xs text-destructive' : 'text-xs text-muted-foreground'}>
              {nameError ?? 'Must be a valid Docker Compose network key.'}
            </p>
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="z-subnet">Zone subnet (CIDR)</Label>
            <Input
              id="z-subnet"
              value={subnet}
              onChange={(e) => setSubnet(e.target.value)}
              placeholder="10.200.20.0/24"
            />
            <p className="text-xs text-muted-foreground">
              Required. Must be inside {env.networkPool} and must not overlap an existing zone.
            </p>
          </div>
          <label className="flex items-center gap-2 text-sm sm:col-span-2">
            <Switch checked={internal} onCheckedChange={setInternal} />
            <span className="text-muted-foreground">Internal network (no external access through this zone)</span>
          </label>
          {submitError && (
            <p className="text-sm text-destructive" role="alert">
              {submitError}
            </p>
          )}
        </FormSection>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
            disabled={!normalizedName || nameError !== null || subnetError !== null || submitting}
            onClick={() => void submit()}
          >
            {submitting ? 'Creating…' : 'Create zone'}
          </Button>
        </DialogFooter>
      </DrawerContent>
    </Drawer>
  )
}
