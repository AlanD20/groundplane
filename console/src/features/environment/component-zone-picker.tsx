import { useId, useRef, useState } from 'react'

import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import type { Zone } from '@/lib/types'

import {
  createAndSelectZone,
  mergeOrdinaryZones,
  toggleSelectedZone,
  type ZoneCreateInput,
} from './component-zone-picker-state'
import { Checkbox } from '@/components/ui/checkbox'

export type ComponentZonePickerProps = {
  zones: Zone[]
  selectedZoneIds: string[]
  onChange: (ids: string[]) => void
  onCreate: (input: ZoneCreateInput) => Promise<Zone>
  networkPool: string
  disabled?: boolean
}

export function ComponentZonePicker({
  zones,
  selectedZoneIds,
  onChange,
  onCreate,
  networkPool,
  disabled = false,
}: ComponentZonePickerProps) {
  const id = useId()
  const creatingRef = useRef(false)
  const [createdZones, setCreatedZones] = useState<Zone[]>([])
  const [name, setName] = useState('')
  const [subnet, setSubnet] = useState('')
  const [internal, setInternal] = useState(false)
  const [creating, setCreating] = useState(false)
  const [createError, setCreateError] = useState<string | null>(null)

  const availableZones = mergeOrdinaryZones(zones, createdZones)
  const normalizedName = name.trim()
  const normalizedSubnet = subnet.trim()
  const nameError = normalizedName && !/^[A-Za-z0-9._-]+$/.test(normalizedName)
    ? 'Use only letters, numbers, dot, underscore, and hyphen.'
    : null
  const subnetError = normalizedSubnet ? null : 'Subnet is required.'
  const createDisabled = disabled || creating || !normalizedName || nameError !== null || subnetError !== null

  const createZone = async () => {
    if (createDisabled || creatingRef.current) return
    creatingRef.current = true
    setCreating(true)
    setCreateError(null)
    try {
      const created = await createAndSelectZone(
        { name: normalizedName, subnet: normalizedSubnet, internal },
        selectedZoneIds,
        onCreate,
        onChange,
      )
      setCreatedZones((current) => mergeOrdinaryZones(current, [created]))
      setName('')
      setSubnet('')
      setInternal(false)
    } catch (error) {
      setCreateError(error instanceof Error ? error.message : 'Unable to create Zone')
    } finally {
      creatingRef.current = false
      setCreating(false)
    }
  }

  return (
    <div className="flex flex-col gap-4">
      <fieldset className="flex flex-col gap-2" disabled={disabled || creating} aria-busy={creating}>
        <legend className="text-xs font-medium text-muted-foreground">Zones</legend>
        {availableZones.length === 0 ? (
          <p className="text-xs text-muted-foreground">No ordinary Environment Zones exist yet.</p>
        ) : (
          <div className="grid gap-2 sm:grid-cols-2">
            {availableZones.map((zone) => (
              <label
                key={zone.id}
                className="flex items-start gap-2 rounded-lg border border-border bg-background px-3 py-2 text-sm"
              >
                <Checkbox

                  className="mt-0.5 accent-primary"
                  checked={selectedZoneIds.includes(zone.id)}
                  onChange={(event) => onChange(toggleSelectedZone(selectedZoneIds, zone.id, event.target.checked))}
                />
                <span className="min-w-0">
                  <span className="block truncate font-medium text-foreground">{zone.name}</span>
                  <span className="block truncate font-mono text-xs text-muted-foreground">{zone.subnet}</span>
                  <span className="block text-xs text-muted-foreground">
                    {zone.internal ? 'internal' : 'egress allowed'}
                  </span>
                </span>
              </label>
            ))}
          </div>
        )}
      </fieldset>

      <div className="rounded-lg border border-border bg-surface p-3">
        <p className="text-sm font-medium text-foreground">Create a Zone</p>
        <p className="mt-1 text-xs text-muted-foreground">
          Creating a Zone does not enable the Component. The Zone remains if enabling is cancelled or fails.
        </p>
        <fieldset className="mt-3 grid gap-3 sm:grid-cols-2" disabled={disabled || creating}>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor={`${id}-name`}>Zone name</Label>
            <Input
              id={`${id}-name`}
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder="frontend"
              aria-invalid={nameError !== null}
              aria-describedby={`${id}-name-help`}
            />
            <p
              id={`${id}-name-help`}
              className={nameError ? 'text-xs text-destructive' : 'text-xs text-muted-foreground'}
            >
              {nameError ?? 'Must be a valid Docker Compose network key.'}
            </p>
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor={`${id}-subnet`}>Subnet</Label>
            <Input
              id={`${id}-subnet`}
              value={subnet}
              onChange={(event) => setSubnet(event.target.value)}
              placeholder="10.200.20.0/24"
              aria-invalid={subnetError !== null}
              aria-describedby={`${id}-subnet-help`}
            />
            <p
              id={`${id}-subnet-help`}
              className={subnetError ? 'text-xs text-destructive' : 'text-xs text-muted-foreground'}
            >
              {subnetError ?? `Must be inside ${networkPool} and disjoint from every existing Zone.`}
            </p>
          </div>
          <label className="flex items-center gap-2 text-sm sm:col-span-2">
            <Checkbox

              className="accent-primary"
              checked={internal}
              onChange={(event) => setInternal(event.target.checked)}
            />
            <span className="text-muted-foreground">internal network — no egress, no published ports</span>
          </label>
        </fieldset>
        {createError && <p className="mt-3 text-sm text-destructive" role="alert">{createError}</p>}
        <div className="mt-3 flex justify-end">
          <Button type="button" size="sm" disabled={createDisabled} onClick={() => void createZone()}>
            {creating ? 'Creating…' : 'Create and select Zone'}
          </Button>
        </div>
      </div>
    </div>
  )
}
