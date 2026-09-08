import type { Zone } from '@/lib/types'

export type ZoneCreateInput = {
  name: string
  subnet: string
  internal: boolean
}

export function toggleSelectedZone(
  selectedZoneIds: string[],
  zoneId: string,
  checked: boolean,
): string[] {
  if (checked) {
    return selectedZoneIds.includes(zoneId) ? selectedZoneIds : [...selectedZoneIds, zoneId]
  }
  return selectedZoneIds.filter((selectedId) => selectedId !== zoneId)
}

export function mergeOrdinaryZones(zones: Zone[], createdZones: Zone[]): Zone[] {
  const merged: Zone[] = []
  const includedIds = new Set<string>()

  for (const zone of [...zones, ...createdZones]) {
    if (zone.ownerKind !== 'environment' || includedIds.has(zone.id)) continue
    includedIds.add(zone.id)
    merged.push(zone)
  }

  return merged
}

export async function createAndSelectZone(
  input: ZoneCreateInput,
  selectedZoneIds: string[],
  onCreate: (input: ZoneCreateInput) => Promise<Zone>,
  onChange: (ids: string[]) => void,
): Promise<Zone> {
  const created = await onCreate(input)
  onChange(toggleSelectedZone(selectedZoneIds, created.id, true))
  return created
}
