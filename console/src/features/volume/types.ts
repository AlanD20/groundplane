import type { operations } from '@/lib/api.generated'

export type Volume = {
  id: string
  environmentId: string
  slug: string
  key: string
  path?: string
  state?: 'creating' | 'active' | 'create_failed' | 'deleting'
  createTaskId?: string
  originTaskId?: string
  currentTaskId?: string
}

type VolumeDeletionImpactWire = operations['volume.removal-impact']['responses'][200]['content']['application/json']

export type VolumeDeletionImpactItem = NonNullable<NonNullable<VolumeDeletionImpactWire['items']>[number]>
export type VolumeDeletionImpactPage = Omit<VolumeDeletionImpactWire, 'items'> & {
  items: VolumeDeletionImpactItem[]
}
