import type { operations } from '@/lib/api.generated'
import type { Volume, VolumeDeletionImpactPage } from './types'

export type VolumeRequest = <Response>(
  path: string,
  expectedStatus: number,
  init?: {
    method?: 'GET' | 'POST' | 'PATCH' | 'PUT' | 'DELETE'
    body?: unknown
    signal?: AbortSignal
    idempotencyKey?: string
  },
) => Promise<Response>

type VolumeResponse = operations['volume.show']['responses'][200]['content']['application/json']
type VolumePageResponse = operations['volume.list']['responses'][200]['content']['application/json']
type VolumeMutationResponse = operations['volume.create']['responses'][201]['content']['application/json']
export type VolumeImpactPageResponse = operations['volume.removal-impact']['responses'][200]['content']['application/json']

function volumeFromAPI(volume: VolumeResponse): Volume {
  const state = volumeState(volume.state)
  return {
    id: volume.id,
    environmentId: volume.environment_id,
    slug: volume.slug,
    key: volume.key,
    path: volume.path,
    state,
    createTaskId: volume.create_task_id,
    originTaskId: volume.origin_task_id,
    currentTaskId: volume.current_task_id,
  }
}

function volumeState(value: string | undefined): Volume['state'] {
  if (value === undefined) return undefined
  switch (value) {
    case 'creating':
    case 'active':
    case 'create_failed':
    case 'deleting':
      return value
    default:
      throw new Error(`Controller returned unknown Volume state ${value}`)
  }
}

export async function listAllVolumes(request: VolumeRequest, environmentId: string, signal?: AbortSignal): Promise<Volume[]> {
  const volumes: Volume[] = []
  let cursor = ''
  do {
    const query = new URLSearchParams({
      environment: environmentId,
      limit: '200',
    })
    if (cursor) query.set('cursor', cursor)
    const page = await request<VolumePageResponse>(`/volumes?${query}`, 200, {
      signal,
    })
    volumes.push(...(page.items ?? []).map(volumeFromAPI))
    cursor = page.next_cursor ?? ''
  } while (cursor)
  return volumes
}

export async function createVolume(request: VolumeRequest, environmentId: string, input: { slug: string; key?: string }): Promise<Volume> {
  const response = await request<VolumeMutationResponse>('/volumes', 201, {
    method: 'POST',
    body: {
      environment_id: environmentId,
      slug: input.slug,
      ...(input.key ? { key: input.key } : {}),
    },
  })
  return volumeFromAPI(response.volume)
}

export async function getVolume(request: VolumeRequest, volumeId: string): Promise<Volume> {
  const response = await request<VolumeResponse>(`/volumes/${encodeURIComponent(volumeId)}`, 200)
  return volumeFromAPI(response)
}

export async function editVolume(request: VolumeRequest, volumeId: string, slug: string): Promise<Volume> {
  const response = await request<VolumeMutationResponse>(`/volumes/${encodeURIComponent(volumeId)}`, 200, { method: 'PATCH', body: { slug } })
  return volumeFromAPI(response.volume)
}

export async function getVolumeDeletionImpact(request: VolumeRequest, volumeId: string, cursor = '', limit = 40): Promise<VolumeDeletionImpactPage> {
  const query = new URLSearchParams({ limit: String(limit) })
  if (cursor) query.set('cursor', cursor)
  const response = await request<VolumeImpactPageResponse>(
    `/volumes/${encodeURIComponent(volumeId)}/deletion-impact?${query.toString()}`,
    200,
  )
  if (response.items === null || response.items === undefined) {
    throw new Error('Controller returned a Volume deletion-impact page without items')
  }
  const items = []
  for (const item of response.items) {
    if (item === null) throw new Error('Controller returned a null Volume deletion-impact item')
    items.push(item)
  }
  return { ...response, items }
}

export async function removeVolume(request: VolumeRequest, volumeId: string, impactToken: string, confirmKey: string): Promise<string> {
  const query = new URLSearchParams({
    impact_token: impactToken,
    confirm_key: confirmKey,
  })
  const response = await request<{ task_id: string }>(`/volumes/${encodeURIComponent(volumeId)}?${query.toString()}`, 202, { method: 'DELETE' })
  if (!response.task_id) throw new Error('Controller did not return a removal task')
  return response.task_id
}
