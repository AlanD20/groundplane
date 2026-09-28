import type { operations } from '@/lib/api.generated'
import { controllerRequest } from '@/lib/controller-json-request'

export type ImageInventory = operations['image.list']['responses'][200]['content']['application/json']
export type HostImage = ImageInventory['images'][number]

export function listImages(signal?: AbortSignal) {
  return controllerRequest<ImageInventory>('/images', 200, { signal })
}

export function removeImage(id: string, key: string) {
  return controllerRequest<operations['image.remove']['responses'][202]['content']['application/json']>(`/images/${encodeURIComponent(id)}`, 202, { method: 'DELETE', idempotencyKey: key })
}

export function imageReferences(image: HostImage): string[] {
  return [...image.digests, ...image.tags]
}

export function imageSize(bytes: number): string {
  return bytes >= 1024 ** 3 ? `${(bytes / 1024 ** 3).toFixed(2)} GiB` : `${(bytes / 1024 ** 2).toFixed(1)} MiB`
}
