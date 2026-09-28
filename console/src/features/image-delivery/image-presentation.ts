import { imageReferences, type HostImage } from './api'

export function repositoryName(reference: string): string {
  const name = reference.split('@')[0]
  const colon = name.lastIndexOf(':')
  return colon > name.lastIndexOf('/') ? name.slice(0, colon) : name
}

export function imageRepositories(image: HostImage): string[] {
  return [...new Set([...imageReferences(image), ...image.fetches.map(fetch => fetch.requested)].map(repositoryName))]
}

export function imageName(image: HostImage): string {
  return imageRepositories(image)[0]?.split('/').at(-1) ?? 'Unnamed image'
}

export function imageTag(reference: string): string {
  const colon = reference.lastIndexOf(':')
  return colon > reference.lastIndexOf('/') ? reference.slice(colon + 1) : reference
}

export function shortImageId(id: string): string {
  return id.replace('sha256:', '').slice(0, 12)
}
