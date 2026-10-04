import { imageReferences, type HostImage } from '@/features/image-delivery/api';

const family = /^(?:(?:docker\.io|registry-1\.docker\.io)\/)?(?:library\/)?postgres:16(?:\.[0-9]+)?-alpine(?:[0-9]+\.[0-9]+)?(?:@sha256:[0-9a-f]{64})?$/;
const digest = /@sha256:[0-9a-f]{64}$/;

function patchFetches(images: HostImage[]) {
  return images.flatMap(image => image.fetches ?? [])
    .filter(fetch => fetch.status === 'completed' && family.test(fetch.requested) && digest.test(fetch.image))
    .sort((left, right) => right.requested_at.localeCompare(left.requested_at));
}

export function postgresImageReferences(image: HostImage): string[] {
  return [...new Set([
    ...imageReferences(image).filter(reference => family.test(reference)),
    ...patchFetches([image]).map(fetch => fetch.requested + fetch.image.slice(fetch.image.lastIndexOf('@'))),
  ])];
}

// Fetch records tie the selected upstream family tag to its exact downloaded
// bytes. Do not silently select another patch or use the mutable tag as authority.
export function postgresUpdateReference(value: string, images: HostImage[]): string {
  if (digest.test(value)) return value;
  const canonical = value.replace(/^(?:(?:docker\.io|registry-1\.docker\.io)\/)?library\//, '')
    .replace(/^(?:docker\.io|registry-1\.docker\.io)\//, '');
  const fetch = patchFetches(images).find(candidate => candidate.requested
    .replace(/^(?:(?:docker\.io|registry-1\.docker\.io)\/)?library\//, '')
    .replace(/^(?:docker\.io|registry-1\.docker\.io)\//, '') === canonical);
  return fetch ? value + fetch.image.slice(fetch.image.lastIndexOf('@')) : value;
}
