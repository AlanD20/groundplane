import { imageReferences, type HostImage } from '@/features/image-delivery/api';

const digest = /@sha256:[0-9a-f]{64}$/;

function patchFetches(images: HostImage[], family: RegExp) {
  return images.flatMap(image => image.fetches ?? [])
    .filter(fetch => fetch.status === 'completed' && family.test(fetch.requested) && digest.test(fetch.image))
    .sort((left, right) => right.requested_at.localeCompare(left.requested_at));
}

export function managedImageReferences(image: HostImage, family: RegExp): string[] {
  return [...new Set([
    ...imageReferences(image).filter(reference => family.test(reference)),
    ...patchFetches([image], family).map(fetch => digest.test(fetch.requested) ? fetch.requested : fetch.requested + fetch.image.slice(fetch.image.lastIndexOf('@'))),
  ])];
}

// Fetch records tie the selected upstream family tag to its exact downloaded
// bytes. Do not silently select another patch or use the mutable tag as authority.
export function managedUpdateReference(value: string, images: HostImage[], family: RegExp): string {
  if (!family.test(value)) throw new Error('Choose an upstream image matching the selected server version.')
  if (digest.test(value)) return value;
  const canonical = value.replace(/^(?:(?:docker\.io|registry-1\.docker\.io)\/)?library\//, '')
    .replace(/^(?:docker\.io|registry-1\.docker\.io)\//, '');
  const fetch = patchFetches(images, family).find(candidate => candidate.requested
    .replace(/^(?:(?:docker\.io|registry-1\.docker\.io)\/)?library\//, '')
    .replace(/^(?:docker\.io|registry-1\.docker\.io)\//, '') === canonical);
  return fetch ? value + fetch.image.slice(fetch.image.lastIndexOf('@')) : value;
}
