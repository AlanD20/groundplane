import { imageReferences, type HostImage } from "@/features/image-delivery/api";
import { selectedImageReferences } from "@/features/image-delivery/image-presentation";
import { repositoryName } from "@/features/image-delivery/image-presentation";

const digest = /@sha256:[0-9a-f]{64}$/;

function patchFetches(images: HostImage[], family: RegExp) {
  return images
    .flatMap((image) => image.fetches ?? [])
    .filter(
      (fetch) =>
        fetch.status === "completed" &&
        family.test(fetch.requested) &&
        digest.test(fetch.image),
    )
    .sort((left, right) => right.requested_at.localeCompare(left.requested_at));
}

export function managedImageReferences(
  image: HostImage,
  family: RegExp,
): string[] {
  return [
    ...new Set([
      ...imageReferences(image).filter((reference) => family.test(reference)),
      ...selectedImageReferences(image).filter((reference) =>
        family.test(reference),
      ),
      ...patchFetches([image], family).map((fetch) =>
        digest.test(fetch.requested)
          ? fetch.requested
          : fetch.requested + fetch.image.slice(fetch.image.lastIndexOf("@")),
      ),
    ]),
  ];
}

// Pair a current local tag with that same image's repository digest. Historical
// Fetch records must never override a tag that now points at different content.
export function managedUpdateReference(
  value: string,
  images: HostImage[],
  family: RegExp,
): string {
  if (!family.test(value))
    throw new Error(
      "Choose an upstream image matching the selected server version.",
    );
  if (digest.test(value)) return value;
  const canonical = (reference: string) =>
    reference
      .replace(/^(?:docker\.io|registry-1\.docker\.io)\//, "")
      .replace(/^library\//, "");
  const current = images.find((image) =>
    image.tags.some((tag) => canonical(tag) === canonical(value)),
  );
  const references =
    current?.digests.filter(
      (reference) =>
        digest.test(reference) &&
        canonical(repositoryName(reference)) ===
          canonical(repositoryName(value)),
    ) ?? [];
  const pinned =
    references.find((reference) => reference.endsWith("@" + current?.id)) ??
    references[0];
  return pinned ? value + pinned.slice(pinned.lastIndexOf("@")) : value;
}
