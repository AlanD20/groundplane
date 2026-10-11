import { imageReferences, type HostImage } from "./api";

export function repositoryName(reference: string): string {
  if (isDigestReference(reference) && !reference.includes("@")) return "";
  const name = reference.split("@")[0];
  const colon = name.lastIndexOf(":");
  return colon > name.lastIndexOf("/") ? name.slice(0, colon) : name;
}

export function imageRepositories(image: HostImage): string[] {
  return [
    ...new Set(imageReferences(image).map(repositoryName).filter(Boolean)),
  ];
}

export function imageName(image: HostImage): string {
  return (
    imageCurrentTags(image)[0] ?? shortImageId(preferredImageDigest(image))
  );
}

export function imageTag(reference: string): string {
  const colon = reference.lastIndexOf(":");
  return colon > reference.lastIndexOf("/")
    ? reference.slice(colon + 1)
    : reference;
}

export function shortImageId(id: string): string {
  const digest = id.match(/(?:^|@)sha256:([a-f0-9]+)/i);
  return (digest?.[1] ?? id.replace("sha256:", "")).slice(0, 12);
}

export function compactDigest(reference: string): string {
  const digest = reference.match(/(?:^|@)sha256:([a-f0-9]+)/i);
  return digest ? `sha256:${digest[1].slice(0, 12)}` : shortImageId(reference);
}

export function isDigestReference(reference: string): boolean {
  return /(?:^|@)sha256:[a-f0-9]{64}$/i.test(reference.trim());
}

export function imageCurrentTags(image: HostImage): string[] {
  return [...new Set(image.tags)].sort((left, right) =>
    left.localeCompare(right),
  );
}

export function preferredImageDigest(image: HostImage): string {
  return (
    [...new Set(image.digests)].sort((left, right) =>
      left.localeCompare(right),
    )[0] ?? image.id
  );
}

export function previousFetchReference(image: HostImage): string | null {
  const currentTags = new Set(imageCurrentTags(image));
  return (
    image.fetches.find(
      (fetch) =>
        fetch.status === "completed" &&
        !isDigestReference(fetch.requested) &&
        !currentTags.has(fetch.requested),
    )?.requested ?? null
  );
}

export type ImageChoice = {
  label: string;
  references: string[];
  value: string;
};

export type SoftwareIdentityReference = {
  label: string;
  short?: string;
  value: string;
};

export type SoftwareIdentityPresentation = {
  available: boolean;
  qualifier?: string;
  references: SoftwareIdentityReference[];
  title: string;
};

export function softwareIdentityPresentation({
  artifactReference,
  artifactLabel = "Registry output",
  digest,
  selectedTag,
  sourceBuild = false,
  sourceCommit,
  unavailable = "Identity not reported by the Controller",
  version,
}: {
  artifactReference?: string | null;
  artifactLabel?: string;
  digest?: string | null;
  selectedTag?: string | null;
  sourceBuild?: boolean;
  sourceCommit?: string | null;
  unavailable?: string;
  version?: string | null;
}): SoftwareIdentityPresentation {
  const recordedVersion = version?.trim() ?? "";
  // Only an explicitly recorded commit is presentation authority. Release
  // builds keep it in details; source builds lead with it rather than a digest.
  const recordedCommit = sourceCommit?.trim() ?? "";
  const recordedDigest = digest?.trim() ?? "";
  const recordedArtifact = artifactReference?.trim() ?? "";
  const recordedTag = selectedTag?.trim() ?? "";
  const immutableArtifact = isDigestReference(recordedArtifact)
    ? recordedArtifact
    : "";
  const fallbackDigest = recordedDigest || immutableArtifact;
  const references: SoftwareIdentityReference[] = [];

  if (recordedTag && immutableArtifact) {
    references.push({
      label: "Installed from tag",
      value: `${immutableArtifact.slice(0, immutableArtifact.lastIndexOf("@"))}:${recordedTag}`,
    });
  }

  if (recordedCommit) {
    references.push({
      label: "Source commit",
      short: recordedCommit.slice(0, 12),
      value: recordedCommit,
    });
  }
  if (recordedArtifact) {
    references.push({
      label: artifactLabel,
      short: isDigestReference(recordedArtifact)
        ? compactDigest(recordedArtifact)
        : undefined,
      value: recordedArtifact,
    });
  }
  if (
    recordedDigest &&
    !references.some((reference) => reference.value === recordedDigest)
  ) {
    references.push({
      label: "Digest",
      short: compactDigest(recordedDigest),
      value: recordedDigest,
    });
  }

  if (sourceBuild && recordedCommit) {
    return {
      available: true,
      qualifier: "Source build",
      references,
      title: recordedCommit.slice(0, 12),
    };
  }
  if (recordedTag || recordedVersion) {
    return {
      available: true,
      references,
      title: recordedTag || recordedVersion,
    };
  }
  if (recordedCommit) {
    return {
      available: true,
      qualifier: "Source build",
      references,
      title: recordedCommit.slice(0, 12),
    };
  }
  if (fallbackDigest) {
    return {
      available: true,
      references,
      title: compactDigest(fallbackDigest),
    };
  }
  return { available: false, references, title: unavailable };
}

// A picker offers one choice per immutable local image. Current Docker tags are
// real selectable names; retained Fetch requests only explain untagged content.
export function imageChoice(
  image: HostImage,
  references: string[],
): ImageChoice | null {
  const available = [
    ...new Set(references.map((reference) => reference.trim()).filter(Boolean)),
  ];
  if (available.length === 0) return null;

  const currentTags = imageCurrentTags(image).filter((tag) =>
    available.includes(tag),
  );
  const previous = previousFetchReference(image);
  const previousFetch = previous
    ? image.fetches.find(
        (fetch) => fetch.status === "completed" && fetch.requested === previous,
      )
    : undefined;
  const digestSeparator = previousFetch?.image.lastIndexOf("@") ?? -1;
  const digestSuffix =
    previousFetch && digestSeparator >= 0
      ? previousFetch.image.slice(digestSeparator)
      : undefined;
  const historicalReference =
    previousFetch && digestSuffix
      ? available.find(
          (reference) =>
            reference === previousFetch.image ||
            reference === `${previousFetch.requested}${digestSuffix}`,
        )
      : undefined;
  const value =
    currentTags[0] ??
    historicalReference ??
    available.find(isDigestReference) ??
    available[0];
  const compactDigest = shortImageId(
    isDigestReference(value) ? value : preferredImageDigest(image),
  );
  const label =
    currentTags.length > 0
      ? `${currentTags.join(" · ")} · ${compactDigest}`
      : previous
        ? `${compactDigest} · Previously fetched as ${previous}`
        : `${compactDigest} · Digest only`;

  return { label, references: available, value };
}
