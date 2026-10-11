import { CompactReference } from "@/components/common/compact-reference";
import { cn } from "@/lib/utils";
import { softwareIdentityPresentation } from "./image-presentation";
import type { components } from "@/lib/api.generated";

export function ContainerSoftwareIdentity({
  software,
  className,
  showReferences = true,
  unavailable,
}: {
  software?: components["schemas"]["ContainerIdentity"] | null;
  className?: string;
  showReferences?: boolean;
  unavailable?: string;
}) {
  return (
    <SoftwareIdentity
      artifactReference={software?.image}
      digest={software?.config_digest}
      version={software?.version}
      selectedTag={software?.selected_tag}
      sourceBuild={software?.source?.kind === "source_ref"}
      sourceCommit={software?.source?.commit}
      className={className}
      showReferences={showReferences}
      unavailable={unavailable}
    />
  );
}

export function SoftwareIdentity({
  artifactReference,
  artifactLabel,
  className,
  digest,
  selectedTag,
  showReferences = true,
  sourceBuild = false,
  sourceCommit,
  unavailable,
  version,
}: {
  artifactReference?: string | null;
  artifactLabel?: string;
  className?: string;
  digest?: string | null;
  selectedTag?: string | null;
  showReferences?: boolean;
  sourceBuild?: boolean;
  sourceCommit?: string | null;
  unavailable?: string;
  version?: string | null;
}) {
  const identity = softwareIdentityPresentation({
    artifactReference,
    artifactLabel,
    digest,
    selectedTag,
    sourceBuild,
    sourceCommit,
    unavailable,
    version,
  });

  return (
    <span className={cn("flex min-w-0 flex-col items-start gap-1", className)}>
      <span
        className={cn(
          "max-w-full break-words",
          identity.available ? "font-medium" : "text-muted-foreground",
        )}
      >
        {identity.title}
        {identity.qualifier ? (
          <span className="ml-1 font-normal text-muted-foreground">
            · {identity.qualifier}
          </span>
        ) : null}
      </span>
      {showReferences
        ? identity.references.map((reference) => (
            <CompactReference
              key={`${reference.label}:${reference.value}`}
              label={reference.label}
              short={reference.short}
              value={reference.value}
            />
          ))
        : null}
    </span>
  );
}
