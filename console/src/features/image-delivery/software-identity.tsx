import { CompactReference } from "@/components/common/compact-reference";
import { cn } from "@/lib/utils";
import { softwareIdentityPresentation } from "./image-presentation";

export function SoftwareIdentity({
  artifactReference,
  artifactLabel,
  className,
  digest,
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
