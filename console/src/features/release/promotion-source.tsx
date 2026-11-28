import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { readEnvironmentMetadata } from "@/features/environment/workspace-read";
import type { Environment } from "@/lib/types";
import { fetchReleaseDetail, type ReleaseDetailResponse } from "./api";

const releaseIdPattern = /^dep_[0-7][0-9A-HJKMNP-TV-Z]{25}$/;

type Source = {
  release: ReleaseDetailResponse;
  environmentName: string;
};

type Lookup = { id: string; target: string };

type LookupResult = { lookup: Lookup } & (
  { source: Source } | { error: string }
);

async function readSource(
  id: string,
  destinationId: string,
  projectId: string,
  signal: AbortSignal,
): Promise<Source> {
  const release = await fetchReleaseDetail(id, signal);
  if (release.id !== id || release.state !== "completed")
    throw new Error("Choose a completed source Release.");
  if (
    !release.local_image_id ||
    release.rollback_material?.status !== "available"
  )
    throw new Error("The source Release has no retained image to promote.");
  if (release.environment_id === destinationId)
    throw new Error(
      "Choose a Release from another Environment in this Project.",
    );
  const environment = await readEnvironmentMetadata(
    release.environment_id,
    signal,
  );
  if (
    !environment ||
    environment.id !== release.environment_id ||
    environment.projectId !== projectId ||
    environment.deletionTaskId
  )
    throw new Error(
      "Choose a Release from another Environment in this Project.",
    );
  return { release, environmentName: environment.name };
}

export function usePromotionSource(
  enabled: boolean,
  destination: Environment,
  target: string,
) {
  const { id: destinationId, projectId } = destination;
  const [draft, setDraft] = useState({ target, value: "" });
  const releaseId = draft.target === target ? draft.value : "";
  const [requested, setRequested] = useState<Lookup | null>(null);
  const lookup =
    enabled && requested?.target === target && requested.id === releaseId.trim()
      ? requested
      : null;
  const [result, setResult] = useState<LookupResult | null>(null);

  useEffect(() => {
    if (!lookup) return;
    const controller = new AbortController();
    void readSource(
      lookup.id,
      destinationId,
      projectId,
      controller.signal,
    ).then(
      (source) => {
        if (!controller.signal.aborted) setResult({ lookup, source });
      },
      (cause: unknown) => {
        if (!controller.signal.aborted)
          setResult({
            lookup,
            error:
              cause instanceof Error
                ? cause.message
                : "Unable to load the source Release.",
          });
      },
    );
    return () => controller.abort();
  }, [lookup, destinationId, projectId]);

  const settled = lookup && result?.lookup === lookup ? result : null;
  return {
    releaseId,
    valid: releaseIdPattern.test(releaseId.trim()),
    loading: Boolean(lookup && !settled),
    source: settled && "source" in settled ? settled.source : null,
    error: settled && "error" in settled ? settled.error : null,
    edit: (value: string) => {
      setDraft({ target, value });
      setRequested(null);
    },
    review: () => setRequested({ target, id: releaseId.trim() }),
    resetReview: () => setRequested(null),
  };
}

export function PromotionSourceFields({
  selection,
  serviceName,
  environmentName,
  strategy,
  sourceServiceName,
}: {
  selection: ReturnType<typeof usePromotionSource>;
  serviceName: string;
  environmentName: string;
  strategy: string;
  sourceServiceName?: string;
}) {
  const { releaseId, valid, loading, source, error, edit, review } = selection;
  const invalid = releaseId.trim() !== "" && !valid;
  return (
    <div className="flex flex-col gap-2">
      <Label htmlFor="dep-source-release">Source Release ID</Label>
      <div className="flex flex-col gap-2 sm:flex-row">
        <Input
          id="dep-source-release"
          className="min-w-0 flex-1 font-mono"
          placeholder="dep_…"
          value={releaseId}
          onChange={(event) => edit(event.target.value)}
          aria-describedby="dep-source-release-help"
          aria-invalid={invalid || undefined}
          autoComplete="off"
          spellCheck={false}
        />
        <Button
          type="button"
          variant="outline"
          disabled={!valid || loading}
          onClick={review}
        >
          {loading ? "Loading Release…" : "Review Release"}
        </Button>
      </div>
      <p
        id="dep-source-release-help"
        className={
          invalid ? "text-xs text-destructive" : "text-xs text-muted-foreground"
        }
      >
        {invalid
          ? "Enter the full Release ID beginning with dep_."
          : "Copy the ID from a completed deployment in another Environment in this Project."}
      </p>
      {error && (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      )}
      {source && (
        <dl
          aria-live="polite"
          className="grid gap-2 rounded-lg border border-border bg-surface px-3 py-2 text-sm sm:grid-cols-[max-content_minmax(0,1fr)]"
        >
          <dt className="text-muted-foreground">From</dt>
          <dd className="break-words">
            {sourceServiceName ?? source.release.service_id} ·{" "}
            {source.environmentName}
          </dd>
          <dt className="text-muted-foreground">To</dt>
          <dd className="break-words">
            {serviceName} · {environmentName}
          </dd>
          <dt className="text-muted-foreground">Image</dt>
          <dd className="break-all font-mono text-xs">
            {source.release.image}
          </dd>
          <dt className="text-muted-foreground">Image ID</dt>
          <dd className="break-all font-mono text-xs">
            {source.release.local_image_id}
          </dd>
          <dt className="text-muted-foreground">Strategy</dt>
          <dd>{strategy}</dd>
        </dl>
      )}
      <p className="text-xs text-muted-foreground">
        Promotion reuses the source image and runs the destination Service’s
        rollout and hooks. Review the destination configuration, migrations, and
        backups before deploying.
      </p>
    </div>
  );
}
