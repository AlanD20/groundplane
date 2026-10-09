import { CompactReference } from "@/components/common/compact-reference";
import type { ActivityEntry } from "@/lib/types";
import { imageSize } from "./api";

export function ImageFetchTaskDetails({ task }: { task: ActivityEntry }) {
  const fetch = task.imageFetch;
  if (!fetch) return null;
  const progress = fetch.progress;
  const active = task.status === "pending" || task.status === "running";
  const phases: Record<string, string> = {
    checking: "Checking local image",
    downloading: "Downloading layers",
    verifying: "Verifying image",
    verified: "Image verified",
  };
  const summary =
    task.status === "completed"
      ? "Image ready"
      : task.status === "pending"
        ? "Queued"
        : task.status === "failed"
          ? "Fetch failed"
          : task.status === "aborted"
            ? "Fetch aborted"
            : task.status === "timed_out"
              ? "Fetch timed out"
              : (phases[progress.phase ?? ""] ?? "Fetching image");
  return (
    <div className="space-y-4">
      <section
        aria-live="polite"
        className="space-y-2 rounded-lg border border-border bg-surface p-4"
      >
        <p className="text-sm font-medium">{summary}</p>
        <p className="text-xs text-muted-foreground">
          Platform: {fetch.platform}
        </p>
        {active && (progress.total_bytes ?? 0) > 0 && (
          <div className="space-y-1">
            <p className="text-sm">
              {imageSize(progress.downloaded_bytes ?? 0)} /{" "}
              {imageSize(progress.total_bytes ?? 0)} downloaded
            </p>
            <p className="text-xs text-muted-foreground">
              Layer count updates as layers are discovered.
            </p>
          </div>
        )}
        {!active && progress.phase && task.status !== "completed" && (
          <p className="text-xs text-muted-foreground">
            Last stage: {phases[progress.phase] ?? progress.phase}
          </p>
        )}
      </section>
      {progress.error_detail && (
        <div
          role="alert"
          className="space-y-2 rounded-lg border border-destructive/40 bg-destructive/5 p-4 text-sm"
        >
          <p className="font-medium text-destructive">
            {progress.error_detail}
          </p>
          {progress.error_code && (
            <p className="text-xs text-muted-foreground">
              Error code: {progress.error_code}
            </p>
          )}
          <p className="text-xs text-muted-foreground">
            Retry uses the same digest. Fetch again to resolve a newer tag.
          </p>
        </div>
      )}
      {task.status === "failed" && !progress.error_detail && (
        <p className="text-sm text-muted-foreground">
          No failure explanation was recorded for this attempt. The Controller
          log contains its diagnostic.
        </p>
      )}
      <div className="flex flex-wrap gap-3">
        <CompactReference label="Requested image" value={fetch.requested} />
        <CompactReference label="Digest" value={fetch.image} />
      </div>
    </div>
  );
}
