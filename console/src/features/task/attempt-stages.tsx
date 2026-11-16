import { formatTimestamp } from "@/lib/format-timestamp";
import { useEffect, useState } from "react";
import { Disclosure } from "@/components/common/disclosure";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { requestTaskStages, type TaskStages } from "./api";
import { taskDuration } from "./task-duration";

export function AttemptStages({
  taskId,
  workId,
  attemptId,
  revision,
}: {
  taskId: string;
  workId: string;
  attemptId: string;
  revision: number;
}) {
  const [value, setValue] = useState<TaskStages | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [reload, setReload] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    setError(null);
    void requestTaskStages(
      taskId,
      workId,
      attemptId,
      revision,
      controller.signal,
    )
      .then((result) => {
        if (!controller.signal.aborted) setValue(result);
      })
      .catch((cause: unknown) => {
        if (!controller.signal.aborted)
          setError(
            cause instanceof Error
              ? cause.message
              : "Unable to load stage evidence",
          );
      });
    return () => controller.abort();
  }, [taskId, workId, attemptId, revision, reload]);
  return (
    <div className="space-y-3 pt-3">
      {value?.incomplete && !value.expired && (
        <p role="status" className="text-sm text-muted-foreground">
          Some execution details could not be saved. Stages below may be
          incomplete; use the attempt outcome to determine whether the operation
          finished.
        </p>
      )}
      {error ? (
        <div role="alert" className="space-y-2 text-sm text-destructive">
          <p>{error}</p>
          <Button
            size="sm"
            variant="outline"
            onClick={() => setReload((count) => count + 1)}
          >
            Retry loading
          </Button>
        </div>
      ) : !value ? (
        <p role="status" className="text-sm text-muted-foreground">
          Loading stages…
        </p>
      ) : value.expired ? (
        <p className="text-sm text-muted-foreground">
          Stage details expired under diagnostic retention. The recorded attempt
          outcome and recovery state are unchanged.
        </p>
      ) : !value.available ? (
        <p className="text-sm text-muted-foreground">
          Stage diagnostics are unavailable. This does not mean no effects
          occurred.
        </p>
      ) : (
        <div className="space-y-3">
          {value.items.length === 0 && (
            <p className="text-sm text-muted-foreground">
              No stage observations are retained.
            </p>
          )}
          <ol className="divide-y divide-border border-l-2 border-border pl-4">
            {value.items.map((stage) => (
              <StageRow key={stage.key} stage={stage} />
            ))}
          </ol>
        </div>
      )}
    </div>
  );
}

function StageRow({ stage }: { stage: TaskStages["items"][number] }) {
  const needsAttention = stage.state === "failed" || stage.state === "unknown";
  const [open, setOpen] = useState(needsAttention);
  useEffect(() => {
    if (needsAttention) setOpen(true);
  }, [needsAttention]);
  const duration =
    stage.started_at && stage.finished_at
      ? taskDuration(
          (Date.parse(stage.finished_at) - Date.parse(stage.started_at)) / 1000,
        )
      : null;
  return (
    <li className="min-w-0 py-3">
      <Disclosure
        open={open}
        onOpenChange={setOpen}
        summaryClassName="items-start"
        summary={
          <span className="min-w-0 flex-1 space-y-1">
            <span className="flex flex-wrap items-center justify-between gap-2">
              <span className="text-sm font-medium">{stage.name}</span>
              <span className="flex items-center gap-2">
                {duration && (
                  <span className="text-xs text-muted-foreground">
                    {duration}
                  </span>
                )}
                <StatusBadge status={stage.state} />
              </span>
            </span>
            {!open && stage.observed && (
              <span className="block line-clamp-1 text-xs font-normal text-muted-foreground">
                {stage.observed}
              </span>
            )}
          </span>
        }
      >
        <dl className="space-y-3 pt-3 text-sm">
          <div>
            <dt className="text-xs text-muted-foreground">Expected result</dt>
            <dd className="break-words [overflow-wrap:anywhere]">
              {stage.expected}
            </dd>
          </div>
          <div>
            <dt className="text-xs text-muted-foreground">Result</dt>
            <dd className="break-words [overflow-wrap:anywhere]">
              {stage.observed || "No result recorded yet"}
            </dd>
          </div>
          {stage.failure_code && (
            <div>
              <dt className="text-xs text-muted-foreground">Error code</dt>
              <dd className="break-words text-destructive">
                {stage.failure_code}
              </dd>
            </div>
          )}
          {stage.started_at && (
            <div>
              <dt className="text-xs text-muted-foreground">
                Recorded stage start
              </dt>
              <dd>
                <time dateTime={stage.started_at}>
                  {formatTimestamp(stage.started_at, "Not recorded", {
                    seconds: true,
                  })}
                </time>
              </dd>
            </div>
          )}
          {stage.finished_at && (
            <div>
              <dt className="text-xs text-muted-foreground">
                Recorded stage completion
              </dt>
              <dd>
                <time dateTime={stage.finished_at}>
                  {formatTimestamp(stage.finished_at, "Not recorded", {
                    seconds: true,
                  })}
                </time>
              </dd>
            </div>
          )}
        </dl>
      </Disclosure>
    </li>
  );
}
