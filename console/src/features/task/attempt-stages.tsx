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
          {value.updated_at && (
            <p className="text-xs text-muted-foreground">
              Last recorded update{" "}
              <time dateTime={value.updated_at}>
                {new Date(value.updated_at).toLocaleString()}
              </time>
            </p>
          )}
          {value.items.length === 0 && (
            <p className="text-sm text-muted-foreground">
              No stage observations are retained.
            </p>
          )}
          <ol className="space-y-2">
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
  const duration =
    stage.started_at && stage.finished_at
      ? taskDuration(
          (Date.parse(stage.finished_at) - Date.parse(stage.started_at)) / 1000,
        )
      : null;
  return (
    <li className="min-w-0 rounded-md border border-border p-3">
      <Disclosure
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
            <span className="block break-words text-sm [overflow-wrap:anywhere]">
              {stage.observed || stage.expected}
            </span>
          </span>
        }
      >
        <dl className="space-y-3 pt-3 text-sm">
          <div>
            <dt className="text-xs text-muted-foreground">Expected</dt>
            <dd className="break-words [overflow-wrap:anywhere]">
              {stage.expected}
            </dd>
          </div>
          <div>
            <dt className="text-xs text-muted-foreground">Recorded result</dt>
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
                  {new Date(stage.started_at).toLocaleString()}
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
                  {new Date(stage.finished_at).toLocaleString()}
                </time>
              </dd>
            </div>
          )}
        </dl>
      </Disclosure>
    </li>
  );
}
