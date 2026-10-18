import { useEffect, useState } from "react";
import { Disclosure } from "@/components/common/disclosure";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { requestTaskStages, type TaskStages } from "./api";

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
  const [open, setOpen] = useState(false);
  return (
    <Disclosure
      summary="Stages and results"
      open={open}
      onOpenChange={setOpen}
      summaryClassName="text-sm font-medium"
    >
      {open && (
        <StageEvidence
          taskId={taskId}
          workId={workId}
          attemptId={attemptId}
          revision={revision}
        />
      )}
    </Disclosure>
  );
}

function StageEvidence({
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
    setValue(null);
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
      {value?.incomplete && (
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
      ) : !value.available ? (
        <p className="text-sm text-muted-foreground">
          Stage diagnostics are unavailable. This does not mean no effects
          occurred.
        </p>
      ) : (
        <ol className="space-y-3">
          {value.items.map((stage) => (
            <li
              key={stage.key}
              className="space-y-2 rounded-md border border-border p-3"
            >
              <div className="flex flex-wrap items-center justify-between gap-2">
                <h4 className="text-sm font-medium">{stage.name}</h4>
                <StatusBadge status={stage.state} />
              </div>
              <dl className="space-y-2 text-sm">
                <div>
                  <dt className="text-xs text-muted-foreground">Expected</dt>
                  <dd className="break-words [overflow-wrap:anywhere]">
                    {stage.expected}
                  </dd>
                </div>
                <div>
                  <dt className="text-xs text-muted-foreground">Observed</dt>
                  <dd className="break-words [overflow-wrap:anywhere]">
                    {stage.observed || "No result recorded yet"}
                  </dd>
                </div>
              </dl>
              {stage.failure_code && (
                <p className="break-words text-xs text-destructive">
                  Error: {stage.failure_code}
                </p>
              )}
              {stage.started_at && (
                <p className="text-xs text-muted-foreground">
                  Started {new Date(stage.started_at).toLocaleString()}
                  {stage.finished_at
                    ? ` · Finished ${new Date(stage.finished_at).toLocaleString()}`
                    : " · No completion recorded"}
                </p>
              )}
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}
