import { useEffect, useState } from "react";
import { Disclosure } from "@/components/common/disclosure";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { requestTaskAttemptEvents, type TaskAttemptEvents } from "./api";

export function AttemptEvents({
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
  const [value, setValue] = useState<TaskAttemptEvents | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [reload, setReload] = useState(0);
  useEffect(() => {
    if (!open) return;
    const controller = new AbortController();
    setValue(null);
    setError(null);
    void requestTaskAttemptEvents(
      taskId,
      workId,
      attemptId,
      revision,
      0,
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
              : "Unable to read execution events",
          );
      });
    return () => controller.abort();
  }, [open, taskId, workId, attemptId, revision, reload]);
  return (
    <Disclosure
      summary="Recorded events"
      open={open}
      onOpenChange={setOpen}
      summaryClassName="text-sm font-medium"
    >
      {open && (
        <div className="space-y-3 pt-3">
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
              Loading events…
            </p>
          ) : (
            <>
              {value.finished_at && (
                <p className="text-sm">
                  Attempt finished: <StatusBadge status={value.outcome} />
                </p>
              )}
              {(value.expired || value.first > 1) && (
                <p className="text-sm text-muted-foreground">
                  Earlier events are no longer retained. This is not a complete
                  execution history.
                </p>
              )}
              {value.incomplete && (
                <p className="text-sm text-muted-foreground">
                  Some diagnostics could not be saved. The attempt outcome is
                  recorded separately.
                </p>
              )}
              {value.items.length === 0 ? (
                <p className="text-sm text-muted-foreground">
                  No events are retained for this attempt. This does not mean no
                  work occurred.
                </p>
              ) : (
                <ol className="space-y-3">
                  {value.items.map((event) => (
                    <li
                      key={event.sequence}
                      className="space-y-1 border-l border-border pl-3 text-sm"
                    >
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <span className="font-medium">{event.stage.name}</span>
                        <StatusBadge status={event.stage.state} />
                      </div>
                      <time
                        dateTime={event.recorded_at}
                        className="text-xs text-muted-foreground"
                      >
                        {new Date(event.recorded_at).toLocaleString()}
                      </time>
                      {event.stage.observed && (
                        <p className="break-words [overflow-wrap:anywhere]">
                          {event.stage.observed}
                        </p>
                      )}
                      {event.stage.failure_code && (
                        <p className="text-xs text-destructive">
                          {event.stage.failure_code}
                        </p>
                      )}
                    </li>
                  ))}
                </ol>
              )}
            </>
          )}
        </div>
      )}
    </Disclosure>
  );
}
