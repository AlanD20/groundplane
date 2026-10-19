import { useEffect, useState } from "react";
import { Disclosure } from "@/components/common/disclosure";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { CopyButton } from "@/components/common/copy-button";
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
  const [snapshot, setSnapshot] = useState(revision);
  const [loading, setLoading] = useState(false);
  const [query, setQuery] = useState("");
  const [state, setState] = useState("all");
  const search = query.trim().toLowerCase();
  const events = (value?.items ?? []).filter(
    (event) =>
      (state === "all" || event.stage.state === state) &&
      [
        event.stage.name,
        event.stage.expected,
        event.stage.observed,
        event.stage.failure_code,
      ].some((text) => text?.toLowerCase().includes(search)),
  );
  useEffect(() => {
    if (!open) return;
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    void requestTaskAttemptEvents(
      taskId,
      workId,
      attemptId,
      snapshot,
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
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [open, taskId, workId, attemptId, snapshot, reload]);
  return (
    <Disclosure
      summary="Recorded events"
      open={open}
      onOpenChange={(nextOpen) => {
        if (nextOpen) {
          setSnapshot(revision);
        }
        setOpen(nextOpen);
      }}
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
              <div className="flex flex-wrap items-center gap-2">
                <Input
                  aria-label="Search retained execution events"
                  placeholder="Search execution events…"
                  className="min-w-40 flex-1"
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                />
                <Select
                  aria-label="Filter execution event state"
                  className="w-40"
                  value={state}
                  onValueChange={setState}
                  options={[
                    { value: "all", label: "All states" },
                    { value: "running", label: "Running" },
                    { value: "failed", label: "Failed" },
                    { value: "succeeded", label: "Succeeded" },
                    { value: "waiting", label: "Waiting" },
                    { value: "skipped", label: "Skipped" },
                    { value: "unknown", label: "Unknown" },
                  ]}
                />
                <CopyButton
                  label="Copy visible execution events"
                  value={events
                    .map(
                      (event) =>
                        `${event.recorded_at} ${event.stage.name} [${event.stage.state}] ${event.stage.observed || event.stage.expected}${event.stage.failure_code ? ` (${event.stage.failure_code})` : ""}`,
                    )
                    .join("\n")}
                />
              </div>
              {value.finished_at && (
                <p className="text-sm">
                  Attempt finished:{" "}
                  <StatusBadge status={value.outcome ?? "unknown"} />
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
              <div className="flex flex-wrap items-center gap-2">
                <p className="text-xs text-muted-foreground">
                  Retained events at the captured revision; not following live.
                </p>
                <Button
                  size="sm"
                  variant="outline"
                  disabled={loading}
                  onClick={() => {
                    setSnapshot(revision);
                    setReload((count) => count + 1);
                  }}
                >
                  Refresh events
                </Button>
              </div>
              {value.items.length === 0 ? (
                <p className="text-sm text-muted-foreground">
                  No events are retained for this attempt. This does not mean no
                  work occurred.
                </p>
              ) : events.length === 0 ? (
                <p className="text-sm text-muted-foreground">
                  No retained events match these filters.
                </p>
              ) : (
                <ol className="space-y-3">
                  {events.map((event) => (
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
