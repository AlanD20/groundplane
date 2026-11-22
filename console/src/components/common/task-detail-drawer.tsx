import { formatTimestamp } from "@/lib/format-timestamp";
("use client");

import { useEffect, useState } from "react";
import { Inspector } from "@/components/common/inspector";
import { CompactReference } from "@/components/common/compact-reference";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { requestTask, abortTask, type TaskResponse } from "@/features/task/api";
import { TaskExecution } from "@/features/task/task-execution";
import { Disclosure } from "@/components/common/disclosure";
import { taskFromAPI } from "@/features/task/journal-model";
import { taskPresentation } from "@/features/task/task-overview";
import { taskDuration } from "@/features/task/task-duration";
import { useStore } from "@/lib/store";
import { Link } from "react-router-dom";
import type {
  ActivityEntry,
  TaskJournalScope,
  TaskJournalSurface,
} from "@/lib/types";

export function TaskDetailDrawer({
  entry,
  taskId,
  onOpenChange,
}: {
  scope: TaskJournalScope;
  surface: TaskJournalSurface;
  onOpenChange: (open: boolean) => void;
} & (
  { entry: ActivityEntry; taskId?: never } | { taskId: string; entry?: never }
)) {
  const id = taskId ?? entry!.id;
  const store = useStore();
  const { watchTaskEvents } = store;
  const [detail, setDetail] = useState<TaskResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [pending, setPending] = useState(false);
  const [reload, setReload] = useState(0);
  const [page, setPage] = useState<{ after?: string; revision?: number }>({});
  useEffect(() => {
    setPage((current) => (current.after || current.revision ? {} : current));
    setDetail(null);
    setError(null);
  }, [id]);
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let stopEvents = () => {};
    let polling = false;
    let finished = false;
    let eventRefreshScheduled = false;
    const schedule = (delay: number) => {
      if (timer !== undefined) clearTimeout(timer);
      timer = setTimeout(() => void poll(), delay);
    };
    const poll = async () => {
      timer = undefined;
      eventRefreshScheduled = false;
      if (polling || finished || controller.signal.aborted) return;
      polling = true;
      try {
        const response = await requestTask(id, controller.signal, {
          limit: 25,
          ...page,
        });
        if (controller.signal.aborted) return;
        setDetail(response);
        setError(null);
        finished = !!response.task.finished_at;
        if (finished) stopEvents();
      } catch (cause) {
        if (!controller.signal.aborted)
          setError(
            cause instanceof Error ? cause.message : "Unable to load Task",
          );
      } finally {
        polling = false;
        if (!controller.signal.aborted) setLoading(false);
      }
      if (!controller.signal.aborted && !finished && !page.revision)
        schedule(2000);
    };
    setLoading(true);
    if (!page.revision) {
      stopEvents = watchTaskEvents(
        id,
        () => {
          if (polling || finished || eventRefreshScheduled) return;
          eventRefreshScheduled = true;
          schedule(200);
        },
        () => {
          // Keep the current detail visible. The fallback read handles disconnects.
        },
      );
    }
    void poll();
    return () => {
      controller.abort();
      stopEvents();
      if (timer) clearTimeout(timer);
    };
  }, [id, page, reload, watchTaskEvents]);
  const task = detail?.task.id === id ? detail.task : null;
  const presentation = task ? taskPresentation(taskFromAPI(task), store) : null;
  async function cancel() {
    setPending(true);
    setError(null);
    try {
      await abortTask(id);
      setPage({});
      setReload((value) => value + 1);
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : "Unable to request cancellation",
      );
    } finally {
      setPending(false);
    }
  }
  return (
    <Inspector
      open
      onOpenChange={onOpenChange}
      context="Task"
      title={task ? (presentation?.title ?? task.target_name) : "Task details"}
      status={task && <StatusBadge status={task.status} />}
      footer={
        <>
          <Button
            variant="outline"
            size="sm"
            onClick={() => {
              setPage({});
              setReload((value) => value + 1);
            }}
          >
            Refresh
          </Button>
          {task?.cancel.allowed && (
            <Button
              variant="destructive"
              size="sm"
              disabled={pending}
              onClick={() => void cancel()}
            >
              Request cancellation
            </Button>
          )}
          <Button
            variant="outline"
            size="sm"
            onClick={() => onOpenChange(false)}
          >
            Close
          </Button>
        </>
      }
    >
      {loading && !task && (
        <p role="status" className="text-sm text-muted-foreground">
          Loading Task…
        </p>
      )}
      {error && (
        <div role="alert" className="space-y-2 text-sm text-destructive">
          <p>{error}</p>
          <Button
            size="sm"
            variant="outline"
            onClick={() => {
              setPage({});
              setReload((value) => value + 1);
            }}
          >
            Retry loading
          </Button>
        </div>
      )}
      {task && detail && (
        <div className="min-w-0 space-y-4">
          <section className="space-y-2 rounded-lg border border-border p-4">
            <div className="grid grid-cols-4 gap-3 py-2">
              {[
                ["Completed", task.counts.succeeded],
                ["Running", task.counts.running + task.counts.assigned],
                ["Waiting", task.counts.waiting + task.counts.ready],
                [
                  "Failed / attention",
                  task.counts.failed + task.counts.attention,
                ],
              ].map(([label, count]) => (
                <div key={label}>
                  <p className="text-lg font-semibold tabular-nums">{count}</p>
                  <p className="text-xs text-muted-foreground">{label}</p>
                </div>
              ))}
            </div>
            <Disclosure
              summary="All operation counts"
              summaryClassName="text-xs text-muted-foreground"
            >
              <p className="pt-2 text-xs text-muted-foreground">
                {task.counts.succeeded} succeeded · {task.counts.failed} failed
                · {task.counts.running + task.counts.assigned} active ·{" "}
                {task.counts.waiting} waiting
                {task.counts.ready > 0 && ` · ${task.counts.ready} ready`}
                {task.counts.attention > 0 &&
                  ` · ${task.counts.attention} need attention`}
                {task.counts.cancelled > 0 &&
                  ` · ${task.counts.cancelled} cancelled`}
                {task.counts.superseded > 0 &&
                  ` · ${task.counts.superseded} superseded`}
              </p>
            </Disclosure>
            <p className="text-xs text-muted-foreground">
              {presentation?.scope} ·{" "}
              {task.finished_at ? "Finished after" : "Requested"}{" "}
              {taskDuration(
                Math.max(
                  0,
                  ((task.finished_at
                    ? Date.parse(task.finished_at)
                    : Date.now()) -
                    Date.parse(task.created_at)) /
                    1000,
                ),
              )}
              {!task.finished_at && " ago"}
            </p>
            {presentation?.destination && (
              <Link
                className="inline-block text-sm text-primary underline underline-offset-4"
                to={presentation.destination.href}
                onClick={() => onOpenChange(false)}
              >
                {presentation.destination.label}
              </Link>
            )}
            {task.stop_requested && !task.finished_at && (
              <p className="text-sm">
                Stop requested. Running effects must be accounted for before
                resource protection is released.
              </p>
            )}
            {task.status === "attention_required" && (
              <p role="status" className="text-sm text-warning">
                Work needs attention. Inspect its outcome and cleanup state
                below.
              </p>
            )}
            {!task.finished_at &&
              !task.cancel.allowed &&
              task.cancel.reason && (
                <p className="text-xs text-muted-foreground">
                  {task.cancel.reason}
                </p>
              )}
            <Disclosure
              summary="Task details"
              summaryClassName="text-xs text-muted-foreground"
            >
              <div className="space-y-3 pt-3">
                <CompactReference value={task.id} label="Task" />
                <dl className="grid grid-cols-2 gap-3 text-xs">
                  <div>
                    <dt className="text-muted-foreground">Created</dt>
                    <dd>
                      {formatTimestamp(task.created_at, "Not recorded", {
                        seconds: true,
                      })}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-muted-foreground">
                      {task.finished_at ? "Finished" : "Deadline"}
                    </dt>
                    <dd>
                      {formatTimestamp(
                        task.finished_at || task.deadline,
                        "Not recorded",
                        { seconds: true },
                      )}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-muted-foreground">Requested by</dt>
                    <dd>{task.actor}</dd>
                  </div>
                </dl>
              </div>
            </Disclosure>
          </section>
          {task.history_expired ? (
            <p className="text-sm text-muted-foreground">
              Execution details are no longer retained.
            </p>
          ) : (
            <>
              <TaskExecution
                key={task.id}
                detail={detail}
                onChanged={() => {
                  setPage({});
                  setReload((value) => value + 1);
                }}
              />
              <div className="flex gap-2">
                {page.after && (
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => setPage({})}
                  >
                    First work items
                  </Button>
                )}
                {detail.work.more && (
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() =>
                      setPage({
                        after: detail.work.after,
                        revision: detail.work.revision,
                      })
                    }
                  >
                    Next work items
                  </Button>
                )}
              </div>
              {page.revision && (
                <p className="text-xs text-muted-foreground">
                  Browsing a fixed snapshot. Refresh to see current execution.
                </p>
              )}
            </>
          )}
        </div>
      )}
    </Inspector>
  );
}
