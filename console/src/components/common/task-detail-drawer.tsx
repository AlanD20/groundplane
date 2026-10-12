"use client";

import { Inspector } from "@/components/common/inspector";
import { CompactReference } from "@/components/common/compact-reference";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { ImageFetchTaskDetails } from "@/features/image-delivery/image-fetch-task-details";
import { ConnectorCheckPanel } from "@/features/connectors/connector-check-panel";
import { BackingCheckPanel } from "@/features/attach/backing-check-panel";
import { EdgeCheckPanel } from "@/features/environment/edge-check-panel";
import { DNSCheckPanel } from "@/features/platform-component/dns-check-panel";
import { RunnerCheckPanel } from "@/features/runners/runner-check-panel";
import { requestTask } from "@/features/task/api";
import { taskFromAPI } from "@/features/task/journal-model";
import {
  TaskIdentity,
  TaskOverview,
  taskPresentation,
} from "@/features/task/task-overview";
import { TaskExecutionTerminal } from "@/features/task/task-execution-terminal";
import { BlueprintExecutions } from "@/features/task/blueprint-executions";
import { useStore } from "@/lib/store";
import { taskDetailActions } from "@/lib/task-detail-actions";
import type {
  ActivityEntry,
  TaskJournalScope,
  TaskJournalSurface,
} from "@/lib/types";
import { RefreshCw, X } from "lucide-react";
import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";

export function TaskDetailDrawer({
  entry,
  taskId,
  scope,
  surface,
  onOpenChange,
}: {
  scope: TaskJournalScope;
  surface: TaskJournalSurface;
  onOpenChange: (open: boolean) => void;
} & (
  { entry: ActivityEntry; taskId?: never } | { taskId: string; entry?: never }
)) {
  const store = useStore();
  const [detail, setDetail] = useState<ActivityEntry | null>(null);
  const [detailLoading, setDetailLoading] = useState(true);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [actionPending, setActionPending] = useState(false);
  const [retryReview, setRetryReview] = useState(false);
  const [reload, setReload] = useState(0);
  const [executionLimit, setExecutionLimit] = useState(5);
  const [executionCursors, setExecutionCursors] = useState<string[]>([""]);
  const suppliedId = taskId ?? entry!.id;
  const [id, setId] = useState(suppliedId);
  const [, setSearch] = useSearchParams();
  useEffect(() => {
    setId(suppliedId);
    setExecutionCursors([""]);
  }, [suppliedId]);
  const task = detail ?? (id === suppliedId ? entry : undefined);

  useEffect(() => {
    const controller = new AbortController();
    setActionError(null);
    setActionPending(false);
    setDetailLoading(true);
    setDetailError(null);
    setDetail(null);
    let timer: ReturnType<typeof setTimeout> | undefined;
    const poll = async () => {
      let terminal = false;
      try {
        const loaded = taskFromAPI(
          await requestTask(
            id,
            controller.signal,
            executionCursors.length > 1 || executionLimit !== 5
              ? { limit: executionLimit, cursor: executionCursors.at(-1) }
              : undefined,
          ),
        );
        if (controller.signal.aborted) return;
        setDetail(loaded);
        setDetailError(null);
        terminal = ["completed", "failed", "timed_out", "aborted"].includes(
          loaded.status,
        );
      } catch (error) {
        if (controller.signal.aborted) return;
        setDetailError(
          error instanceof Error
            ? error.message
            : "Unable to load Task details",
        );
      } finally {
        if (!controller.signal.aborted) setDetailLoading(false);
      }
      if (!terminal && !controller.signal.aborted)
        timer = setTimeout(() => void poll(), 1000);
    };
    void poll();
    return () => {
      controller.abort();
      if (timer) clearTimeout(timer);
    };
  }, [id, reload, executionLimit, executionCursors]);

  const actions = taskDetailActions(detail);

  async function runAction(action: "abort" | "retry") {
    setActionError(null);
    setActionPending(true);
    try {
      if (action === "abort") {
        await store.abortTask(id);
        setReload((value) => value + 1);
      } else {
        const nextId = await store.retryTask(id);
        setReload((value) => value + 1);
        setId(nextId);
        setSearch((current) => {
          const next = new URLSearchParams(current);
          if (next.get("task") === id) next.set("task", nextId);
          return next;
        });
      }
      setActionPending(false);
      setRetryReview(false);
      void store.loadTaskJournal(surface, scope).catch(() => undefined);
    } catch (error) {
      setActionError(
        error instanceof Error ? error.message : `Unable to ${action} Task`,
      );
      setActionPending(false);
    }
  }

  return (
    <Inspector
      open
      onOpenChange={onOpenChange}
      context={task ? `Task / ${taskPresentation(task, store).scope}` : "Task"}
      title={
        task
          ? task.imageFetch
            ? task.title
            : taskPresentation(task, store).title
          : "Task details"
      }
      status={task && <StatusBadge status={task.status} />}
      actions={
        task && <TaskIdentity task={task} onClose={() => onOpenChange(false)} />
      }
      footer={
        <>
          {actions.abort && (
            <Button
              variant="destructive"
              size="sm"
              disabled={actionPending}
              onClick={() => void runAction("abort")}
            >
              <X className="size-4" /> Abort
            </Button>
          )}
          {actions.cancel && (
            <Button
              variant="destructive"
              size="sm"
              disabled={actionPending}
              onClick={() => void runAction("abort")}
            >
              <X className="size-4" /> Cancel
            </Button>
          )}
          {actions.retry && task?.type !== "check" && (
            <Button
              size="sm"
              disabled={actionPending}
              onClick={() =>
                detail?.interruptedDetachRetry
                  ? setRetryReview(true)
                  : void runAction("retry")
              }
            >
              <RefreshCw className="size-4" />{" "}
              {detail?.interruptedDetachRetry
                ? "Retry captured Detach hook"
                : "Retry"}
            </Button>
          )}
          <Button
            variant="outline"
            size="sm"
            disabled={actionPending}
            onClick={() => onOpenChange(false)}
          >
            Close
          </Button>
        </>
      }
    >
      <div className="flex flex-col gap-4">
        {retryReview && detail?.interruptedDetachRetry && (
          <section
            className="space-y-3 rounded-lg border border-border bg-muted/30 p-4"
            aria-label="Confirm captured Detach hook retry"
          >
            <h3 className="text-sm font-medium">
              Run the captured Detach hook again?
            </h3>
            <p className="text-sm text-muted-foreground">
              The hook started, but no result was saved. It may already have
              changed the backing service. Retry runs the original command and
              inputs once in the same Task. Only continue if repeating it is
              safe.
            </p>
            <CompactReference
              value={detail.interruptedDetachRetry.attach_id}
              label="Attach"
            />
            <div className="flex gap-2">
              <Button
                size="sm"
                disabled={actionPending}
                onClick={() => void runAction("retry")}
              >
                Authorize one retry
              </Button>
              <Button
                variant="outline"
                size="sm"
                disabled={actionPending}
                onClick={() => setRetryReview(false)}
              >
                Cancel
              </Button>
            </div>
          </section>
        )}
        {detailLoading && (
          <p
            role="status"
            className="rounded-lg border border-border bg-surface px-3 py-2 text-xs text-muted-foreground"
          >
            Loading Task details…
          </p>
        )}
        {detailError && (
          <div
            role="alert"
            className="flex items-center justify-between gap-3 rounded-lg border border-destructive/40 bg-destructive/5 px-3 py-2 text-xs text-destructive"
          >
            <span>{detailError}</span>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setReload((value) => value + 1)}
            >
              Retry
            </Button>
          </div>
        )}
        {detail && task && (
          <>
            {task.imageFetch ? (
              <>
                <TaskExecutionTerminal task={task} />
                <ImageFetchTaskDetails task={task} />
              </>
            ) : (
              <>
                <TaskOverview task={task}>
                  {task.executor === "blueprint" ? (
                    <BlueprintExecutions
                      task={task}
                      limit={executionLimit}
                      loading={detailLoading}
                      previous={executionCursors.length > 1}
                      onLimit={(value) => {
                        setExecutionLimit(value);
                        setExecutionCursors([""]);
                      }}
                      onPrevious={() =>
                        setExecutionCursors((current) => current.slice(0, -1))
                      }
                      onNext={(cursor) =>
                        setExecutionCursors((current) => [...current, cursor])
                      }
                    />
                  ) : undefined}
                </TaskOverview>
                {task.type === "check" &&
                  task.resourceKind === "connector" &&
                  task.environmentId && (
                    <ConnectorCheckPanel
                      environmentId={task.environmentId}
                      identity={task.id}
                      taskId={task.id}
                      open
                    />
                  )}
                {task.type === "check" &&
                  task.resourceKind === "attach" &&
                  task.environmentId && (
                    <BackingCheckPanel
                      environmentId={task.environmentId}
                      taskId={task.id}
                      open
                    />
                  )}
                {task.type === "check" &&
                  (task.resourceKind === "route" ||
                    task.resourceKind === "component") &&
                  task.environmentId && (
                    <EdgeCheckPanel
                      environmentId={task.environmentId}
                      taskId={task.id}
                      open
                    />
                  )}
                {task.type === "check" &&
                  task.resourceKind === "component" &&
                  !task.environmentId && (
                    <DNSCheckPanel
                      componentId={task.target}
                      taskId={task.id}
                      open
                    />
                  )}
                {task.type === "check" && task.resourceKind === "runner" && (
                  <RunnerCheckPanel
                    runnerId={task.target}
                    taskId={task.id}
                    open
                  />
                )}
              </>
            )}
          </>
        )}
        {actionError && (
          <p
            role="alert"
            className="rounded-lg border border-destructive/40 bg-destructive/5 px-3 py-2 text-xs text-destructive"
          >
            {actionError}
          </p>
        )}
        {actionPending && (
          <p role="status" className="text-xs text-muted-foreground">
            Submitting Task action…
          </p>
        )}
      </div>
    </Inspector>
  );
}
