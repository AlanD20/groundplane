import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Disclosure } from "@/components/common/disclosure";
import { CompactReference } from "@/components/common/compact-reference";
import { StatusBadge } from "@/components/common/status-badge";
import { AttemptStages } from "./attempt-stages";
import { AttemptEvents } from "./attempt-events";
import { WorkRetry } from "./work-retry";
import { cleanupLabel, operationLabel } from "./operation-label";
import {
  requestTaskAttempts,
  requestTaskImage,
  type TaskAttempts,
  type TaskImage,
  type TaskWork,
} from "./api";

const waiting: Record<string, string> = {
  input: "Waiting for captured input to be prepared",
  dependency: "Waiting for prerequisite work to finish",
  conflict: "Waiting for conflicting work to release this resource",
  reservation:
    "Waiting for the blocking Task to finish its reserved resource changes",
  executor: "Waiting for an eligible executor",
  operation_unavailable:
    "The host is connected, but this operation's prerequisites are unavailable. Backup work requires reconciled host staging.",
  capacity: "Waiting for an execution slot",
  retry: "Waiting for the scheduled retry",
};

export function WorkInspection({
  taskId,
  work,
  revision,
  targetName,
  onChanged,
}: {
  taskId: string;
  work: TaskWork;
  revision: number;
  targetName?: string;
  onChanged?: () => void;
}) {
  return (
    <section
      id={`task-work-${work.id}`}
      className="min-w-0 space-y-4 rounded-lg border border-border p-4"
    >
      <header className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0 space-y-1">
          <h3 className="break-words text-sm font-medium">
            {targetName || operationLabel(work.operation)}
          </h3>
          {targetName && (
            <p className="text-xs text-muted-foreground">
              {operationLabel(work.operation)}
            </p>
          )}
          <p className="text-xs text-muted-foreground">
            {work.placement.executor === "agent"
              ? "Runs on the managed host"
              : "Runs in the Controller"}
          </p>
        </div>
        <StatusBadge status={work.outcome || work.phase} />
      </header>
      {work.preparation && (
        <div className="space-y-2 rounded-md bg-muted/40 p-3 text-sm">
          <p className="font-medium">
            {work.outcome === "succeeded"
              ? "Completed without an executor"
              : "Stopped before execution"}
          </p>
          <p className="break-words">{work.preparation.summary}</p>
          <p className="text-xs text-muted-foreground">
            Recorded{" "}
            <time dateTime={work.preparation.finished_at}>
              {new Date(work.preparation.finished_at).toLocaleString()}
            </time>
          </p>
          {work.preparation.dependency_id && (
            <CompactReference
              value={work.preparation.dependency_id}
              label="Prerequisite work"
            />
          )}
        </div>
      )}
      {work.wait && (
        <div
          role="status"
          className="space-y-1 rounded-md bg-muted/40 p-3 text-sm"
        >
          <p>
            {waiting[work.wait.reason] ?? "Waiting for execution permission"}
          </p>
          {work.wait.task_id && (
            <a
              className="underline"
              href={`?task=${encodeURIComponent(work.wait.task_id)}`}
            >
              Inspect blocking Task
            </a>
          )}
          {work.wait.until && (
            <p className="text-xs text-muted-foreground">
              Not before {new Date(work.wait.until).toLocaleString()}
            </p>
          )}
        </div>
      )}
      {work.cleanup !== "none" && (
        <div className="space-y-1 rounded-md border border-border p-3 text-sm">
          <p className="font-medium">{cleanupLabel(work.cleanup)}</p>
          <p className="text-xs text-muted-foreground">
            {work.cleanup === "complete"
              ? "Cleanup finished. The original action outcome above is unchanged."
              : "The original action outcome is unchanged. Resource protection remains until its effects are resolved."}
          </p>
        </div>
      )}
      {work.attempt_count > 0 ? (
        <WorkEvidence
          key={`${taskId}/${work.id}`}
          taskId={taskId}
          work={work}
          revision={revision}
        />
      ) : !work.preparation && !work.wait ? (
        <p className="text-sm text-muted-foreground">
          {work.phase === "settled"
            ? "Settled in the Controller without an executor attempt."
            : "No executor has started this work yet."}
        </p>
      ) : null}
      {onChanged && (
        <WorkRetry
          key={`${taskId}/${work.id}/${work.attempt_count}`}
          taskId={taskId}
          work={work}
          onAccepted={onChanged}
        />
      )}
      <Disclosure
        summary="Technical details"
        summaryClassName="text-xs text-muted-foreground"
      >
        <div className="space-y-2 pt-2">
          <CompactReference value={work.target_id} label="Resource" />
          <CompactReference value={work.id} label="Work item" />
          {work.placement.host_id && (
            <CompactReference value={work.placement.host_id} label="Host" />
          )}
          <p className="text-xs text-muted-foreground">
            Operation: {work.operation}
          </p>
        </div>
      </Disclosure>
    </section>
  );
}

function WorkEvidence({
  taskId,
  work,
  revision,
}: {
  taskId: string;
  work: TaskWork;
  revision: number;
}) {
  const [attempts, setAttempts] = useState<TaskAttempts | null>(null);
  const [image, setImage] = useState<TaskImage | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [historyRevision, setHistoryRevision] = useState<number | null>(null);
  const history = historyRevision !== null;
  const inspectionRevision = historyRevision ?? revision;
  const [after, setAfter] = useState(0);
  const [reload, setReload] = useState(0);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    setAfter(0);
  }, [taskId, work.id, historyRevision]);
  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    void Promise.all([
      requestTaskAttempts(
        taskId,
        work.id,
        inspectionRevision,
        history ? after : Math.max(0, work.attempt_count - 1),
        controller.signal,
      ),
      work.operation === "image"
        ? requestTaskImage(
            taskId,
            work.id,
            inspectionRevision,
            controller.signal,
          )
        : Promise.resolve(null),
    ])
      .then(([page, details]) => {
        if (!controller.signal.aborted) {
          setAttempts(page);
          setImage(details);
        }
      })
      .catch((cause: unknown) => {
        if (!controller.signal.aborted)
          setError(
            cause instanceof Error
              ? cause.message
              : "Unable to load execution evidence",
          );
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [
    taskId,
    work.id,
    work.operation,
    work.attempt_count,
    history,
    inspectionRevision,
    after,
    reload,
  ]);
  return (
    <div className="min-w-0 space-y-3 pt-3">
      {loading && !attempts && (
        <p role="status" className="text-sm text-muted-foreground">
          Loading execution evidence…
        </p>
      )}
      {error && (
        <div role="alert" className="space-y-2 text-sm text-destructive">
          <p>{error}</p>
          <Button
            variant="outline"
            size="sm"
            onClick={() => setReload((value) => value + 1)}
          >
            Retry loading
          </Button>
        </div>
      )}
      {image && (
        <div className="space-y-2 text-sm">
          {image.requested && (
            <p className="break-words font-medium">{image.requested}</p>
          )}
          <p>{image.summary}</p>
          {image.progress?.error_detail && (
            <p role="alert" className="break-words text-destructive">
              {image.progress.error_detail}
            </p>
          )}
          {image.progress && (
            <p className="text-xs text-muted-foreground">
              Downloaded{" "}
              {(image.progress.downloaded_bytes ?? 0).toLocaleString()} bytes of{" "}
              {(image.progress.total_bytes ?? 0).toLocaleString()} reported
              bytes. Cached layers are excluded.
            </p>
          )}
          {image.progress_unavailable && (
            <p className="text-xs text-muted-foreground">
              Download progress is not available.
            </p>
          )}
          <Disclosure
            summary="Exact image identity"
            summaryClassName="text-xs text-muted-foreground"
          >
            <div className="space-y-2 pt-2">
              <CompactReference value={image.image_id} label="Image" />
              {image.manifest && (
                <CompactReference value={image.manifest} label="Manifest" />
              )}
            </div>
          </Disclosure>
        </div>
      )}
      {attempts && (
        <>
          {!history &&
            work.original_failure &&
            work.original_failure.number < work.attempt_count && (
              <section
                className="space-y-2 rounded-md border border-border p-3"
                aria-label="Original failure"
              >
                <h4 className="text-sm font-medium">
                  Original failure · attempt {work.original_failure.number}
                </h4>
                <p className="text-xs text-muted-foreground">
                  This failure remains recorded independently of later retries
                  and cleanup.
                </p>
                <AttemptStages
                  key={work.original_failure.id}
                  taskId={taskId}
                  workId={work.id}
                  attemptId={work.original_failure.id}
                  revision={attempts.revision}
                />
              </section>
            )}
          {attempts.items.length === 0 && (
            <p className="text-sm text-muted-foreground">
              No execution attempt has started.
            </p>
          )}
          <ol className="space-y-3">
            {attempts.items.map((attempt) => (
              <li
                key={attempt.id}
                className="space-y-1 border-l-2 border-border pl-3 text-sm"
              >
                <p className="font-medium">
                  {history ? "Attempt" : "Latest attempt"} {attempt.number} ·{" "}
                  {attempt.direction === "recovery" ? "Cleanup" : "Execution"}
                </p>
                <p>
                  {attempt.outcome ||
                    (attempt.started_at ? "Running" : "Assigned, not started")}
                </p>
                <p className="text-xs text-muted-foreground">
                  Assigned {new Date(attempt.assigned_at).toLocaleString()}
                  {attempt.finished_at
                    ? ` · Finished ${new Date(attempt.finished_at).toLocaleString()}`
                    : ` · Deadline ${new Date(attempt.deadline).toLocaleString()}`}
                </p>
                <AttemptStages
                  taskId={taskId}
                  workId={work.id}
                  attemptId={attempt.id}
                  revision={attempts.revision}
                />
                <AttemptEvents
                  taskId={taskId}
                  workId={work.id}
                  attemptId={attempt.id}
                  revision={attempts.revision}
                />
                <Disclosure
                  summary="Executor identity"
                  summaryClassName="text-xs text-muted-foreground"
                >
                  <div className="space-y-1 pt-2">
                    <CompactReference value={attempt.id} label="Attempt" />
                    {attempt.host_id && (
                      <CompactReference value={attempt.host_id} label="Host" />
                    )}
                    {attempt.agent_id && (
                      <CompactReference
                        value={attempt.agent_id}
                        label="Agent"
                      />
                    )}
                    <p>Process generation {attempt.generation}</p>
                  </div>
                </Disclosure>
              </li>
            ))}
          </ol>
          <div className="flex gap-2">
            {work.attempt_count > 1 && (
              <Button
                size="sm"
                variant="outline"
                aria-pressed={history}
                onClick={() => {
                  setAfter(0);
                  setHistoryRevision((value) =>
                    value === null ? revision : null,
                  );
                }}
              >
                {history ? "Latest attempt" : "Earlier attempts"}
              </Button>
            )}
            {history && after > 0 && (
              <Button size="sm" variant="outline" onClick={() => setAfter(0)}>
                First attempts
              </Button>
            )}
            {history && attempts.more && (
              <Button
                size="sm"
                variant="outline"
                onClick={() => setAfter(attempts.after)}
              >
                Next attempts
              </Button>
            )}
          </div>
          {history && (
            <p className="text-xs text-muted-foreground">
              Earlier attempts are shown at the captured revision. Return to the
              latest attempt for live updates.
            </p>
          )}
        </>
      )}
    </div>
  );
}
