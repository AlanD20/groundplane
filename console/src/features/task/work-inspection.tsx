import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Disclosure } from "@/components/common/disclosure";
import { CompactReference } from "@/components/common/compact-reference";
import { StatusBadge } from "@/components/common/status-badge";
import { AttemptStages } from "./attempt-stages";
import { AttemptEvents } from "./attempt-events";
import { WorkRetry } from "./work-retry";
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
  executor: "Waiting for an eligible executor",
  capacity: "Waiting for an execution slot",
  retry: "Waiting for the scheduled retry",
};

export function WorkInspection({
  taskId,
  work,
  revision,
  onChanged,
}: {
  taskId: string;
  work: TaskWork;
  revision: number;
  onChanged?: () => void;
}) {
  const [open, setOpen] = useState(false);
  const atomicKeyRotation = work.operation === "secret_rotation" && work.attempt_count === 0;
  return (
    <section id={`task-work-${work.id}`} className="min-w-0 space-y-3 rounded-lg border border-border p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-medium">
          {atomicKeyRotation ? "Rotate Backup encryption key" : work.operation === "etcd_configuration" ? "Apply etcd configuration" : work.operation.replaceAll("_", " ")}
        </h3>
        <StatusBadge status={work.outcome || work.phase} />
      </div>
      <CompactReference value={work.target_id} label="Resource" />
      <p className="text-xs text-muted-foreground">
        {work.placement.executor === "agent" ? "Agent" : "Controller"}
        {work.placement.host_id ? ` on ${work.placement.host_id}` : ""}
      </p>
      {atomicKeyRotation && (
        <p className="text-sm">
          {work.outcome === "succeeded"
            ? "The new key is active for future backups. Existing recovery points keep their original key era."
            : work.phase === "settled"
              ? "Rotation ended before the key changed. Submit a new rotation when ready."
              : "Waiting to atomically replace this Environment’s Backup encryption key."}
        </p>
      )}
      {work.preparation && (
        <div className="space-y-2 rounded-md bg-muted/40 p-3 text-sm">
          <p className="font-medium">Stopped before execution</p>
          <p className="break-words">{work.preparation.summary}</p>
          <p className="text-xs text-muted-foreground">
            No executor was assigned. Finished{" "}
            <time dateTime={work.preparation.finished_at}>
              {new Date(work.preparation.finished_at).toLocaleString()}
            </time>
          </p>
          {work.preparation.dependency_id && (
            <CompactReference
              value={work.preparation.dependency_id}
              label="Unsuccessful prerequisite"
            />
          )}
        </div>
      )}
      {work.wait && (
        <div className="space-y-1 rounded-md bg-muted/40 p-3 text-sm">
          <p>{waiting[work.wait.reason] ?? work.wait.reason}</p>
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
        <p className="text-sm">
          Cleanup: {work.cleanup.replaceAll("_", " ")}. The original action
          outcome is shown separately.
        </p>
      )}
      {!work.preparation && !atomicKeyRotation && (
        <Disclosure
          open={open}
          onOpenChange={setOpen}
          summary={`Execution details · ${work.attempt_count} attempt${work.attempt_count === 1 ? "" : "s"}`}
          summaryClassName="text-sm font-medium"
        >
          {open && (
            <WorkEvidence taskId={taskId} work={work} revision={revision} />
          )}
        </Disclosure>
      )}
      {onChanged && (
        <WorkRetry
          key={`${taskId}/${work.id}/${work.attempt_count}`}
          taskId={taskId}
          work={work}
          onAccepted={onChanged}
        />
      )}
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
  const [after, setAfter] = useState(0);
  const [reload, setReload] = useState(0);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    setAfter(0);
  }, [taskId, work.id, revision]);
  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    setAttempts(null);
    setImage(null);
    void Promise.all([
      requestTaskAttempts(taskId, work.id, revision, after, controller.signal),
      work.operation === "image"
        ? requestTaskImage(taskId, work.id, revision, controller.signal)
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
  }, [taskId, work.id, work.operation, revision, after, reload]);
  return (
    <div className="min-w-0 space-y-3 pt-3">
      {loading && (
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
                  Attempt {attempt.number} ·{" "}
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
            {after > 0 && (
              <Button size="sm" variant="outline" onClick={() => setAfter(0)}>
                First attempts
              </Button>
            )}
            {attempts.more && (
              <Button
                size="sm"
                variant="outline"
                onClick={() => setAfter(attempts.after)}
              >
                Next attempts
              </Button>
            )}
          </div>
        </>
      )}
    </div>
  );
}
