import { useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { useStore } from "@/lib/store";
import type { TaskRetryRequest, TaskWork } from "./api";

export function WorkRetry({
  taskId,
  work,
  onAccepted,
}: {
  taskId: string;
  work: TaskWork;
  onAccepted: () => void;
}) {
  const store = useStore();
  const pending = useRef(false);
  const captured = useRef<TaskRetryRequest | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [accepted, setAccepted] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const cleanup = work.cleanup === "required" || work.cleanup === "stopped";
  if (!work.retry.allowed && !captured.current) {
    if (work.phase !== "attention" && work.outcome !== "failed") return null;
    return (
      <div className="space-y-2">
        <Button size="sm" variant="outline" disabled>
          {cleanup ? "Retry cleanup" : "Retry action"}
        </Button>
        <p className="text-xs text-muted-foreground">
          {work.retry.reason || "Retry is unavailable for this result."}
        </p>
      </div>
    );
  }
  async function submit() {
    if (pending.current || accepted) return;
    pending.current = true;
    setSubmitting(true);
    setError(null);
    captured.current ??= {
      work_item_id: work.id,
      attempt: work.attempt_count,
      ...(work.attempt_count === 0
        ? { queued_failure_at: work.updated_at }
        : {}),
    };
    try {
      await store.retryTask(taskId, captured.current);
      setAccepted(true);
      onAccepted();
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "Unable to submit Retry",
      );
    } finally {
      pending.current = false;
      setSubmitting(false);
    }
  }
  return (
    <div className="space-y-2">
      <Button
        size="sm"
        variant="outline"
        disabled={submitting || accepted}
        onClick={() => void submit()}
      >
        {accepted
          ? "Retry accepted"
          : submitting
            ? "Submitting Retry…"
            : error
              ? "Resend Retry request"
              : cleanup
                ? "Retry cleanup"
                : "Retry action"}
      </Button>
      <p className="text-xs text-muted-foreground">
        {cleanup
          ? "Continues recovery using the captured inputs. The original failure remains recorded."
          : "Retries this action using its captured inputs."}
      </p>
      {error && (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      )}
    </div>
  );
}
