import { useEffect, useState } from "react";
import { requestTask } from "@/features/task/api";
import { taskFromAPI } from "@/features/task/journal-model";
import { TaskExecutionTerminal } from "@/features/task/task-execution-terminal";
import type { ActivityEntry } from "@/lib/types";

export function ReleaseTaskLog({ taskId }: { taskId: string }) {
  const [task, setTask] = useState<ActivityEntry | null>(null);
  const [error, setError] = useState<string>();
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    setTask(null);
    setError(undefined);
    const refresh = async () => {
      let terminal = false;
      try {
        const next = taskFromAPI(await requestTask(taskId, controller.signal));
        if (controller.signal.aborted) return;
        setTask(next);
        setError(undefined);
        terminal = ["completed", "failed", "timed_out", "aborted"].includes(
          next.status,
        );
      } catch (cause) {
        if (controller.signal.aborted) return;
        setError(
          cause instanceof Error
            ? cause.message
            : "Unable to load deployment log",
        );
      }
      if (!terminal && !controller.signal.aborted)
        timer = setTimeout(refresh, 2000);
    };
    void refresh();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [taskId]);
  return (
    <div className="min-w-0 space-y-3">
      {error && (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      )}
      {task ? (
        <TaskExecutionTerminal task={task} />
      ) : (
        !error && (
          <p role="status" className="text-sm text-muted-foreground">
            Loading deployment log…
          </p>
        )
      )}
    </div>
  );
}
