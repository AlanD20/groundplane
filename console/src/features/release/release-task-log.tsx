import { useEffect, useState } from "react";
import { requestTask, type TaskResponse } from "@/features/task/api";
import { TaskExecution } from "@/features/task/task-execution";
import { TaskLink } from "@/components/common/task-link";

export function ReleaseTaskLog({ taskId }: { taskId: string }) {
  const [task, setTask] = useState<TaskResponse | null>(null);
  const [error, setError] = useState<string>();
  const [reload, setReload] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    setTask(null);
    setError(undefined);
    const refresh = async () => {
      let terminal = false;
      try {
        const next = await requestTask(taskId, controller.signal);
        if (controller.signal.aborted) return;
        setTask(next);
        setError(undefined);
        terminal = !!next.task.finished_at;
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
  }, [taskId, reload]);
  return (
    <div className="min-w-0 space-y-3">
      {error && (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      )}
      {task ? (
        <>
          <TaskExecution
            detail={task}
            onChanged={() => setReload((value) => value + 1)}
          />
          {task.work.more && (
            <TaskLink taskId={taskId}>Inspect all work</TaskLink>
          )}
        </>
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
