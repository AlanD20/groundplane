import { useEffect, useState } from "react";
import { TaskLink } from "@/components/common/task-link";
import { requestTask } from "./api";
import { taskFromAPI } from "./journal-model";

/** Resolve a captured Task's readable title without changing its stable target. */
export function TaskReferenceLink({ taskId }: { taskId: string }) {
  const [title, setTitle] = useState<{ id: string; text: string }>();
  useEffect(() => {
    const abort = new AbortController();
    void requestTask(taskId, abort.signal, { limit: 1 })
      .then((result) => {
        if (!abort.signal.aborted)
          setTitle({ id: taskId, text: taskFromAPI(result.task).title });
      })
      .catch(() => undefined);
    return () => abort.abort();
  }, [taskId]);
  return (
    <TaskLink taskId={taskId}>
      {title?.id === taskId ? title.text : taskId}
    </TaskLink>
  );
}
