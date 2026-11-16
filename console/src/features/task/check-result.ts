import { requestTask, type TaskResponse } from "./api";

// Waiting Tasks have no native check record. Observe preparation first; only
// captured work permits reading its feature-specific execution evidence.
export async function readCheckTaskResult<T>(
  taskId: string,
  signal: AbortSignal,
  readResult: () => Promise<T>,
  onTaskDetail: (detail: TaskResponse) => void,
): Promise<{ taskDetail: TaskResponse; result: T | null }> {
  const taskDetail = await requestTask(taskId, signal);
  if (!signal.aborted) onTaskDetail(taskDetail);
  const captured =
    !taskDetail.task.pending_preparation && taskDetail.work.items.length > 0;
  const result = captured && !signal.aborted ? await readResult() : null;
  return { taskDetail, result };
}
