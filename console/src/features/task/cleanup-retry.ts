import { requestTask, type TaskRetryRequest, type TaskWork } from "./api";

// Single-operation check dialogs select their own recorded cleanup. Capture this
// once with the request key; a repeated HTTP request must not select newer work.
export async function captureCleanupRetry(
  taskId: string,
  operation: TaskWork["operation"],
): Promise<TaskRetryRequest> {
  const detail = await requestTask(taskId);
  const work = detail.work.items;
  if (
    detail.task.id !== taskId ||
    detail.task.history_expired ||
    detail.work.more ||
    work.length !== 1 ||
    work[0].operation !== operation ||
    !work[0].retry.allowed ||
    work[0].phase !== "attention" ||
    work[0].active_attempt_id ||
    !["required", "stopped"].includes(work[0].cleanup) ||
    work[0].attempt_count < 1
  ) {
    throw new Error(
      "This Task has no stopped cleanup attempt to retry. Refresh its details.",
    );
  }
  return { work_item_id: work[0].id, attempt: work[0].attempt_count };
}
