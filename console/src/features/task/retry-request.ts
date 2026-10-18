import { newULID } from "@/lib/utils";
import type { TaskRetryRequest } from "./api";

// Only request identity is stored, never captured operation inputs or Secrets.
// Keep uncertain submissions across drawer close/reload; a new attempt has a
// different key and cannot reinterpret the old operator action.
function storageKey(taskId: string, input: TaskRetryRequest) {
  if (
    !/^task_[0-9A-HJKMNP-TV-Z]{26}$/.test(taskId) ||
    !/^work_[0-9A-HJKMNP-TV-Z]{26}$/.test(input.work_item_id) ||
    !Number.isSafeInteger(input.attempt) ||
    input.attempt < 0 ||
    (input.attempt === 0 &&
      (!input.queued_failure_at ||
        !Number.isFinite(Date.parse(input.queued_failure_at)))) ||
    (input.attempt !== 0 && input.queued_failure_at !== undefined)
  )
    throw new Error(
      "Retry requires an identified Task, work item and attempt.",
    );
  return `groundplane-retry:${taskId}:${input.work_item_id}:${input.attempt}:${input.queued_failure_at ?? ""}`;
}

export function captureRetryKey(
  taskId: string,
  input: TaskRetryRequest,
): string {
  const name = storageKey(taskId, input);
  const existing = sessionStorage.getItem(name);
  if (existing !== null) {
    if (!/^groundplane:[0-9A-HJKMNP-TV-Z]{26}$/.test(existing))
      throw new Error(
        "The saved Retry request is invalid. No Retry was submitted.",
      );
    return existing;
  }
  const key = `groundplane:${newULID()}`;
  // Fail before submitting if the browser cannot retain replay identity.
  sessionStorage.setItem(name, key);
  return key;
}

export function forgetRetryKey(
  taskId: string,
  input: TaskRetryRequest,
  key: string,
) {
  const name = storageKey(taskId, input);
  if (sessionStorage.getItem(name) === key) sessionStorage.removeItem(name);
}
