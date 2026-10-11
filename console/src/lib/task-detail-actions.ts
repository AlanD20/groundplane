import type { ActivityEntry } from "./types.ts";

export type TaskDetailActions = {
  abort: boolean;
  cancel: boolean;
  retry: boolean;
};

export function taskDetailActions(
  task:
    | (Pick<ActivityEntry, "type" | "status"> &
        Partial<
          Pick<
            ActivityEntry,
            "target" | "executor" | "resourceKind" | "interruptedDetachRetry"
          >
        >)
    | null,
): TaskDetailActions {
  if (task === null || task.type === "backup_prune") {
    return { abort: false, cancel: false, retry: false };
  }
  return {
    abort: task.status === "running",
    cancel: task.status === "pending",
    retry:
      !!task.interruptedDetachRetry ||
      (task.executor !== "blueprint" &&
        !(
          task.type === "check" &&
          (task.resourceKind === "component" || task.resourceKind === "runner")
        ) &&
        !(task.type === "update" && task.target === "controller") &&
        (task.status === "failed" ||
          task.status === "timed_out" ||
          task.status === "aborted")),
  };
}
