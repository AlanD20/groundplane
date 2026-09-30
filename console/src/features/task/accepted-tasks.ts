import { useSyncExternalStore } from "react";

export type TaskNotification = { id: string; toast: boolean; unread: boolean };
let accepted: TaskNotification[] = [];
const listeners = new Set<() => void>();

// Keep only Task identities, never response bodies or operator inputs.
export function reportAcceptedTasks(response: unknown) {
  if (!response || typeof response !== "object") return;
  const ids: string[] = [];
  for (const field of [
    "task_id",
    "create_task_id",
    "reconcile_task_id",
    "deletion_task_id",
  ]) {
    if (field in response) {
      const value = (response as Record<string, unknown>)[field];
      if (typeof value === "string" && /^task_[0-9A-Z]{26}$/.test(value))
        ids.push(value);
    }
  }
  if (!ids.length) return;
  const next = [...new Set(ids)].filter(
    (id) => !accepted.some((item) => item.id === id),
  );
  accepted = [
    ...next.map((id) => ({ id, toast: true, unread: true })),
    ...accepted,
  ].slice(0, 50);
  listeners.forEach((listener) => listener());
}

export function dismissAcceptedTask(id: string) {
  accepted = accepted.map((item) =>
    item.id === id ? { ...item, toast: false } : item,
  );
  listeners.forEach((listener) => listener());
}

export function markTaskNotificationsRead() {
  accepted = accepted.map((item) => ({ ...item, unread: false, toast: false }));
  listeners.forEach((listener) => listener());
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export function useAcceptedTasks() {
  return useSyncExternalStore(subscribe, () => accepted);
}
