import { useSyncExternalStore } from "react";

export type TaskNotification = {
  id: string;
  toastUntil?: number;
  unread: boolean;
};
const toastLifetimeMs = 5_000;
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
  if (!next.length) return;
  const toastUntil = Date.now() + toastLifetimeMs;
  accepted = [
    ...next.map((id) => ({ id, toastUntil, unread: true })),
    ...accepted,
  ].slice(0, 50);
  listeners.forEach((listener) => listener());
}

export function dismissAcceptedTask(id: string) {
  accepted = accepted.map((item) =>
    item.id === id ? { ...item, toastUntil: undefined } : item,
  );
  listeners.forEach((listener) => listener());
}

export function readTaskNotification(id: string) {
  accepted = accepted.map((item) =>
    item.id === id ? { ...item, unread: false, toastUntil: undefined } : item,
  );
  listeners.forEach((listener) => listener());
}

export function markTaskNotificationsRead() {
  if (!accepted.some((item) => item.unread || item.toastUntil)) return;
  accepted = accepted.map((item) => ({
    ...item,
    unread: false,
    toastUntil: undefined,
  }));
  listeners.forEach((listener) => listener());
}

export function expireTaskToasts(now: number) {
  accepted = accepted.map((item) =>
    item.toastUntil !== undefined && item.toastUntil <= now
      ? { ...item, toastUntil: undefined }
      : item,
  );
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
