import type { TaskResponse } from "./api";

type Listener = (task: TaskResponse) => void;
const listeners = new Set<Listener>();
const observed = new Map<string, string>();
const terminal = new Set(["completed", "failed", "timed_out", "aborted"]);

// Every Task reader shares this boundary. Refresh must outlive the dialog that
// dispatched work, and failed work may also have changed observable resources.
export function observeTerminalTask(task: TaskResponse) {
  if (!terminal.has(task.status) || !listeners.size) return;
  const version = `${task.status}/${task.updated_at}`;
  if (observed.get(task.id) === version) return;
  observed.set(task.id, version);
  if (observed.size > 512) observed.delete(observed.keys().next().value!);
  listeners.forEach((listener) => listener(task));
}

export function subscribeTerminalTasks(listener: Listener) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}
