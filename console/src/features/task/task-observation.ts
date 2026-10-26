import type { TaskResponse } from "./api";
import { reportResourceChange } from "../../lib/resource-observation.ts";

type Listener = (task: TaskResponse) => void;
const listeners = new Set<Listener>();
const observed = new Map<string, string>();
const progress = new Map<string, string>();

export function observeTask(response: TaskResponse) {
  const task = response.task;
  const version = `${task.status}/${task.updated_at}`;
  if (progress.get(task.id) !== version) {
    progress.set(task.id, version);
    if (progress.size > 512) progress.delete(progress.keys().next().value!);
    reportResourceChange();
  }
  observeTerminalTask(response);
}

// Every Task reader shares this boundary. Refresh must outlive the dialog that
// dispatched work, and failed work may also have changed observable resources.
export function observeTerminalTask(response: TaskResponse) {
  const task = response.task;
  if (!task.finished_at || !listeners.size) return;
  const version = `${task.status}/${task.updated_at}`;
  if (observed.get(task.id) === version) return;
  observed.set(task.id, version);
  if (observed.size > 512) observed.delete(observed.keys().next().value!);
  listeners.forEach((listener) => listener(response));
}

export function subscribeTerminalTasks(listener: Listener) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}
