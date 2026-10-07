import { useEffect, useRef, useState } from "react";
import { subscribeTerminalTasks } from "./terminal-observation";
import { ResourceRefreshQueue } from "./resource-refresh-queue";
import type { TaskResponse } from "./api";

export function useResourceRefresh(
  refresh: (tasks: TaskResponse[], isCurrent: () => boolean) => Promise<void>,
) {
  const refreshRef = useRef(refresh);
  refreshRef.current = refresh;
  const queueRef = useRef<ResourceRefreshQueue | null>(null);
  const [resourceRefreshErrors, setErrors] = useState<string[]>([]);
  useEffect(() => {
    const tasks = new Map<string, Map<string, TaskResponse>>();
    const queue = new ResourceRefreshQueue(async (key, isCurrent) => {
      const group = tasks.get(key);
      if (group) {
        await refreshRef.current([...group.values()], isCurrent);
        if (isCurrent()) tasks.delete(key);
      }
    }, setErrors);
    queueRef.current = queue;
    const unsubscribe = subscribeTerminalTasks((task) => {
      const key =
        task.environment_id ??
        task.project_id ??
        task.tenant_id ??
        task.resource_kind ??
        task.target;
      const group = tasks.get(key) ?? new Map<string, TaskResponse>();
      group.set(`${task.resource_kind}/${task.type}`, task);
      tasks.set(key, group);
      queue.invalidate(key);
    });
    return () => {
      unsubscribe();
      queue.close();
      queueRef.current = null;
    };
  }, []);
  return {
    resourceRefreshErrors,
    retryResourceRefresh: () => queueRef.current?.retry(),
  };
}
