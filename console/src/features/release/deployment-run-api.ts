import { requestTask, type TaskResponse } from "@/features/task/api";

export async function requestDeploymentRun(
  taskId: string,
  signal: AbortSignal,
): Promise<TaskResponse> {
  const limit = 100;
  const first = await requestTask(taskId, signal, { limit });
  let work = first.work;
  if (!work.more) return first;

  const items = [...work.items];
  const cursors = new Set<string>();
  const revision = work.revision;
  while (work.more) {
    signal.throwIfAborted();
    const after = work.after;
    if (!after || cursors.has(after)) {
      throw new Error("The Controller returned an invalid operation cursor.");
    }
    cursors.add(after);
    ({ work } = await requestTask(taskId, signal, { limit, after, revision }));
    if (work.revision !== revision) {
      throw new Error(
        "The operation snapshot changed. Reload the deployment run.",
      );
    }
    items.push(...work.items);
  }
  return { ...first, work: { ...work, items } };
}
