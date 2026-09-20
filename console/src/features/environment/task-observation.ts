type TaskLike = { status: string };

const terminalTaskStatuses = new Set([
  "completed",
  "failed",
  "timed_out",
  "aborted",
]);

function waitForDelay(delayMs: number, signal?: AbortSignal): Promise<void> {
  if (!signal) return new Promise((resolve) => setTimeout(resolve, delayMs));
  if (signal.aborted)
    return Promise.reject(
      signal.reason ?? new DOMException("Aborted", "AbortError"),
    );
  return new Promise((resolve, reject) => {
    let timer: ReturnType<typeof setTimeout>;
    const abort = () => {
      clearTimeout(timer);
      reject(signal.reason ?? new DOMException("Aborted", "AbortError"));
    };
    timer = setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve();
    }, delayMs);
    signal.addEventListener("abort", abort, { once: true });
  });
}

export async function observeEnvironmentTask<Task extends TaskLike>(
  requestTask: (taskId: string, signal?: AbortSignal) => Promise<Task>,
  taskId: string,
  active: { current: boolean },
  signal?: AbortSignal,
) {
  let delayMs = 250;
  while (active.current && !signal?.aborted) {
    const task = await requestTask(taskId, signal);
    if (terminalTaskStatuses.has(task.status)) return task;
    await waitForDelay(delayMs, signal);
    delayMs = Math.min(delayMs * 2, 2_000);
  }
  throw signal?.reason ?? new Error("Environment task observation stopped");
}
