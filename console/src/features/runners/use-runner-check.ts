import { useEffect, useRef, useState } from "react";
import { ControllerRequestError } from "@/lib/controller-request-errors";
import { requestTask, type TaskResponse } from "@/features/task/api";
import {
  getRunnerCheck,
  pendingRunnerCheck,
  releaseRunnerCheck,
  runnerCheckRequest,
  runnerCheckSubmission,
  startRunnerCheck,
  type RunnerCheckResult,
} from "./check-api";

export function useRunnerCheck(
  runnerId: string,
  open: boolean,
  initialTaskId = "",
) {
  const [taskId, setTaskId] = useState(initialTaskId);
  const [result, setResult] = useState<RunnerCheckResult | null>(null);
  const [taskDetail, setTaskDetail] = useState<TaskResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [sending, setSending] = useState(false);
  const [uncertain, setUncertain] = useState(!!pendingRunnerCheck(runnerId));
  const [reload, setReload] = useState(0);
  const generation = useRef(0);
  const scope = useRef<AbortController | null>(null);
  const busy = useRef(false);

  useEffect(() => {
    const current = ++generation.current;
    const controller = new AbortController();
    scope.current = controller;
    busy.current = false;
    setSending(false);
    setTaskId(initialTaskId);
    setResult(null);
    setTaskDetail(null);
    setError(null);
    setUncertain(!!pendingRunnerCheck(runnerId));
    setLoading(open && !initialTaskId);
    if (open && !initialTaskId)
      void getRunnerCheck(runnerId, "", controller.signal)
        .then((next) => {
          if (generation.current === current && !controller.signal.aborted) {
            setResult(next);
            setTaskId(next.task_id);
          }
        })
        .catch((cause) => {
          if (
            generation.current === current &&
            !controller.signal.aborted &&
            !(cause instanceof ControllerRequestError && cause.status === 404)
          )
            setError(
              cause instanceof Error
                ? cause.message
                : "Unable to read the last Runner check",
            );
        })
        .finally(() => {
          if (generation.current === current && !controller.signal.aborted)
            setLoading(false);
        });
    return () => {
      controller.abort();
      generation.current++;
    };
  }, [runnerId, open, initialTaskId]);

  useEffect(() => {
    if (!open || !taskId) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const poll = async () => {
      try {
        const detail = await requestTask(taskId, controller.signal);
        if (controller.signal.aborted) return;
        setTaskDetail(detail);
        if (detail.work.items.length > 0) {
          const next = await getRunnerCheck(
            runnerId,
            taskId,
            controller.signal,
          );
          if (controller.signal.aborted) return;
          setResult(next);
        } else setResult(null);
        if (controller.signal.aborted) return;
        setError(null);
        if (!detail.task.finished_at)
          timer = setTimeout(() => void poll(), 1000);
      } catch (cause) {
        if (!controller.signal.aborted)
          setError(
            cause instanceof Error
              ? cause.message
              : "Unable to read Runner check progress",
          );
      }
    };
    void poll();
    return () => {
      controller.abort();
      if (timer) clearTimeout(timer);
    };
  }, [runnerId, taskId, open, reload]);

  async function run(after: string[] = []): Promise<string> {
    const signal = scope.current?.signal;
    if (busy.current || loading || initialTaskId || !signal || signal.aborted)
      throw new Error("The Runner check cannot be submitted in this state");
    const current = generation.current;
    busy.current = true;
    setSending(true);
    setError(null);
    const pending = runnerCheckSubmission(runnerId);
    try {
      pending.body ??= runnerCheckRequest(after);
      const accepted = await startRunnerCheck(
        runnerId,
        pending.body,
        pending.key,
        signal,
      );
      releaseRunnerCheck(runnerId, pending);
      if (generation.current === current) {
        setUncertain(false);
        setResult(null);
        setTaskId(accepted);
        setReload((value) => value + 1);
      }
      return accepted;
    } catch (cause) {
      if (
        !pending.body ||
        (cause instanceof ControllerRequestError &&
          (cause.code === "state.conflict" ||
            ![409, 500, 502, 503, 504].includes(cause.status)))
      )
        releaseRunnerCheck(runnerId, pending);
      if (generation.current !== current) throw cause;
      setUncertain(!!pendingRunnerCheck(runnerId));
      setError(
        cause instanceof Error
          ? cause.message
          : "Unable to start the Runner check",
      );
      throw cause;
    } finally {
      if (generation.current === current) {
        busy.current = false;
        setSending(false);
      }
    }
  }
  return {
    taskId,
    result,
    taskDetail,
    error,
    loading,
    sending,
    uncertain,
    run,
    active: !!taskId && !taskDetail?.task.finished_at,
    refresh: () => setReload((value) => value + 1),
  };
}
