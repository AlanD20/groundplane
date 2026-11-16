import { useEffect, useRef, useState } from "react";
import { readCheckTaskResult } from "@/features/task/check-result";
import type { TaskResponse } from "@/features/task/api";
import { ControllerRequestError } from "@/lib/controller-request-errors";
import { newULID } from "@/lib/utils";
import { retryTask, type TaskRetryRequest } from "@/features/task/api";
import { captureCleanupRetry } from "@/features/task/cleanup-retry";
import {
  backingCheckRequest,
  getBackingCheck,
  startBackingCheck,
  type BackingCheckRequest,
  type BackingCheckResult,
} from "./check-api";

export function useBackingCheck(
  environmentId: string,
  open: boolean,
  attachId?: string,
  grantId = "",
  initialTaskId?: string,
) {
  const [taskId, setTaskId] = useState(initialTaskId ?? "");
  const [taskDetail, setTaskDetail] = useState<TaskResponse | null>(null);
  const [result, setResult] = useState<BackingCheckResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [sending, setSending] = useState(false);
  const [reload, setReload] = useState(0);
  const generation = useRef(0);
  const scope = useRef<AbortController | null>(null);
  const busy = useRef(false);
  const intent = useRef<{ key: string; body?: BackingCheckRequest } | null>(
    null,
  );
  const cleanupIntent = useRef<{
    taskId: string;
    key: string;
    input?: TaskRetryRequest;
  } | null>(null);
  const selectionIdentity = `${environmentId}/${attachId ?? ""}/${grantId}/${initialTaskId ?? ""}`;
  const selected = useRef(selectionIdentity);
  useEffect(() => {
    const current = ++generation.current;
    const controller = new AbortController();
    scope.current = controller;
    setTaskId(initialTaskId ?? "");
    setTaskDetail(null);
    setResult(null);
    setError(null);
    setSending(false);
    busy.current = false;
    if (selected.current !== selectionIdentity) {
      selected.current = selectionIdentity;
      intent.current = null;
      cleanupIntent.current = null;
    }
    const discover = open && !!attachId && !initialTaskId;
    setLoading(discover);
    if (discover)
      void getBackingCheck(environmentId, controller.signal, {
        attachId,
        grantId,
      })
        .then((next) => {
          if (!controller.signal.aborted && generation.current === current) {
            setResult(next);
            setTaskId(next.task_id);
          }
        })
        .catch((cause) => {
          if (
            !controller.signal.aborted &&
            generation.current === current &&
            !(cause instanceof ControllerRequestError && cause.status === 404)
          )
            setError(
              cause instanceof Error
                ? cause.message
                : "Unable to read the last access check",
            );
        })
        .finally(() => {
          if (!controller.signal.aborted && generation.current === current)
            setLoading(false);
        });
    return () => {
      controller.abort();
      generation.current++;
    };
  }, [
    environmentId,
    attachId,
    grantId,
    initialTaskId,
    open,
    selectionIdentity,
  ]);
  useEffect(() => {
    if (!open || !taskId) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const poll = async () => {
      try {
        const next = await readCheckTaskResult(
          taskId,
          controller.signal,
          () => getBackingCheck(environmentId, controller.signal, { taskId }),
          setTaskDetail,
        );
        if (controller.signal.aborted) return;
        setResult(next.result);
        setError(null);
        // Refresh terminal metadata too: a saved check must become stale when
        // its credentials or deployed consumer change while this panel is open.
        // This reads the recorded result; it never reruns the access probe.
        timer = setTimeout(
          () => void poll(),
          next.taskDetail.task.finished_at ? 10000 : 1000,
        );
      } catch (cause) {
        if (!controller.signal.aborted)
          setError(
            cause instanceof Error
              ? cause.message
              : "Unable to read access check progress",
          );
      }
    };
    void poll();
    return () => {
      controller.abort();
      if (timer) clearTimeout(timer);
    };
  }, [environmentId, taskId, open, reload]);
  async function run(after: string[] = []) {
    if (
      busy.current ||
      loading ||
      !attachId ||
      !scope.current ||
      scope.current.signal.aborted
    )
      return;
    const current = generation.current,
      signal = scope.current.signal;
    busy.current = true;
    setSending(true);
    setError(null);
    intent.current ??= { key: newULID() };
    const pending = intent.current;
    try {
      pending.body ??= backingCheckRequest(attachId, grantId, after);
      const accepted = await startBackingCheck(
        environmentId,
        pending.body,
        pending.key,
        signal,
      );
      if (generation.current === current) {
        intent.current = null;
        setTaskDetail(null);
        setResult(null);
        setTaskId(accepted);
        setReload((value) => value + 1);
      }
    } catch (cause) {
      if (generation.current !== current) return;
      if (
        cause instanceof ControllerRequestError &&
        (cause.code === "state.conflict" ||
          ![409, 500, 502, 503, 504].includes(cause.status))
      )
        intent.current = null;
      setError(
        cause instanceof Error
          ? cause.message
          : "Unable to start the access check",
      );
    } finally {
      if (generation.current === current) {
        busy.current = false;
        setSending(false);
      }
    }
  }
  async function cleanup() {
    if (busy.current || !taskId || !result?.cleanup_required) return;
    const current = generation.current;
    busy.current = true;
    setSending(true);
    setError(null);
    if (cleanupIntent.current?.taskId !== taskId)
      cleanupIntent.current = { taskId, key: newULID() };
    try {
      const captured = cleanupIntent.current;
      captured.input ??= await captureCleanupRetry(taskId, "backing_check");
      if (generation.current !== current) return;
      const next = await retryTask(taskId, captured.input, captured.key);
      if (generation.current === current) {
        cleanupIntent.current = null;
        setTaskDetail(null);
        setResult(null);
        setTaskId(next);
        setReload((value) => value + 1);
      }
    } catch (cause) {
      if (generation.current === current)
        setError(
          cause instanceof Error
            ? cause.message
            : "Unable to resume helper cleanup",
        );
    } finally {
      if (generation.current === current) {
        busy.current = false;
        setSending(false);
      }
    }
  }
  return {
    taskId,
    taskDetail,
    uncertain: intent.current !== null,
    result,
    error,
    sending,
    loading,
    run,
    cleanup,
    active: !!taskId && !taskDetail?.task.finished_at,
    refresh: () => setReload((value) => value + 1),
  };
}
