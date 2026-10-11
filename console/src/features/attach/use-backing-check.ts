import { useEffect, useRef, useState } from "react";
import { ControllerRequestError } from "@/lib/controller-request-errors";
import { newULID } from "@/lib/utils";
import { retryTask } from "@/features/task/api";
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
  const cleanupIntent = useRef<{ taskId: string; key: string } | null>(null);
  const selectionIdentity = `${environmentId}/${attachId ?? ""}/${grantId}/${initialTaskId ?? ""}`;
  const selected = useRef(selectionIdentity);
  useEffect(() => {
    const current = ++generation.current;
    const controller = new AbortController();
    scope.current = controller;
    setTaskId(initialTaskId ?? "");
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
        const next = await getBackingCheck(environmentId, controller.signal, {
          taskId,
        });
        if (controller.signal.aborted) return;
        setResult(next);
        setError(null);
        // Refresh terminal metadata too: a saved check must become stale when
        // its credentials or deployed consumer change while this panel is open.
        // This reads the recorded result; it never reruns the access probe.
        timer = setTimeout(
          () => void poll(),
          ["ready", "checking", "cleaning"].includes(next.phase) ? 1000 : 10000,
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
  async function run() {
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
      pending.body ??= await backingCheckRequest(
        environmentId,
        attachId,
        grantId,
        signal,
      );
      const accepted = await startBackingCheck(
        environmentId,
        pending.body,
        pending.key,
        signal,
      );
      if (generation.current === current) {
        intent.current = null;
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
      const next = await retryTask(taskId, cleanupIntent.current.key);
      if (generation.current === current) {
        cleanupIntent.current = null;
        setResult(null);
        setTaskId(next);
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
    result,
    error,
    sending,
    loading,
    run,
    cleanup,
    active:
      !!taskId &&
      (!result || ["ready", "checking", "cleaning"].includes(result.phase)),
    refresh: () => setReload((value) => value + 1),
  };
}
