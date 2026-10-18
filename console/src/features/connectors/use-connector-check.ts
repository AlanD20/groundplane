import { useEffect, useRef, useState } from "react";
import { newULID } from "@/lib/utils";
import { ControllerRequestError } from "@/lib/controller-request-errors";
import { retryTask, type TaskRetryRequest } from "@/features/task/api";
import { captureCleanupRetry } from "@/features/task/cleanup-retry";
import {
  checkConnectorConnection,
  getConnectorCheck,
  getLatestConnectorCheck,
  type ConnectorCheckRequest,
  type ConnectorCheckResult,
} from "./api";

const activePhases = new Set<ConnectorCheckResult["phase"]>([
  "ready",
  "checking",
  "cleaning",
]);

export function useConnectorCheck(
  environmentId: string,
  identity: string,
  open: boolean,
  initialTaskId?: string,
  connectorId?: string,
) {
  const [taskId, setTaskId] = useState(initialTaskId ?? "");
  const [result, setResult] = useState<ConnectorCheckResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [sending, setSending] = useState(false);
  const [loadingLatest, setLoadingLatest] = useState(false);
  const [reload, setReload] = useState(0);
  const [latestReload, setLatestReload] = useState(0);
  const intent = useRef<{
    identity: string;
    key: string;
    input?: ConnectorCheckRequest;
  } | null>(null);
  const testedIdentity = useRef(identity);
  const generation = useRef(0);
  const submitting = useRef(false);
  const cleanupIntent = useRef<{
    taskId: string;
    key: string;
    input?: TaskRetryRequest;
  } | null>(null);
  useEffect(() => {
    const current = ++generation.current;
    const controller = new AbortController();
    setTaskId(initialTaskId ?? "");
    setResult(null);
    setError(null);
    setSending(false);
    intent.current = null;
    cleanupIntent.current = null;
    testedIdentity.current = identity;
    submitting.current = false;
    const discover = open && !initialTaskId && !!connectorId;
    setLoadingLatest(discover);
    if (discover) {
      void getLatestConnectorCheck(
        environmentId,
        connectorId,
        controller.signal,
      )
        .then((next) => {
          if (controller.signal.aborted || generation.current !== current)
            return;
          setResult(next);
          setTaskId(next.task_id);
        })
        .catch((cause) => {
          if (controller.signal.aborted || generation.current !== current)
            return;
          if (!(
            cause instanceof ControllerRequestError && cause.status === 404
          ))
            setError(
              cause instanceof Error
                ? cause.message
                : "Unable to read the last connection check",
            );
        })
        .finally(() => {
          if (!controller.signal.aborted && generation.current === current)
            setLoadingLatest(false);
        });
    }
    return () => {
      controller.abort();
      generation.current++;
    };
  }, [environmentId, initialTaskId, connectorId, open, latestReload]);
  useEffect(() => {
    if (!taskId || !open) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const poll = async () => {
      try {
        const next = await getConnectorCheck(
          environmentId,
          taskId,
          controller.signal,
        );
        if (controller.signal.aborted) return;
        setResult(next);
        setError(null);
        if (activePhases.has(next.phase))
          timer = setTimeout(() => void poll(), 1000);
      } catch (cause) {
        if (!controller.signal.aborted)
          setError(
            cause instanceof Error
              ? cause.message
              : "Unable to read connection check",
          );
      }
    };
    void poll();
    return () => {
      controller.abort();
      if (timer) clearTimeout(timer);
    };
  }, [environmentId, taskId, open, reload]);

  async function run(getRequest: () => Promise<ConnectorCheckRequest>) {
    if (submitting.current || loadingLatest) return;
    submitting.current = true;
    const runGeneration = generation.current;
    const runIdentity = identity;
    setSending(true);
    setError(null);
    if (intent.current?.identity !== runIdentity)
      intent.current = { identity: runIdentity, key: newULID() };
    const request = intent.current;
    try {
      // Preserve the exact body and key when acceptance is uncertain. A fresh
      // detail read on retry could select a different revision under that key.
      request.input ??= await getRequest();
      if (generation.current !== runGeneration) return;
      const accepted = await checkConnectorConnection(
        environmentId,
        request.input,
        request.key,
      );
      if (generation.current !== runGeneration) return;
      intent.current = null;
      testedIdentity.current = runIdentity;
      setResult(null);
      setTaskId(accepted);
      setReload((value) => value + 1);
    } catch (cause) {
      if (generation.current === runGeneration) {
        if (
          cause instanceof ControllerRequestError &&
          ![409, 500, 502, 503, 504].includes(cause.status)
        )
          intent.current = null;
        setError(
          cause instanceof Error
            ? cause.message
            : "Unable to start connection check",
        );
      }
    } finally {
      if (generation.current === runGeneration) {
        submitting.current = false;
        setSending(false);
      }
    }
  }

  async function retryCleanup() {
    if (submitting.current || !taskId || !result?.cleanup_required) return;
    submitting.current = true;
    const runGeneration = generation.current;
    setSending(true);
    setError(null);
    if (cleanupIntent.current?.taskId !== taskId)
      cleanupIntent.current = { taskId, key: newULID() };
    try {
      const captured = cleanupIntent.current;
      captured.input ??= await captureCleanupRetry(taskId, "connector_check");
      if (generation.current !== runGeneration) return;
      const next = await retryTask(taskId, captured.input, captured.key);
      if (generation.current === runGeneration) {
        cleanupIntent.current = null;
        setResult(null);
        setTaskId(next);
      }
    } catch (cause) {
      if (generation.current === runGeneration)
        setError(
          cause instanceof Error ? cause.message : "Unable to retry cleanup",
        );
    } finally {
      if (generation.current === runGeneration) {
        submitting.current = false;
        setSending(false);
      }
    }
  }
  const active = !!taskId && (!result || activePhases.has(result.phase));
  return {
    taskId,
    result,
    error,
    sending,
    active,
    loadingLatest,
    stale: result?.stale || testedIdentity.current !== identity,
    run,
    retryCleanup,
    refresh: () =>
      taskId
        ? setReload((value) => value + 1)
        : setLatestReload((value) => value + 1),
  };
}
