import { useEffect, useRef, useState } from "react";
import { ControllerRequestError } from "@/lib/controller-request-errors";
import { newULID } from "@/lib/utils";
import { retryTask } from "@/features/task/api";
import {
  edgeCheckRequest,
  getEdgeCheck,
  startEdgeCheck,
  tunnelCheckRequest,
  type EdgeCheckSubmission,
  type TunnelCheckSelection,
  type EdgeCheckResult,
} from "./edge-check-api";

// Uncertain acceptance survives panel unmount/reopen. Only non-secret intent is
// retained; definitive acceptance/rejection removes it. It never starts a probe.
const pendingChecks = new Map<
  string,
  { key: string; submission?: EdgeCheckSubmission }
>();
const pendingCleanup = new Map<string, string>();
const runningPhases = new Set([
  "ready",
  "private_checking",
  "public_checking",
  "cleaning",
]);

export function useEdgeCheck(
  environmentId: string,
  open: boolean,
  routeId?: string,
  connectorId = "",
  path = "",
  initialTaskId?: string,
  tunnel?: TunnelCheckSelection,
) {
  const [taskId, setTaskId] = useState(initialTaskId ?? "");
  const [result, setResult] = useState<EdgeCheckResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [sending, setSending] = useState(false);
  const [reload, setReload] = useState(0);
  const generation = useRef(0);
  const scope = useRef<AbortController | null>(null);
  const busy = useRef(false);
  const originServiceId = tunnel?.originServiceId;
  const originPort = tunnel?.port;
  const hostname = tunnel?.hostname;
  const originPath = tunnel?.path;
  const identity = JSON.stringify([
    environmentId,
    routeId,
    connectorId,
    path,
    originServiceId,
    originPort,
    hostname,
    originPath,
  ]);
  useEffect(() => {
    const current = ++generation.current;
    const controller = new AbortController();
    scope.current = controller;
    setTaskId(initialTaskId ?? "");
    setResult(null);
    setError(null);
    setSending(false);
    busy.current = false;
    const discover =
      open && (!!routeId || (!!tunnel && !!connectorId)) && !initialTaskId;
    setLoading(discover);
    if (discover)
      void getEdgeCheck(
        environmentId,
        controller.signal,
        routeId ? { routeId } : { connectorId },
      )
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
    routeId,
    connectorId,
    path,
    initialTaskId,
    open,
    originServiceId,
    originPort,
    hostname,
    originPath,
  ]);
  useEffect(() => {
    if (!open || !taskId) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const poll = async () => {
      try {
        const next = await getEdgeCheck(environmentId, controller.signal, {
          taskId,
        });
        if (controller.signal.aborted) return;
        setResult(next);
        setError(null);
        timer = setTimeout(
          () => void poll(),
          runningPhases.has(next.phase) ? 1000 : 10000,
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
      (!routeId && !tunnel) ||
      !connectorId ||
      !scope.current ||
      scope.current.signal.aborted
    )
      return;
    if (!pendingChecks.has(identity) && pendingChecks.size >= 32) {
      setError(
        "Resolve an earlier uncertain check submission before starting another.",
      );
      return;
    }
    const current = generation.current,
      signal = scope.current.signal;
    busy.current = true;
    setSending(true);
    setError(null);
    let pending = pendingChecks.get(identity);
    if (!pending) {
      pending = { key: newULID() };
      pendingChecks.set(identity, pending);
    }
    try {
      if (!pending.submission) {
        if (routeId)
          pending.submission = {
            kind: "route",
            body: await edgeCheckRequest(
              environmentId,
              routeId,
              connectorId,
              path,
              signal,
            ),
          };
        else if (tunnel)
          pending.submission = {
            kind: "tunnel",
            connectorId,
            body: await tunnelCheckRequest(
              environmentId,
              connectorId,
              tunnel,
              signal,
            ),
          };
      }
      if (!pending.submission)
        throw new Error(
          "Select a Route or Tunnel origin before checking access.",
        );
      const accepted = await startEdgeCheck(
        environmentId,
        pending.submission,
        pending.key,
        signal,
      );
      pendingChecks.delete(identity);
      if (generation.current === current) {
        setResult(null);
        setTaskId(accepted);
        setReload((value) => value + 1);
      }
    } catch (cause) {
      if (
        cause instanceof ControllerRequestError &&
        (cause.code === "state.conflict" ||
          ![409, 500, 502, 503, 504].includes(cause.status))
      )
        pendingChecks.delete(identity);
      if (generation.current === current)
        setError(
          cause instanceof Error
            ? cause.message
            : "Unable to start public access check",
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
    if (!pendingCleanup.has(taskId) && pendingCleanup.size >= 32) {
      setError(
        "Resolve an earlier uncertain cleanup submission before starting another.",
      );
      return;
    }
    const current = generation.current;
    busy.current = true;
    setSending(true);
    setError(null);
    let key = pendingCleanup.get(taskId);
    if (!key) {
      key = newULID();
      pendingCleanup.set(taskId, key);
    }
    try {
      const next = await retryTask(taskId, key);
      pendingCleanup.delete(taskId);
      if (generation.current === current) {
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
    loading,
    sending,
    run,
    cleanup,
    active: !!taskId && (!result || runningPhases.has(result.phase)),
  };
}
