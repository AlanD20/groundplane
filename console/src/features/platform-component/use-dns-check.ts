import { useEffect, useRef, useState } from "react";
import { ControllerRequestError } from "@/lib/controller-request-errors";
import { newULID } from "@/lib/utils";
import {
  dnsCheckRequest,
  getDNSCheck,
  startDNSCheck,
  type DNSCheckQuery,
  type DNSCheckRequest,
  type DNSCheckResult,
} from "./dns-check-api";

export function useDNSCheck(
  componentId: string,
  open: boolean,
  initialTaskId = "",
) {
  const [taskId, setTaskId] = useState(initialTaskId);
  const [result, setResult] = useState<DNSCheckResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [sending, setSending] = useState(false);
  const [uncertain, setUncertain] = useState(false);
  const [reload, setReload] = useState(0);
  const generation = useRef(0);
  const scope = useRef<AbortController | null>(null);
  const busy = useRef(false);
  const intent = useRef<{
    key: string;
    queries: DNSCheckQuery[];
    body?: DNSCheckRequest;
  } | null>(null);
  const identity = `${componentId}/${initialTaskId}`;
  const selection = useRef(identity);

  useEffect(() => {
    const current = ++generation.current;
    const controller = new AbortController();
    scope.current = controller;
    busy.current = false;
    if (selection.current !== identity) {
      selection.current = identity;
      intent.current = null;
    }
    setUncertain(intent.current !== null);
    setSending(false);
    setTaskId(initialTaskId);
    setResult(null);
    setError(null);
    setLoading(open && !initialTaskId);
    if (open && !initialTaskId)
      void getDNSCheck(componentId, "", controller.signal)
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
                : "Unable to read the last DNS check",
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
  }, [componentId, initialTaskId, identity, open]);

  useEffect(() => {
    if (!open || !taskId) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const poll = async () => {
      try {
        const next = await getDNSCheck(componentId, taskId, controller.signal);
        if (controller.signal.aborted) return;
        setResult(next);
        setError(null);
        // Only read recorded evidence, including terminal-result freshness.
        timer = setTimeout(
          () => void poll(),
          next.phase === "finished" ? 10000 : 1000,
        );
      } catch (cause) {
        if (!controller.signal.aborted)
          setError(
            cause instanceof Error
              ? cause.message
              : "Unable to read DNS check progress",
          );
      }
    };
    void poll();
    return () => {
      controller.abort();
      if (timer) clearTimeout(timer);
    };
  }, [componentId, taskId, open, reload]);

  async function run(queries: DNSCheckQuery[]) {
    if (
      busy.current ||
      loading ||
      initialTaskId ||
      !scope.current ||
      scope.current.signal.aborted
    )
      return;
    const current = generation.current,
      signal = scope.current.signal;
    busy.current = true;
    setSending(true);
    setError(null);
    intent.current ??= {
      key: newULID(),
      queries: queries.map((query) => ({ ...query })),
    };
    const pending = intent.current;
    try {
      pending.body ??= await dnsCheckRequest(
        componentId,
        pending.queries,
        signal,
      );
      const accepted = await startDNSCheck(
        componentId,
        pending.body,
        pending.key,
        signal,
      );
      if (generation.current === current) {
        intent.current = null;
        setUncertain(false);
        setResult(null);
        setTaskId(accepted);
        setReload((value) => value + 1);
      }
    } catch (cause) {
      if (generation.current !== current) return;
      if (
        !pending.body ||
        (cause instanceof ControllerRequestError &&
          (cause.code === "state.conflict" ||
            ![409, 500, 502, 503, 504].includes(cause.status)))
      )
        intent.current = null;
      setUncertain(intent.current !== null);
      setError(
        cause instanceof Error
          ? cause.message
          : "Unable to start the DNS check",
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
    uncertain,
    run,
    active: !!taskId && (!result || result.phase !== "finished"),
    refresh: () => setReload((value) => value + 1),
  };
}
