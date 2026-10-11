import { useCallback, useEffect, useRef, useState } from "react";
import { newULID } from "@/lib/utils";
import { reportAcceptedTasks } from "@/features/task/accepted-tasks";
import { getDNS, saveDNS, type DNSChange, type DNSView } from "./api";

export function useEnvironmentDNS(environmentId: string) {
  const [view, setView] = useState<DNSView>();
  const [error, setError] = useState<string>();
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const read = useRef<AbortController | null>(null);
  const write = useRef<AbortController | null>(null);
  const intent = useRef<{ fingerprint: string; key: string } | null>(null);
  const awaitingTask = useRef(false);

  const refresh = useCallback(async () => {
    read.current?.abort();
    const controller = new AbortController();
    read.current = controller;
    try {
      const next = await getDNS(environmentId, controller.signal);
      if (controller.signal.aborted) return;
      setView(next);
      setError(undefined);
      if (awaitingTask.current) {
        if (next.state === "applied" || next.state === "resolver_disabled") {
          awaitingTask.current = false;
        } else if (next.task_id) {
          reportAcceptedTasks({ task_id: next.task_id });
          awaitingTask.current = false;
        }
      }
    } catch (cause) {
      if (!controller.signal.aborted)
        setError(
          cause instanceof Error
            ? cause.message
            : "Unable to read DNS records.",
        );
    } finally {
      if (!controller.signal.aborted) setLoading(false);
    }
  }, [environmentId]);

  useEffect(() => {
    let closed = false;
    let timer: ReturnType<typeof setTimeout>;
    async function poll() {
      if (!document.hidden) await refresh();
      if (!closed) timer = setTimeout(() => void poll(), 5000);
    }
    void poll();
    return () => {
      closed = true;
      clearTimeout(timer);
      read.current?.abort();
      write.current?.abort();
    };
  }, [refresh]);

  const save = async (change: DNSChange) => {
    if (write.current && !write.current.signal.aborted)
      throw new Error("A DNS save is already in progress.");
    const fingerprint = JSON.stringify(change);
    if (intent.current?.fingerprint !== fingerprint)
      intent.current = { fingerprint, key: newULID() };
    const controller = new AbortController();
    write.current = controller;
    setSaving(true);
    try {
      await saveDNS(
        environmentId,
        change,
        intent.current.key,
        controller.signal,
      );
      intent.current = null;
      awaitingTask.current = true;
      await refresh();
    } finally {
      write.current = null;
      if (!controller.signal.aborted) setSaving(false);
    }
  };
  return { view, error, loading, saving, refresh, save };
}
