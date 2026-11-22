import { useVisibleRefresh } from "@/lib/use-visible-refresh";
import { useCallback, useEffect, useRef, useState } from "react";
import {
  getMonitoring,
  monitoringKey,
  type MonitoringRequest,
  type MonitoringResponse,
} from "./api";

type CacheEntry = { value: MonitoringResponse; readAt: number };
const maximumCacheEntries = 24;
const cache = new Map<string, CacheEntry>();
const maximumCursorPages = 32;

export function useMonitoringCursor(resetKey: string) {
  const [state, setState] = useState<{
    key: string;
    cursors: (string | undefined)[];
    index: number;
    offset: number;
  }>({ key: resetKey, cursors: [undefined], index: 0, offset: 0 });

  useEffect(() => {
    setState({ key: resetKey, cursors: [undefined], index: 0, offset: 0 });
  }, [resetKey]);

  const current =
    state.key === resetKey
      ? state
      : { key: resetKey, cursors: [undefined], index: 0, offset: 0 };

  return {
    cursor: current.cursors[current.index],
    page: current.offset + current.index + 1,
    canPrevious: current.index > 0,
    previous: () =>
      setState((current) => ({
        ...(current.key === resetKey
          ? current
          : { key: resetKey, cursors: [undefined], index: 0, offset: 0 }),
        index: current.key === resetKey ? Math.max(0, current.index - 1) : 0,
      })),
    next: (cursor: string) =>
      setState((current) => {
        const active =
          current.key === resetKey
            ? current
            : { key: resetKey, cursors: [undefined], index: 0, offset: 0 };
        let cursors = [...active.cursors.slice(0, active.index + 1), cursor];
        let offset = active.offset;
        if (cursors.length > maximumCursorPages) {
          cursors = cursors.slice(cursors.length - maximumCursorPages);
          offset += 1;
        }
        return { key: resetKey, cursors, index: cursors.length - 1, offset };
      }),
  };
}

function cached(key: string): CacheEntry | undefined {
  const entry = cache.get(key);
  if (!entry) return undefined;
  cache.delete(key);
  cache.set(key, entry);
  return entry;
}

function remember(key: string, entry: CacheEntry) {
  cache.delete(key);
  cache.set(key, entry);
  while (cache.size > maximumCacheEntries)
    cache.delete(cache.keys().next().value!);
}

function errorMessage(error: unknown): string {
  return error instanceof Error
    ? error.message
    : "Monitoring could not be loaded.";
}

export function useMonitoringQuery(request: MonitoringRequest) {
  const key = monitoringKey(request);
  const initial = cached(key);
  const [value, setValue] = useState<MonitoringResponse | undefined>(
    initial?.value,
  );
  const [readAt, setReadAt] = useState<number | undefined>(initial?.readAt);
  const [error, setError] = useState<string>();
  const [loading, setLoading] = useState(!initial);
  const [paused, setPaused] = useState(false);
  const [now, setNow] = useState(() => Date.now());
  const active = useRef<AbortController | undefined>(undefined);
  const previousKey = useRef(key);

  useEffect(() => {
    const entry = cached(key);
    active.current?.abort();
    setValue(entry?.value);
    setReadAt(entry?.readAt);
    setError(undefined);
    setLoading(!entry);
  }, [key]);

  const refresh = useCallback(
    async (lifetime: AbortSignal) => {
      active.current?.abort();
      const controller = new AbortController();
      active.current = controller;
      const abort = () => controller.abort(lifetime.reason);
      lifetime.addEventListener("abort", abort, { once: true });
      setLoading((current) => current && !cached(key));
      try {
        const next = await getMonitoring(request, controller.signal);
        if (controller.signal.aborted) return;
        const observed = Date.now();
        remember(key, { value: next, readAt: observed });
        setValue(next);
        setReadAt(observed);
        setNow(observed);
        setError(undefined);
      } catch (cause) {
        if (!controller.signal.aborted) setError(errorMessage(cause));
      } finally {
        lifetime.removeEventListener("abort", abort);
        if (!controller.signal.aborted) setLoading(false);
      }
    },
    [key, request],
  );

  useVisibleRefresh(refresh, { key, interval: 5_000, enabled: !paused });

  // Pause stops automatic updates. Explicit range/page navigation still needs
  // one bounded read; otherwise an uncached selection would load forever.
  useEffect(() => {
    const changed = previousKey.current !== key;
    previousKey.current = key;
    if (!paused || !changed) return;
    let pending: AbortController | undefined;
    let completed = false;
    const read = () => {
      if (document.hidden) {
        pending?.abort();
        pending = undefined;
        return;
      }
      if (pending || completed) return;
      const controller = new AbortController();
      pending = controller;
      void refresh(controller.signal).then(() => {
        if (controller.signal.aborted) return;
        pending = undefined;
        completed = true;
      });
    };
    document.addEventListener("visibilitychange", read);
    read();
    return () => {
      document.removeEventListener("visibilitychange", read);
      pending?.abort();
    };
  }, [key, paused, refresh]);

  useEffect(() => {
    const visibility = () => {
      if (document.hidden) active.current?.abort();
      else setNow(Date.now());
    };
    const timer = window.setInterval(() => {
      if (!document.hidden) setNow(Date.now());
    }, 1_000);
    document.addEventListener("visibilitychange", visibility);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", visibility);
      active.current?.abort();
    };
  }, [key]);

  return {
    value,
    readAt,
    error,
    loading,
    paused,
    now,
    setPaused,
  };
}
