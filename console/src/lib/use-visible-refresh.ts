import { useEffect, useRef } from "react";
import { subscribeResourceChanges } from "./resource-observation";

// One serialized read per mounted surface. Hidden tabs do no background work;
// focus, reconnect and mutations refresh without remounting forms or tables.
export function useVisibleRefresh(
  refresh: (signal: AbortSignal) => Promise<unknown>,
  {
    key = "",
    interval = 10_000,
    enabled = true,
  }: {
    key?: string;
    interval?: number;
    enabled?: boolean;
  } = {},
) {
  const latest = useRef(refresh);
  latest.current = refresh;
  useEffect(() => {
    if (!enabled) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let running = false;
    let pending = false;
    const schedule = (delay: number) => {
      if (timer) clearTimeout(timer);
      if (!controller.signal.aborted)
        timer = setTimeout(() => void run(), delay);
    };
    const run = async () => {
      if (controller.signal.aborted || document.hidden) return;
      if (running) {
        pending = true;
        return;
      }
      running = true;
      pending = false;
      try {
        await latest.current(controller.signal);
      } catch {
        /* Request owners retain their data and expose their read errors. */
      } finally {
        running = false;
        if (!controller.signal.aborted) schedule(pending ? 100 : interval);
      }
    };
    const invalidate = () => {
      if (running) pending = true;
      else schedule(100);
    };
    const unsubscribe = subscribeResourceChanges(invalidate);
    document.addEventListener("visibilitychange", invalidate);
    window.addEventListener("focus", invalidate);
    window.addEventListener("online", invalidate);
    void run();
    return () => {
      controller.abort();
      if (timer) clearTimeout(timer);
      unsubscribe();
      document.removeEventListener("visibilitychange", invalidate);
      window.removeEventListener("focus", invalidate);
      window.removeEventListener("online", invalidate);
    };
  }, [key, interval, enabled]);
}
