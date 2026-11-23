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
    let deadline = Infinity;
    let running = false;
    let pending = false;
    const cancelTimer = () => {
      if (timer !== undefined) clearTimeout(timer);
      timer = undefined;
      deadline = Infinity;
    };
    const schedule = (delay: number) => {
      if (controller.signal.aborted || document.hidden) return;
      const nextDeadline = performance.now() + delay;
      // New events can bring the read forward, but a burst cannot postpone it.
      if (nextDeadline >= deadline) return;
      cancelTimer();
      deadline = nextDeadline;
      timer = setTimeout(() => {
        cancelTimer();
        void run();
      }, delay);
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
      if (document.hidden) return;
      if (running) pending = true;
      else schedule(100);
    };
    const visibility = () => {
      if (document.hidden) cancelTimer();
      else invalidate();
    };
    const unsubscribe = subscribeResourceChanges(invalidate);
    document.addEventListener("visibilitychange", visibility);
    window.addEventListener("focus", invalidate);
    window.addEventListener("online", invalidate);
    void run();
    return () => {
      controller.abort();
      cancelTimer();
      unsubscribe();
      document.removeEventListener("visibilitychange", visibility);
      window.removeEventListener("focus", invalidate);
      window.removeEventListener("online", invalidate);
    };
  }, [key, interval, enabled]);
}
