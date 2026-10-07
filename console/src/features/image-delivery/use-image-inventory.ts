import { useCallback, useEffect, useRef, useState } from "react";
import { listImages, type ImageInventory } from "./api";
import { subscribeTerminalTasks } from "@/features/task/terminal-observation";

export function useImageInventory() {
  const [inventory, setInventory] = useState<ImageInventory | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const request = useRef<AbortController | null>(null);
  const loaded = useRef(false);
  const refresh = useCallback(async () => {
    request.current?.abort();
    const current = new AbortController();
    request.current = current;
    // Background reads must not make an open inspector unavailable.
    setLoading(!loaded.current);
    try {
      const result = await listImages(current.signal);
      if (current.signal.aborted) return;
      loaded.current = true;
      setInventory(result);
      setError(null);
    } catch (error) {
      if (!current.signal.aborted)
        setError(
          error instanceof Error ? error.message : "Unable to read host images",
        );
    } finally {
      if (!current.signal.aborted) setLoading(false);
    }
  }, []);
  useEffect(() => {
    void refresh();
    const unsubscribe = subscribeTerminalTasks((task) => {
      if (
        [
          "image",
          "service",
          "component",
          "agent",
          "runner",
          "release_group",
        ].includes(task.resource_kind ?? "") ||
        ["deploy", "rollback", "destroy"].includes(task.type)
      )
        void refresh();
    });
    return () => {
      unsubscribe();
      request.current?.abort();
    };
  }, [refresh]);
  return { inventory, loading, error, refresh };
}
