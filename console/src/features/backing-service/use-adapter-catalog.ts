import { useEffect, useState } from "react";
import { controllerRequest } from "@/lib/controller-json-request";
import type { BackingAdapterCatalog } from "./api";

export function useAdapterCatalog(open: boolean) {
  const [catalog, setCatalog] = useState<BackingAdapterCatalog | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    if (!open) return;
    const abort = new AbortController();
    setError(null);
    setCatalog(null);
    void controllerRequest<BackingAdapterCatalog>(
      "/backing-service-adapters",
      200,
      { signal: abort.signal },
    )
      .then((value) => {
        if (!abort.signal.aborted) setCatalog(value);
      })
      .catch((error) => {
        if (!abort.signal.aborted)
          setError(
            error instanceof Error
              ? error.message
              : "Unable to load adapter versions",
          );
      });
    return () => abort.abort();
  }, [open, attempt]);
  return { catalog, error, retry: () => setAttempt((value) => value + 1) };
}
