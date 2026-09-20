import { useCallback, useRef } from "react";
import {
  loadEnvironmentMutationIntents,
  persistEnvironmentMutationIntents,
  type EnvironmentMutationIntent,
} from "@/lib/environment-storage";
export function useEnvironmentMutationIntents() {
  const environmentMutationIntents = useRef(loadEnvironmentMutationIntents());
  const environmentTaskControllers = useRef(new Set<AbortController>());
  const persistEnvironmentMutationIntent = useCallback(
    (intent: EnvironmentMutationIntent) => {
      environmentMutationIntents.current.set(intent.key, intent);
      persistEnvironmentMutationIntents(environmentMutationIntents.current);
    },
    [],
  );
  const clearEnvironmentMutationIntent = useCallback((key: string) => {
    environmentMutationIntents.current.delete(key);
    persistEnvironmentMutationIntents(environmentMutationIntents.current);
  }, []);

  const close = useCallback(() => {
    for (const controller of environmentTaskControllers.current)
      controller.abort();
    environmentTaskControllers.current.clear();
  }, []);
  return {
    environmentMutationIntents,
    environmentTaskControllers,
    persistEnvironmentMutationIntent,
    clearEnvironmentMutationIntent,
    close,
  };
}
