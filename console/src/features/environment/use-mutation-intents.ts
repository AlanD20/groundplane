import { useCallback, useRef } from "react";
import {
  loadEnvironmentMutationIntents,
  persistEnvironmentMutationIntents,
  type EnvironmentMutationIntent,
} from "@/features/environment/operation-storage";
export function useEnvironmentMutationIntents() {
  const environmentMutationIntents = useRef(loadEnvironmentMutationIntents());
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

  return {
    environmentMutationIntents,
    persistEnvironmentMutationIntent,
    clearEnvironmentMutationIntent,
  };
}
