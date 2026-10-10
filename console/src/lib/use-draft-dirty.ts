import { useEffect, useRef } from "react";

export function useDraftDirty(
  value: unknown,
  onDirtyChange?: (dirty: boolean) => void,
) {
  const snapshot = JSON.stringify(value);
  const initial = useRef(snapshot);
  useEffect(() => {
    onDirtyChange?.(snapshot !== initial.current);
  }, [snapshot, onDirtyChange]);
}
