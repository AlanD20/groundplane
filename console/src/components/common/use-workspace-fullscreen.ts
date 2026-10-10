import { useEffect, useRef, useState } from "react";

/** Expand within the workspace, retaining navigation and restoring scroll/focus. */
export function useWorkspaceFullscreen(
  onResize?: () => void,
  onExit?: () => void,
) {
  const [fullscreen, setFullscreen] = useState(false);
  const callbacks = useRef({ onResize, onExit });
  callbacks.current = { onResize, onExit };
  useEffect(() => {
    callbacks.current.onResize?.();
    if (!fullscreen) return;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const escape = (event: KeyboardEvent) => {
      if (event.key === "Escape" && !event.defaultPrevented) {
        setFullscreen(false);
        callbacks.current.onExit?.();
      }
    };
    document.addEventListener("keydown", escape);
    return () => {
      document.removeEventListener("keydown", escape);
      document.body.style.overflow = previousOverflow;
    };
  }, [fullscreen]);
  return [fullscreen, setFullscreen] as const;
}
