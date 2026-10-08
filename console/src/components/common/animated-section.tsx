import {
  createContext,
  useContext,
  useLayoutEffect,
  useCallback,
  useState,
  type ReactNode,
} from "react";
import { cn } from "@/lib/utils";

const AnimatedParent = createContext(false);

/** Owns measured expansion, including async content and interrupted transitions.
 * Keep this component mounted; conditional children are retained during exit.
 */
export function AnimatedSection({
  open = true,
  children,
  className,
}: {
  open?: boolean;
  children: ReactNode;
  className?: string;
}) {
  const nested = useContext(AnimatedParent);
  const [retained, setRetained] = useState(open ? children : null);
  const [height, setHeight] = useState<number | undefined>(
    open ? undefined : 0,
  );
  useLayoutEffect(() => {
    if (open) {
      setRetained(children);
      return;
    }
    const timer = window.setTimeout(() => setRetained(null), 220);
    return () => window.clearTimeout(timer);
  }, [open, children]);
  const content = useCallback((element: HTMLDivElement | null) => {
    if (!element) return;
    const measure = () => setHeight(element.getBoundingClientRect().height);
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    // Rebind when React replaces the content node (including Vite refresh).
    measure();
    return () => observer.disconnect();
  }, []);
  return (
    <div
      data-animated-section
      hidden={!open && retained === null}
      data-open={open}
      inert={!open}
      aria-hidden={!open || undefined}
      className={cn(
        "min-w-0 duration-200 ease-out motion-reduce:transition-none",
        nested
          ? "overflow-visible transition-opacity"
          : "overflow-clip transition-[height,opacity]",
        className,
      )}
      style={{ height: open ? height : 0, opacity: open ? 1 : 0 }}
    >
      <AnimatedParent.Provider value>
        <div ref={content} className="flow-root min-w-0">
          {open ? children : retained}
        </div>
      </AnimatedParent.Provider>
    </div>
  );
}
