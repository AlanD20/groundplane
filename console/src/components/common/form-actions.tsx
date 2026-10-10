import {
  createContext,
  useCallback,
  useContext,
  useId,
  useLayoutEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { cn } from "@/lib/utils";

type Entry = { id: string; height: number };
const Dock = createContext<{
  entries: Entry[];
  update: (id: string, height: number | null) => void;
} | null>(null);
export const FormActionsActive = createContext(true);

/** Keeps simultaneous editors visible without overlapping their actions. */
export function FormActionsProvider({ children }: { children: ReactNode }) {
  const [entries, setEntries] = useState<Entry[]>([]);
  const update = useCallback((id: string, height: number | null) => {
    setEntries((current) => {
      const entry = current.find((item) => item.id === id);
      if (height === null)
        return entry ? current.filter((item) => item.id !== id) : current;
      if (entry?.height === height) return current;
      return entry
        ? current.map((item) => (item.id === id ? { id, height } : item))
        : [...current, { id, height }];
    });
  }, []);
  const height = entries.reduce((sum, item) => sum + item.height, 0);
  return (
    <Dock.Provider value={{ entries, update }}>
      <div
        className="contents"
        style={
          { "--viewport-actions-height": `${height}px` } as React.CSSProperties
        }
      >
        {children}
        <div aria-hidden style={{ height }} />
      </div>
    </Dock.Provider>
  );
}

/** Fixed visually, retained in its native form for submit and disabled semantics. */
export function FormActions({
  children,
  className,
  pinned = true,
  status = "Unsaved draft",
  ...props
}: React.ComponentProps<"div"> & { pinned?: boolean; status?: string }) {
  const dock = useContext(Dock);
  const active = useContext(FormActionsActive);
  const fixed = pinned && active && !!dock;
  const id = useId();
  const ref = useRef<HTMLDivElement>(null);
  const [label, setLabel] = useState("");
  const update = dock?.update;
  useLayoutEffect(() => {
    if (!fixed || !update || !ref.current) return;
    const element = ref.current;
    const owner =
      element.closest("form, section, [data-form-owner]") ??
      element.parentElement;
    const heading =
      owner?.querySelector("h2, h3, [data-slot=dialog-title]") ??
      owner?.parentElement?.closest("section")?.querySelector("h2, h3");
    setLabel(owner?.getAttribute("aria-label") ?? heading?.textContent ?? "");
    const measure = () => update(id, element.getBoundingClientRect().height);
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => {
      observer.disconnect();
      update(id, null);
    };
  }, [fixed, id, update]);
  const index = dock?.entries.findIndex((item) => item.id === id) ?? -1;
  const bottom =
    dock?.entries
      .slice(0, Math.max(0, index))
      .reduce((sum, item) => sum + item.height, 0) ?? 0;
  return (
    <div
      ref={ref}
      {...props}
      data-slot="dialog-footer"
      data-viewport-actions={fixed || undefined}
      style={fixed ? { bottom } : undefined}
      className={cn(
        "form-actions flex flex-wrap items-center justify-end gap-2",
        className,
      )}
    >
      {fixed && (
        <div className="mr-auto min-w-0 text-sm">
          {label && <span className="mr-2 font-medium">{label}</span>}
          <span className="text-muted-foreground">{status}</span>
        </div>
      )}
      {children}
    </div>
  );
}
