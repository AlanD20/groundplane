import {
  EditorSessionProvider,
  EditorDiscard,
  useEditorSession,
} from "./editor-session";
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

const Dock = createContext<{
  height: number;
  update: (id: string, height: number | null) => void;
} | null>(null);
export const FormActionsActive = createContext(true);

/** Reserves viewport space for the active editor action bar. */
export function FormActionsProvider({ children }: { children: ReactNode }) {
  const [measurement, setMeasurement] = useState<{
    id: string;
    height: number;
  } | null>(null);
  const update = useCallback((id: string, height: number | null) => {
    setMeasurement((current) => {
      if (height === null) return current?.id === id ? null : current;
      return current?.id === id && current.height === height
        ? current
        : { id, height };
    });
  }, []);
  const height = measurement?.height ?? 0;
  return (
    <EditorSessionProvider>
      <Dock.Provider value={{ height, update }}>
        <div
          className="contents"
          style={
            {
              "--viewport-actions-height": `${height}px`,
            } as React.CSSProperties
          }
        >
          {children}
          <div aria-hidden style={{ height }} />
        </div>
      </Dock.Provider>
    </EditorSessionProvider>
  );
}

/** Fixed visually, retained in its native form for submit and disabled semantics. */
export function FormActions({
  children,
  className,
  pinned = true,
  status = "Unsaved draft",
  onDiscard,
  dirty,
  ...props
}: React.ComponentProps<"div"> & {
  pinned?: boolean;
  status?: string;
  onDiscard?: () => void;
  dirty?: boolean;
}) {
  const dock = useContext(Dock);
  const active = useContext(FormActionsActive);
  const eligible = pinned && active && !!dock;
  const session = useEditorSession();
  const contextDiscard = useContext(EditorDiscard);
  const discard = onDiscard ?? contextDiscard;
  const latestDirty = useRef(dirty);
  latestDirty.current = dirty;
  const latestDiscard = useRef(discard);
  latestDiscard.current = discard;
  const id = useId();
  const ref = useRef<HTMLDivElement>(null);
  const fixed = eligible && (!session || session.active === id);
  const enter = session?.enter;
  const leave = session?.leave;
  useLayoutEffect(() => {
    if (!eligible || !enter || !leave || !ref.current) return;
    const owner =
      ref.current.closest("form, section, [data-form-owner]") ??
      ref.current.parentElement!;
    const snapshot = () =>
      JSON.stringify(
        [
          ...owner.querySelectorAll(
            'input:not([form=""]), textarea, [role=combobox], .cm-content',
          ),
        ].map((element) =>
          element instanceof HTMLInputElement
            ? [element.value, element.checked]
            : element instanceof HTMLTextAreaElement
              ? element.value
              : element.textContent,
        ),
      );
    const initial = snapshot();
    enter({
      id,
      element: ref.current,
      discard: () => latestDiscard.current?.(),
      dirty: () =>
        latestDirty.current ??
        (!!owner.querySelector(".cm-content") || snapshot() !== initial),
    });
    return () => leave(id);
  }, [eligible, enter, leave, id]);
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
  return (
    <div
      ref={ref}
      {...props}
      hidden={eligible && !!session && session.active !== id}
      data-slot="dialog-footer"
      data-viewport-actions={fixed || undefined}
      style={fixed ? { bottom: 0 } : undefined}
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
