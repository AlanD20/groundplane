import { editorDraftValue } from "@/lib/editor-draft-value";
import {
  EditorSessionProvider,
  type EditorDiscardReason,
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
export const FormDirty = createContext<boolean | undefined>(undefined);

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
  status,
  onDiscard,
  dirty,
  ...props
}: React.ComponentProps<"div"> & {
  pinned?: boolean;
  status?: string;
  onDiscard?: (reason: EditorDiscardReason) => void;
  dirty?: boolean;
}) {
  const dock = useContext(Dock);
  const active = useContext(FormActionsActive);
  const eligible = pinned && active && !!dock;
  const session = useEditorSession();
  const contextDiscard = useContext(EditorDiscard);
  const discard = onDiscard ?? contextDiscard;
  const contextDirty = useContext(FormDirty);
  const latestDirty = useRef(dirty ?? contextDirty);
  const [observedDirty, setObservedDirty] = useState(false);
  latestDirty.current = dirty ?? contextDirty;
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
            'input:not([form=""]), textarea, select, [role=combobox], button[aria-pressed], [data-draft-editor]',
          ),
        ]
          .filter(
            (element) =>
              element.matches("[data-draft-editor]") ||
              !element.closest("[data-draft-editor]"),
          )
          .map((element) =>
            element.matches("[data-draft-editor]")
              ? editorDraftValue(element)
              : element instanceof HTMLInputElement
                ? [element.value, element.checked]
                : element instanceof HTMLTextAreaElement
                  ? element.value
                  : element instanceof HTMLSelectElement
                    ? element.value
                    : element.hasAttribute("aria-pressed")
                      ? element.getAttribute("aria-pressed")
                      : element.textContent,
          ),
      );
    let initial = snapshot();
    let touched = false;
    setObservedDirty(false);
    const updateDirty = () =>
      setObservedDirty(touched && snapshot() !== initial);
    const observer = new MutationObserver(() => {
      if (!touched) initial = snapshot();
      updateDirty();
    });
    observer.observe(owner, {
      subtree: true,
      childList: true,
      attributes: true,
      characterData: true,
    });
    const begin = () => {
      touched = true;
    };
    owner.addEventListener("pointerdown", begin, true);
    owner.addEventListener("keydown", begin, true);
    const changed = () => {
      touched = true;
      updateDirty();
    };
    owner.addEventListener("input", changed, true);
    owner.addEventListener("change", changed, true);
    enter({
      id,
      element: ref.current,
      discard: (reason) => latestDiscard.current?.(reason),
      dirty: () => latestDirty.current ?? (touched && snapshot() !== initial),
    });
    return () => {
      observer.disconnect();
      owner.removeEventListener("pointerdown", begin, true);
      owner.removeEventListener("keydown", begin, true);
      owner.removeEventListener("input", changed, true);
      owner.removeEventListener("change", changed, true);
      leave(id);
    };
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
          <span className="text-muted-foreground">
            {status ??
              ((dirty ?? contextDirty ?? observedDirty)
                ? "Unsaved changes"
                : "")}
          </span>
        </div>
      )}
      {children}
    </div>
  );
}
