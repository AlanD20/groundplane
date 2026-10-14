import { useUnsavedChanges } from "./use-unsaved-changes";
import {
  createContext,
  useCallback,
  useContext,
  useRef,
  useState,
  type ReactNode,
} from "react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";

export type EditorDiscardReason = "navigation" | "editor";

type Session = {
  id: string;
  element: HTMLElement;
  discard: (reason: EditorDiscardReason) => void;
  dirty: () => boolean;
};
const Sessions = createContext<{
  active: string | null;
  enter: (session: Session) => void;
  leave: (id: string) => void;
  finish: () => void;
} | null>(null);
export const EditorDiscard = createContext<
  ((reason: EditorDiscardReason) => void) | undefined
>(undefined);
export const useEditorSession = () => useContext(Sessions);

/** One editable draft owns the viewport action bar at a time. */
export function EditorSessionProvider({ children }: { children: ReactNode }) {
  const current = useRef<Session | null>(null);
  const next = useRef<Session | null>(null);
  const [active, setActive] = useState<string | null>(null);
  const [confirm, setConfirm] = useState(false);
  const navigation = useUnsavedChanges(
    () => current.current?.dirty() ?? false,
    () => {
      const draft = current.current;
      current.current = null;
      setActive(null);
      draft?.discard("navigation");
    },
  );
  const enter = useCallback((session: Session) => {
    if (current.current?.id === session.id) return;
    if (current.current) {
      if (
        current.current.dirty() ||
        current.current.element.querySelector<HTMLButtonElement>("button")
          ?.disabled
      ) {
        next.current = session;
        setConfirm(true);
        return;
      }
      current.current.discard("editor");
    }
    current.current = session;
    setActive(session.id);
  }, []);
  const leave = useCallback((id: string) => {
    if (next.current?.id === id) {
      next.current = null;
      setConfirm(false);
    }
    if (current.current?.id === id) {
      current.current = null;
      setActive(null);
    }
  }, []);
  const keep = () => {
    const pending = next.current;
    next.current = null;
    setConfirm(false);
    pending?.discard("editor");
    current.current?.element.querySelector<HTMLElement>("button")?.focus();
  };
  return (
    <Sessions.Provider
      value={{
        active,
        enter,
        leave,
        finish: () => {
          current.current = null;
          setActive(null);
        },
      }}
    >
      {children}
      {navigation.dialog}
      <Dialog
        open={confirm}
        onOpenChange={(open) => {
          if (!open) keep();
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Switch editors?</DialogTitle>
            <DialogDescription>
              Finish the current edit or discard it before opening another.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={keep}>
              Keep editing
            </Button>
            <Button
              variant="destructive"
              disabled={
                current.current?.element.querySelector<HTMLButtonElement>(
                  "button",
                )?.disabled
              }
              onClick={() => {
                const pending = next.current;
                if (!pending) return;
                const previous = current.current;
                next.current = null;
                current.current = pending;
                setActive(pending.id);
                setConfirm(false);
                previous?.discard("editor");
              }}
            >
              Discard and switch
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </Sessions.Provider>
  );
}
