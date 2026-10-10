import { useCallback, useRef } from "react";
import { useBeforeUnload, useBlocker } from "react-router-dom";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";

/** Use the router's blocker for links, tabs, and browser Back/Forward alike. */
export function useUnsavedChanges(
  dirty: boolean | (() => boolean),
  onDiscard?: () => void,
) {
  const current = useRef(dirty);
  current.current = dirty;
  const isDirty = () =>
    typeof current.current === "function" ? current.current() : current.current;
  const blocker = useBlocker(
    ({ currentLocation, nextLocation }) =>
      isDirty() &&
      (currentLocation.pathname !== nextLocation.pathname ||
        currentLocation.search !== nextLocation.search),
  );
  useBeforeUnload(
    useCallback((event) => {
      if (!isDirty()) return;
      event.preventDefault();
      event.returnValue = "";
    }, []),
  );
  return {
    dialog: (
      <Dialog
        open={blocker.state === "blocked"}
        onOpenChange={(open) => {
          if (!open && blocker.state === "blocked") blocker.reset();
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Discard unsaved changes?</DialogTitle>
            <DialogDescription>
              Your changes have not been saved.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                if (blocker.state === "blocked") blocker.reset();
              }}
            >
              Keep editing
            </Button>
            <Button
              variant="destructive"
              onClick={() => {
                if (blocker.state === "blocked") {
                  current.current = false;
                  onDiscard?.();
                  blocker.proceed();
                }
              }}
            >
              Discard changes
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    ),
  };
}
