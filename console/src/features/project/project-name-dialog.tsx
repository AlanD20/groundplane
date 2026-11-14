import { useState } from "react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useStore } from "@/lib/store";
import type { Project } from "@/lib/types";

export function ProjectNameDialog({
  project,
  onClose,
}: {
  project: Project;
  onClose: () => void;
}) {
  const store = useStore();
  const [name, setName] = useState(project.name);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  async function save() {
    if (busy || !name.trim() || name.trim() === project.name) return;
    setBusy(true);
    setError(null);
    try {
      await store.editProject(project.id, name.trim());
      onClose();
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "Unable to save display name",
      );
    } finally {
      setBusy(false);
    }
  }
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open && !busy) onClose();
      }}
    >
      <DialogContent>
        <form
          onSubmit={(event) => {
            event.preventDefault();
            void save();
          }}
          className="space-y-4"
        >
          <DialogHeader>
            <DialogTitle>Edit display name</DialogTitle>
          </DialogHeader>
          <div className="space-y-2">
            <Label htmlFor="project-display-name">Name</Label>
            <Input
              id="project-display-name"
              autoFocus
              value={name}
              disabled={busy}
              onChange={(event) => setName(event.target.value)}
            />
          </div>
          {error && (
            <p role="alert" className="text-sm text-destructive">
              {error}
            </p>
          )}
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              disabled={busy}
              onClick={onClose}
            >
              Cancel
            </Button>
            <Button
              type="submit"
              disabled={busy || !name.trim() || name.trim() === project.name}
            >
              {busy ? "Saving…" : "Save name"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
