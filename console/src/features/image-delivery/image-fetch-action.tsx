import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import type { operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { newULID } from "@/lib/utils";
import { Loader2 } from "lucide-react";
import { useEffect, useId, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";

type Accepted =
  operations["image.fetch"]["responses"][202]["content"]["application/json"];

export function ImageFetchAction({
  open,
  onOpenChange,
  initialImage,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  initialImage?: string;
}) {
  const [search] = useSearchParams();
  const seed = initialImage ?? search.get("fetch") ?? "";
  const [image, setImage] = useState(seed);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const inputId = useId();
  const intent = useRef<{ image: string; key: string } | null>(null);
  const pending = useRef(false);
  const wasOpen = useRef(false);
  useEffect(() => {
    if (open && !wasOpen.current) {
      setImage(seed);
      setError("");
    }
    wasOpen.current = open;
  }, [open, seed]);

  async function dispatch() {
    const requested = image.trim();
    if (!requested || pending.current) return;
    if (!intent.current || intent.current.image !== requested)
      intent.current = { image: requested, key: newULID() };
    pending.current = true;
    setBusy(true);
    setError("");
    try {
      await controllerRequest<Accepted>("/images/fetch", 202, {
        method: "POST",
        body: { image: requested },
        idempotencyKey: intent.current.key,
      });
      // The transport publishes acceptance to Task notifications. Inventory
      // refreshes on terminal Task observation, independently of this popup.
      intent.current = null;
      onOpenChange(false);
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "Unable to fetch image",
      );
    } finally {
      pending.current = false;
      setBusy(false);
    }
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!pending.current) onOpenChange(next);
      }}
    >
      <DialogContent className="max-w-lg">
        <form
          onKeyDown={(event) => event.stopPropagation()}
          onSubmit={(event) => {
            event.preventDefault();
            event.stopPropagation();
            void dispatch();
          }}
        >
          <DialogHeader>
            <DialogTitle>Fetch image</DialogTitle>
          </DialogHeader>
          <div className="mt-4 space-y-2">
            <Label htmlFor={inputId}>Registry image</Label>
            <Input
              id={inputId}
              value={image}
              disabled={busy}
              autoFocus
              onChange={(event) => setImage(event.target.value)}
              placeholder="nginx:latest or ghcr.io/org/app:tag"
            />
            {error && (
              <p role="alert" className="break-words text-sm text-destructive">
                {error}
              </p>
            )}
          </div>
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              disabled={busy}
              onClick={() => onOpenChange(false)}
            >
              Cancel
            </Button>
            <Button type="submit" disabled={busy || !image.trim()}>
              {busy && <Loader2 className="size-4 animate-spin" />}
              {busy ? "Starting…" : "Fetch image"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
