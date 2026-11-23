"use client";

import { useId, useState } from "react";
import { Eye, EyeOff } from "lucide-react";
import { CopyButton } from "./copy-button";
import { useStore } from "@/lib/store";
import { cn } from "@/lib/utils";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { CodeEditor } from "@/components/ui/code-editor";

// Masked value with an optional typed-confirmation reveal (never cached,
// never logged). The typed confirmation is a PLATFORM-wide preference — off by
// default (Settings → Preferences → "Require typed confirmation to reveal
// secrets"), so revealing is one click unless the operator opted in.
export function RevealValue({
  loadValue,
  label,
  className,
  confirmWord = "reveal",
  sensitive = true,
  file = false,
}: {
  loadValue: () => Promise<string>;
  label?: string;
  className?: string;
  confirmWord?: string;
  sensitive?: boolean;
  file?: boolean;
}) {
  const viewerId = useId();
  const { requireRevealConfirm } = useStore();
  const [open, setOpen] = useState(false);
  const [revealed, setRevealed] = useState(false);
  const [loadedValue, setLoadedValue] = useState<string>();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [typed, setTyped] = useState("");

  const displayedValue = loadedValue;
  const expanded =
    file ||
    (loadedValue?.includes("\n") ?? false) ||
    (loadedValue?.length ?? 0) > 160;
  const showInlineValue = revealed && !expanded;

  async function reveal(): Promise<boolean> {
    setError(null);
    setLoading(true);
    try {
      setLoadedValue(await loadValue());
      setRevealed(true);
      return true;
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "Unable to reveal value",
      );
      return false;
    } finally {
      setLoading(false);
    }
  }

  function hide() {
    setRevealed(false);
    setLoadedValue(undefined);
    setError(null);
  }

  return (
    <div className={cn("min-w-0 max-w-full space-y-1", className)}>
      <div className="flex min-w-0 items-center gap-1">
        {!file && (
          <>
            <code
              className={cn(
                "h-9 min-w-0 flex-1 overflow-x-auto rounded-md px-3 py-2 font-mono text-sm whitespace-pre",
                (showInlineValue || sensitive) && "bg-muted",
              )}
              aria-hidden={!showInlineValue && !sensitive ? true : undefined}
            >
              {showInlineValue ? (
                displayedValue === "" ? (
                  <span className="font-sans italic text-muted-foreground">
                    Empty value
                  </span>
                ) : (
                  displayedValue
                )
              ) : sensitive ? (
                <span aria-label="Hidden secret">{"•".repeat(12)}</span>
              ) : null}
            </code>
            {showInlineValue ? (
              <CopyButton
                className="size-8 shrink-0"
                value={displayedValue ?? ""}
              />
            ) : (
              <span className="size-8 shrink-0" aria-hidden />
            )}
          </>
        )}
        <Button
          variant="ghost"
          size="content"
          type="button"
          onClick={() => {
            if (loading) return;
            if (revealed) hide();
            else if (sensitive && requireRevealConfirm) {
              setTyped("");
              setOpen(true);
            } else void reveal();
          }}
          className={cn(
            "h-9 shrink-0 gap-1 rounded-md px-1.5 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground",
            file ? "w-24" : "w-20",
          )}
          aria-label={
            revealed
              ? "Hide value"
              : file
                ? "View file"
                : sensitive
                  ? "Reveal value"
                  : "Show value"
          }
          aria-disabled={loading}
          aria-busy={loading}
        >
          {revealed ? (
            <EyeOff className="size-3.5" />
          ) : (
            <Eye className="size-3.5" />
          )}
          {loading
            ? "Loading…"
            : revealed
              ? "Hide"
              : file
                ? "View file"
                : sensitive
                  ? "Reveal"
                  : "Show"}
        </Button>
      </div>
      {error && (
        <p
          role="alert"
          className="text-xs text-destructive [overflow-wrap:anywhere]"
        >
          {error}
        </p>
      )}
      <Dialog
        open={revealed && expanded}
        onOpenChange={(next) => {
          if (!next) hide();
        }}
      >
        <DialogContent className="max-w-5xl" submitOnEnter={false}>
          <DialogHeader>
            <DialogTitle>{label ?? "Secret value"}</DialogTitle>
            <DialogDescription>
              {file ? "File contents" : sensitive ? "Secret value" : "Value"} ·
              Read only
            </DialogDescription>
          </DialogHeader>
          {revealed && expanded && (
            <CodeEditor
              id={viewerId}
              label={label ?? "Secret value"}
              value={displayedValue ?? ""}
              readOnly
              className="max-h-[60dvh] overflow-auto"
            />
          )}
          <DialogFooter>
            <CopyButton value={displayedValue ?? ""} />
            <Button variant="outline" onClick={hide}>
              Close
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Reveal {label ?? "secret"}</DialogTitle>
            <DialogDescription>Type to reveal this secret.</DialogDescription>
          </DialogHeader>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="reveal-confirm">Confirmation</Label>
            <Input
              id="reveal-confirm"
              autoFocus
              value={typed}
              onChange={(e) => setTyped(e.target.value)}
              placeholder={confirmWord}
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button
              disabled={typed !== confirmWord || loading}
              onClick={() => {
                void reveal().then((success) => {
                  if (success) setOpen(false);
                });
              }}
            >
              {loading ? "Revealing…" : "Reveal value"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
