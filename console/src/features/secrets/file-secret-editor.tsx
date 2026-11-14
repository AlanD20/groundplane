import { useEffect, useId, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { CodeEditor } from "@/components/ui/code-editor";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { controllerRequest } from "@/lib/controller-json-request";
import { controllerUpdateRejected } from "@/lib/controller-request-errors";
import { useStore } from "@/lib/store";
import type { operations } from "@/lib/api.generated";
import type { ReusableSecret } from "@/lib/types";

type Metadata =
  operations["secret.show"]["responses"][200]["content"]["application/json"];
type Value =
  operations["secret.reveal"]["responses"][200]["content"]["application/json"];
type Edit =
  operations["secret.edit"]["requestBody"]["content"]["application/json"];
type Submission = { body: Edit; key: string };

export function FileSecretEditor({
  secret,
  onClose,
  onSaved,
}: {
  secret: ReusableSecret;
  onClose: () => void;
  onSaved: () => Promise<void>;
}) {
  const { requireRevealConfirm } = useStore();
  const id = useId();
  const [authorized, setAuthorized] = useState(!requireRevealConfirm);
  const [typed, setTyped] = useState("");
  const [draft, setDraft] = useState("");
  const [initial, setInitial] = useState("");
  const [revision, setRevision] = useState<number>();
  const [loading, setLoading] = useState(authorized);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [reload, setReload] = useState(0);
  const pending = useRef<Submission | null>(null);
  const mounted = useRef(true);
  const path = `/secrets/${encodeURIComponent(secret.id)}`;
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      pending.current = null;
    };
  }, []);
  useEffect(() => {
    if (!authorized) return;
    const abort = new AbortController();
    setLoading(true);
    setError(null);
    setDraft("");
    setInitial("");
    setRevision(undefined);
    void (async () => {
      try {
        const before = await controllerRequest<Metadata>(path, 200, {
          signal: abort.signal,
        });
        const value = await controllerRequest<Value>(`${path}/value`, 200, {
          signal: abort.signal,
        });
        const after = await controllerRequest<Metadata>(path, 200, {
          signal: abort.signal,
        });
        if (
          before.id !== secret.id ||
          before.kind !== "file" ||
          before.revision !== after.revision
        )
          throw new Error(
            "This Secret changed while loading. Reload its current contents.",
          );
        if (!abort.signal.aborted) {
          setDraft(value.value);
          setInitial(value.value);
          setRevision(after.revision);
        }
      } catch (cause) {
        if (!abort.signal.aborted)
          setError(
            cause instanceof Error
              ? cause.message
              : "Unable to load file contents",
          );
      } finally {
        if (!abort.signal.aborted) setLoading(false);
      }
    })();
    return () => abort.abort();
  }, [authorized, path, reload, secret.id]);

  const oversized = new TextEncoder().encode(draft).length > 255 * 1024;
  async function save() {
    if (saving || loading || !revision || oversized || draft === initial)
      return;
    setSaving(true);
    setError(null);
    const submission = pending.current ?? {
      body: { revision, value: draft },
      key: crypto.randomUUID(),
    };
    pending.current = submission;
    try {
      await controllerRequest<Metadata>(path, 200, {
        method: "PATCH",
        body: submission.body,
        idempotencyKey: submission.key,
      });
    } catch (cause) {
      if (mounted.current) {
        const rejected = controllerUpdateRejected(cause);
        if (rejected) pending.current = null;
        const reason =
          cause instanceof Error
            ? cause.message
            : "Unable to save file contents";
        setError(
          rejected
            ? reason
            : `${reason}. Save outcome is unknown; retry the same submission to resolve it.`,
        );
        setSaving(false);
      }
      return;
    }
    pending.current = null;
    void onSaved().catch(() => undefined);
    if (mounted.current) onClose();
  }
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
    >
      <DialogContent className="max-w-5xl">
        <form
          className="min-w-0 space-y-4"
          onSubmit={(event) => {
            event.preventDefault();
            if (!authorized) {
              if (typed === "reveal") setAuthorized(true);
            } else void save();
          }}
        >
          <DialogHeader>
            <DialogTitle>Edit {secret.key}</DialogTitle>
            <DialogDescription>
              {secret.scope === "platform" ? "Platform" : "Project"} file Secret
              · {secret.ref}
            </DialogDescription>
          </DialogHeader>
          {!authorized ? (
            <div className="space-y-2">
              <Label htmlFor={`${id}-confirm`}>
                Type reveal to load the current file contents
              </Label>
              <Input
                id={`${id}-confirm`}
                value={typed}
                onChange={(event) => setTyped(event.target.value)}
                autoFocus
                autoComplete="off"
              />
            </div>
          ) : loading ? (
            <p role="status" className="text-sm text-muted-foreground">
              Loading file contents…
            </p>
          ) : revision ? (
            <CodeEditor
              id={id}
              label={secret.key}
              value={draft}
              onValueChange={setDraft}
              disabled={saving || !!pending.current}
              className="max-h-[60dvh] overflow-auto"
            />
          ) : null}
          {oversized && (
            <p role="alert" className="text-sm text-destructive">
              File contents must not exceed 255 KiB.
            </p>
          )}
          {error && (
            <p
              role="alert"
              className="whitespace-pre-wrap text-sm text-destructive"
            >
              {error}
            </p>
          )}
          <DialogFooter>
            {authorized && (
              <Button
                type="button"
                variant="ghost"
                disabled={loading || saving || !!pending.current}
                onClick={() => setReload((value) => value + 1)}
              >
                Reload
              </Button>
            )}
            <Button type="button" variant="outline" onClick={onClose}>
              Cancel
            </Button>
            {!authorized ? (
              <Button type="submit" disabled={typed !== "reveal"}>
                Load contents
              </Button>
            ) : (
              <Button
                type="submit"
                disabled={
                  loading ||
                  saving ||
                  !revision ||
                  oversized ||
                  draft === initial
                }
              >
                {saving
                  ? "Saving…"
                  : pending.current
                    ? "Retry save"
                    : "Save contents"}
              </Button>
            )}
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
