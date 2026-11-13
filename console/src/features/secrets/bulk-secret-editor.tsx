import { useMemo, useRef, useState } from "react";
import {
  ResourceForm,
  ResourceFormHeader,
  ResourceFormTitle,
  ResourceFormFooter,
} from "@/components/common/resource-form";
import { Button } from "@/components/ui/button";
import { CodeEditor } from "@/components/ui/code-editor";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useStore } from "@/lib/store";
import { controllerRequest } from "@/lib/controller-json-request";
import { controllerUpdateRejected } from "@/lib/controller-request-errors";
import type { operations } from "@/lib/api.generated";
import type { ReusableSecret } from "@/lib/types";
import {
  parseBulkEnvironmentEntries,
  formatBulkEnvironmentEntries,
} from "@/features/entry/bulk-entry-input";

type SecretResponse =
  operations["secret.show"]["responses"][200]["content"]["application/json"];
type CreateRequest =
  operations["secret.create"]["requestBody"]["content"]["application/json"];
type EditRequest =
  operations["secret.edit"]["requestBody"]["content"]["application/json"];
type Write = {
  key: string;
  value: string;
  id?: string;
  revision?: number;
  idempotencyKey: string;
  done: boolean;
};
const maximumBytes = 1024 * 1024;

// Bulk editing orchestrates ordinary scope-bound create/value-edit operations.
// Each accepted write is retained if a later one fails; retries keep exact inputs.
export function BulkSecretEditor({
  open,
  onOpenChange,
  secrets,
  projectId,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  secrets: ReusableSecret[];
  projectId?: string;
}) {
  const store = useStore();
  const [text, setText] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [completed, setCompleted] = useState<string[]>([]);
  const [uncertain, setUncertain] = useState(false);
  const pending = useRef<Write[] | null>(null);
  const scope = projectId ? "Project" : "Platform";
  const owned = useMemo(
    () =>
      secrets.filter((secret) =>
        projectId
          ? secret.scope === "project" && secret.projectId === projectId
          : secret.scope === "platform",
      ),
    [secrets, projectId],
  );
  const preview = useMemo(() => {
    if (!text.trim()) return { entries: [], error: null };
    try {
      if (new TextEncoder().encode(text).length > maximumBytes)
        throw new Error("Import at most 1 MiB per batch.");
      const entries = parseBulkEnvironmentEntries(text);
      if (entries.length > 256)
        throw new Error("Import at most 256 keys per batch.");
      for (const entry of entries) {
        if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(entry.key))
          throw new Error(`${entry.key}: use an environment-variable key.`);
        if (new TextEncoder().encode(entry.value).length > 255 * 1024)
          throw new Error(`${entry.key}: value exceeds 255 KiB.`);
        if (owned.find((secret) => secret.key === entry.key)?.kind === "file")
          throw new Error(
            `${entry.key} is a file Secret; .env import cannot replace its kind.`,
          );
      }
      return { entries, error: null };
    } catch (cause) {
      return {
        entries: [],
        error: cause instanceof Error ? cause.message : "Unable to parse input",
      };
    }
  }, [text, owned]);
  function changeText(next: string) {
    if (pending.current || busy) return;
    setText(next);
    setError(null);
    setCompleted([]);
  }
  async function loadValues() {
    setBusy(true);
    setError(null);
    try {
      const values: { key: string; value: string }[] = [];
      const env = owned.filter((secret) => secret.kind === "env");
      if (env.length > 256)
        throw new Error(
          "There are more than 256 keys; paste a smaller batch instead.",
        );
      for (const secret of env) {
        const value = await store.revealReusableSecret(secret.id);
        values.push({ key: secret.key, value });
      }
      const next = formatBulkEnvironmentEntries(values);
      if (new TextEncoder().encode(next).length > maximumBytes)
        throw new Error(
          "Existing values exceed the 1 MiB editor limit; paste a smaller batch instead.",
        );
      setText(next);
      setCompleted([]);
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : "Unable to load existing values",
      );
    } finally {
      setBusy(false);
    }
  }
  async function save() {
    if (busy || preview.error || !preview.entries.length) return;
    setBusy(true);
    setError(null);
    setUncertain(false);
    try {
      if (!pending.current)
        pending.current = preview.entries.map((entry) => {
          const existing = owned.find((secret) => secret.key === entry.key);
          if (existing && !existing.revision)
            throw new Error(
              `${entry.key}: refresh Secrets before replacing an existing value; its revision is missing.`,
            );
          return {
            ...entry,
            id: existing?.id,
            revision: existing?.revision,
            idempotencyKey: crypto.randomUUID(),
            done: false,
          };
        });
      for (const write of pending.current) {
        if (write.done) continue;
        try {
          if (write.id) {
            if (!write.revision)
              throw new Error(
                "Refresh Secrets before replacing an existing value; its revision is missing.",
              );
            const body: EditRequest = {
              revision: write.revision,
              value: write.value,
            };
            await controllerRequest<SecretResponse>(
              `/secrets/${encodeURIComponent(write.id)}`,
              200,
              { method: "PATCH", body, idempotencyKey: write.idempotencyKey },
            );
          } else {
            const body: CreateRequest = {
              key: write.key,
              kind: "env_var",
              value: write.value,
              ...(projectId ? { project_id: projectId } : { platform: true }),
            };
            await controllerRequest<SecretResponse>("/secrets", 201, {
              method: "POST",
              body,
              idempotencyKey: write.idempotencyKey,
            });
          }
          write.done = true;
          write.value = "";
          setCompleted((keys) => [...keys, write.key]);
        } catch (cause) {
          const known = controllerUpdateRejected(cause);
          setUncertain(!known);
          const reason =
            cause instanceof Error ? cause.message : "Write failed";
          throw new Error(
            `${write.key}: ${reason}. Earlier successful keys remain saved.${known ? " Refresh and review the remaining draft." : " Retry to resolve this exact submission."}`,
          );
        }
      }
      pending.current = null;
      setText("");
      await store.refreshReusableSecrets();
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "Unable to save Secrets",
      );
    } finally {
      setBusy(false);
    }
  }
  async function discardPending() {
    if (busy || uncertain) return;
    const remaining = pending.current
      ?.filter((write) => !write.done)
      .map(({ key, value }) => ({ key, value }));
    pending.current = null;
    if (remaining) setText(formatBulkEnvironmentEntries(remaining));
    await store.refreshReusableSecrets();
    setError(null);
  }
  function close(next: boolean) {
    if (busy || uncertain) return;
    onOpenChange(next);
    if (!next) {
      setText("");
      setError(null);
      setCompleted([]);
      pending.current = null;
    }
  }
  return (
    <ResourceForm open={open} onOpenChange={close}>
      <ResourceFormHeader>
        <ResourceFormTitle>
          Import / edit .env · {scope} Secrets
        </ResourceFormTitle>
      </ResourceFormHeader>
      <div className="space-y-4">
        <p className="text-sm text-muted-foreground">
          Pasted keys create or replace {scope.toLowerCase()} env-var Secrets.
          Omitted keys and file Secrets stay unchanged. Values remain hidden
          unless you explicitly load them below.
        </p>
        <div className="flex flex-wrap items-center gap-3">
          <Input
            aria-label="Import .env file"
            type="file"
            accept=".env,text/plain"
            disabled={busy || !!pending.current}
            onChange={async (event) => {
              const file = event.target.files?.[0];
              if (!file) return;
              if (file.size > maximumBytes) {
                setError("Import at most 1 MiB per batch.");
                event.target.value = "";
                return;
              }
              try {
                changeText(await file.text());
              } catch {
                setError("Unable to read selected file");
              }
              event.target.value = "";
            }}
          />
          <Button
            variant="outline"
            size="sm"
            disabled={
              busy ||
              !!pending.current ||
              !owned.some((secret) => secret.kind === "env")
            }
            onClick={loadValues}
          >
            Reveal existing values in editor
          </Button>
        </div>
        <CodeEditor
          id={`bulk-${projectId || "platform"}-secrets`}
          label="Secret values · KEY=value"
          language="shell"
          value={text}
          onValueChange={changeText}
          disabled={busy || !!pending.current}
        />
        {preview.error && (
          <p role="alert" className="text-sm text-destructive">
            {preview.error}
          </p>
        )}
        {preview.entries.length > 0 && (
          <div className="flex flex-wrap gap-2">
            {preview.entries.map((entry) => (
              <Badge key={entry.key} variant="muted">
                {owned.some((secret) => secret.key === entry.key)
                  ? "Update"
                  : "Create"}{" "}
                {entry.key}
              </Badge>
            ))}
          </div>
        )}
        {completed.length > 0 && (
          <p role="status" className="text-sm">
            Saved {completed.length} keys: {completed.join(", ")}
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
      </div>
      <ResourceFormFooter>
        <Button
          variant="outline"
          disabled={busy || uncertain}
          onClick={() => close(false)}
        >
          Close
        </Button>
        {pending.current && !uncertain && (
          <Button variant="outline" disabled={busy} onClick={discardPending}>
            Review remaining keys
          </Button>
        )}
        <Button
          disabled={busy || !!preview.error || !preview.entries.length}
          onClick={save}
        >
          {busy
            ? "Saving…"
            : uncertain
              ? "Resolve submission"
              : "Save submitted keys"}
        </Button>
      </ResourceFormFooter>
    </ResourceForm>
  );
}
