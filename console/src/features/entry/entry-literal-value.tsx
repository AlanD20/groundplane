import { useState } from "react";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { CodeEditor } from "@/components/ui/code-editor";
import { BulkRevealConfirmation } from "@/components/common/bulk-reveal-confirmation";
import { useStore } from "@/lib/store";
import type { EnvironmentEntry } from "@/lib/types";

export function EntryLiteralValue({
  entry, file, secret, value, onChange, ready, onReadyChange,
}: {
  entry: EnvironmentEntry | null;
  file: boolean;
  secret: boolean;
  value: string;
  onChange: (value: string) => void;
  ready: boolean;
  onReadyChange: (ready: boolean) => void;
}) {
  const store = useStore();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [confirm, setConfirm] = useState(false);
  const protectedValue = entry && (entry.secret || entry.encryption === "age");
  async function load() {
    if (!entry) return;
    setBusy(true);
    setError(null);
    try {
      onChange(await store.revealEntry(entry.id));
      onReadyChange(true);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Unable to load the current value.");
    } finally {
      setBusy(false);
    }
  }
  function replace(next: string) {
    onChange(next);
    onReadyChange(true);
  }
  return (
    <div className={`flex flex-col gap-1.5 ${file ? "md:col-span-2" : ""}`}>
      <Label htmlFor="entry-value">{file ? "Contents" : "Value"}</Label>
      {file ? <CodeEditor id="entry-value" label="File contents" value={value}
        disabled={busy} onValueChange={replace} /> :
        <Input id="entry-value" type={secret ? "password" : "text"} value={value}
          disabled={busy} autoComplete="off" onChange={(event) => replace(event.target.value)} />}
      {protectedValue && <>
        <p className="text-xs text-muted-foreground">
          Load the current value to preserve it, or enter a replacement.
          {!ready && " The current value has not been loaded."}
        </p>
        <div className="flex flex-wrap gap-2">
          <Button type="button" variant="outline" size="sm" disabled={busy}
            onClick={() => {
              if (entry.secret && store.requireRevealConfirm) setConfirm(true);
              else void load();
            }}>{busy ? "Loading…" : "Load current value"}</Button>
          <Button type="button" variant="outline" size="sm" disabled={busy}
            onClick={() => replace("")}>Use empty value</Button>
        </div>
      </>}
      {error && <p role="alert" className="text-sm text-destructive">{error}</p>}
      <BulkRevealConfirmation open={confirm} onOpenChange={setConfirm}
        subject={entry?.key ?? entry?.path ?? "the current value"} onConfirm={() => void load()} />
    </div>
  );
}
