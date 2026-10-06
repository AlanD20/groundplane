import { useId, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle,
} from "@/components/ui/dialog";
import { useStore } from "@/lib/store";
import { newULID } from "@/lib/utils";
import type { Environment } from "@/lib/types";
import type { RecoveryPoint } from "./types";
import { backupSourceLabel } from "./environment-backup-projection";

export function RestoreDialog({ env, point, onClose, onTask }: {
  env: Environment;
  point: RecoveryPoint;
  onClose: () => void;
  onTask: (taskId: string) => void;
}) {
  const store = useStore();
  const identityId = useId();
  const [identity, setIdentity] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const pending = useRef(false);
  const key = useRef(newULID());
  const policy = store.getBackupPolicyState(env.id).policy;
  const needsIdentity = point.encrypted && point.keyEra !== policy.keyEra;
  const target = backupSourceLabel(store, env, {
    id: point.sourceId, kind: point.sourceKind, targetId: point.targetId,
  });
  const close = () => {
    setIdentity("");
    onClose();
  };
  const restore = async () => {
    if (pending.current) return;
    pending.current = true;
    setBusy(true);
    setError(null);
    try {
      const taskId = await store.restoreBackup(env.id, {
        source_id: point.sourceId,
        recovery_point_id: point.id,
        ...(identity ? { age_identity: identity } : {}),
      }, key.current);
      setIdentity("");
      onTask(taskId);
      onClose();
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : "Unable to start Restore");
    } finally {
      pending.current = false;
      setBusy(false);
    }
  };
  return (
    <Dialog open onOpenChange={open => { if (!open) close(); }}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Restore {target}</DialogTitle>
          <DialogDescription>
            Replace the current {point.sourceKind === "config" ? "Entries and their values" : point.sourceKind === "volume" ? "Volume contents" : "database contents"} in {env.name} with this Recovery Point.
          </DialogDescription>
        </DialogHeader>
        <dl className="grid gap-3 rounded-lg border border-border bg-surface p-4 text-sm">
          <div><dt className="text-muted-foreground">Recovery Point</dt><dd className="break-all font-mono text-xs">{point.id}</dd></div>
          <div><dt className="text-muted-foreground">Captured</dt><dd>{new Date(point.createdAt).toLocaleString()}</dd></div>
          <div><dt className="text-muted-foreground">Original storage</dt><dd className="break-all text-xs">s3://{point.connectorBucket}/{point.connectorPrefix}</dd><dd className="break-all text-xs text-muted-foreground">{point.connectorId}</dd></div>
          <div><dt className="text-muted-foreground">Target</dt><dd>{target}</dd><dd className="break-all font-mono text-xs text-muted-foreground">{point.targetId}</dd></div>
        </dl>
        <p className="rounded-lg border border-warning/30 bg-warning/10 p-3 text-sm text-warning">
          {point.sourceKind === "config"
            ? "Entries missing from this backup will be removed. Managed files are updated; running Services load the restored values on their next Deploy."
            : "This overwrites current data and temporarily stops Services using this target. Previously running Services restart after Restore finishes."}
        </p>
        {needsIdentity && <div className="space-y-2">
          <Label htmlFor={identityId}>Encryption identity for era {point.keyEra}</Label>
          <Input id={identityId} type="password" autoComplete="off" spellCheck={false} value={identity}
            disabled={busy} onChange={event => { setIdentity(event.target.value); key.current = newULID(); }} />
          <p className="text-xs text-muted-foreground">Use the identity exported before key rotation. GP retains it only for this attempt.</p>
        </div>}
        {error && <p role="alert" className="text-sm text-destructive">{error}</p>}
        <DialogFooter>
          <Button variant="outline" onClick={close}>Cancel</Button>
          <Button variant="destructive" disabled={busy || (needsIdentity && !identity.trim())} onClick={() => void restore()}>
            {busy ? "Starting Restore…" : "Overwrite and restore"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
