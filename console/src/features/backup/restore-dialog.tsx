import { workspaceSectionClassName } from "@/components/common/workspace-section";
import { useEffect, useId, useRef, useState } from "react";
import type { components } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { controllerUpdateRejected } from "@/lib/controller-request-errors";
import { Checkbox } from "@/components/ui/checkbox";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useStore } from "@/lib/store";
import { newULID } from "@/lib/utils";
import type { Environment } from "@/lib/types";
import type { RecoveryPoint } from "./types";
import { backupSourceLabel } from "./environment-backup-projection";

export function RestoreDialog({
  env,
  point,
  onClose,
  onTask,
}: {
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
	const [review, setReview] = useState<components["schemas"]["RestorePreview"] | null>(null);
	const [reviewing, setReviewing] = useState(false);
	const [acknowledged, setAcknowledged] = useState(false);
	const retained = useRef<components["schemas"]["RestoreRequest"] | null>(null);
  const policy = store.getBackupPolicyState(env.id).policy;
  const needsIdentity = point.encrypted && point.keyEra !== policy.keyEra;
	const reviewTarget = async () => {
		if (pending.current || reviewing || retained.current) return;
		setReviewing(true);
		setError(null);
		setAcknowledged(false);
		try {
			const result = await controllerRequest<components["schemas"]["RestorePreview"]>(
				`/environments/${encodeURIComponent(env.id)}/restore/preview`, 200,
				{method: "POST", body: {source_id: point.sourceId, recovery_point_id: point.id, ...(identity ? {age_identity: identity} : {})}},
			);
			setReview(result);
			key.current = newULID();
		} catch (failure) {
			setReview(null);
			setError(failure instanceof Error ? failure.message : "Unable to review Restore target");
		} finally { setReviewing(false); }
	};
	useEffect(() => {
		if (point.sourceKind !== "attach" || needsIdentity) return;
		let active = true;
		const abort = new AbortController();
		setReviewing(true);
		void controllerRequest<components["schemas"]["RestorePreview"]>(`/environments/${encodeURIComponent(env.id)}/restore/preview`, 200,
			{method: "POST", signal: abort.signal, body: {source_id: point.sourceId, recovery_point_id: point.id}})
			.then(result => { if (active) setReview(result); })
			.catch(failure => { if (active) setError(failure instanceof Error ? failure.message : "Unable to review Restore target"); })
			.finally(() => { if (active) setReviewing(false); });
		return () => { active = false; abort.abort(); };
	}, [env.id, point.id, point.sourceId, point.sourceKind, needsIdentity]);
  const target = backupSourceLabel(store, env, {
    id: point.sourceId,
    kind: point.sourceKind,
    targetId: point.targetId,
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
		if (point.sourceKind === "attach" && (!review?.database || review.database.version_difference && !acknowledged)) throw new Error("Review the target versions and acknowledge any difference before Restore");
		const body = retained.current ?? {
			source_id: point.sourceId, recovery_point_id: point.id,
			...(identity ? {age_identity: identity} : {}),
			...(review?.database ? {version_review_sha256: review.database.review_sha256, acknowledge_version_difference: acknowledged} : {}),
		};
		retained.current = body;
      const taskId = await store.restoreBackup(
        env.id,
		body,
        key.current,
      );
      setIdentity("");
      onTask(taskId);
      onClose();
    } catch (failure) {
		if (controllerUpdateRejected(failure)) { retained.current = null; key.current = newULID(); setReview(null); setAcknowledged(false); }
      setError(
        failure instanceof Error ? failure.message : "Unable to start Restore",
      );
    } finally {
      pending.current = false;
      setBusy(false);
    }
  };
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) close();
      }}
    >
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Restore {target}</DialogTitle>
          <DialogDescription>
            Replace the current{" "}
            {point.sourceKind === "config"
              ? "Entries and their values"
              : point.sourceKind === "volume"
                ? "Volume contents"
                : "database contents"}{" "}
            in {env.name} with this Recovery Point.
          </DialogDescription>
        </DialogHeader>
        <dl
          className={workspaceSectionClassName(
            false,
            "grid divide-y divide-border text-sm [&>div]:py-3",
          )}
        >
          <div>
            <dt className="text-muted-foreground">Recovery Point</dt>
            <dd className="break-all font-mono text-xs">{point.id}</dd>
          </div>
          <div>
            <dt className="text-muted-foreground">Captured</dt>
            <dd>{new Date(point.createdAt).toLocaleString()}</dd>
          </div>
          <div>
            <dt className="text-muted-foreground">Original storage</dt>
            <dd className="break-all text-xs">
              s3://{point.connectorBucket}/{point.connectorPrefix}
            </dd>
            <dd className="break-all text-xs text-muted-foreground">
              {point.connectorId}
            </dd>
          </div>
          <div>
            <dt className="text-muted-foreground">Target</dt>
            <dd>{target}</dd>
            <dd className="break-all font-mono text-xs text-muted-foreground">
              {point.targetId}
            </dd>
          </div>
        </dl>
		{point.database && <p className="text-xs text-muted-foreground">Archive: {point.database.family} {point.database.source_server_version} · Backup tool {point.database.backup_tool_version} · {point.database.artifact_format}</p>}
		{point.sourceKind === "attach" && <div className="space-y-3 rounded-lg border border-border p-3 text-sm">
			{review?.database ? <>
				<p>Source {review.database.source_server_version} → Target {review.database.target_server_version}</p>
				<p className="break-words text-xs text-muted-foreground">Backup tool: {review.database.backup_tool_version}<br/>Restore tool: {review.database.restore_tool_version}</p>
				{review.database.version_difference && <label className="flex items-start gap-2"><Checkbox checked={acknowledged} disabled={busy || retained.current !== null} onChange={event => setAcknowledged(event.target.checked)} /><span>I accept unverified compatibility between these versions. I am responsible for checking that the restored data works.</span></label>}
			</> : <p>{reviewing ? "Reading target versions…" : "Target version review required."}</p>}
			<Button variant="outline" disabled={busy || reviewing || retained.current !== null || needsIdentity && !identity.trim()} onClick={() => void reviewTarget()}>Refresh version review</Button>
		</div>}
        <p className="rounded-lg border border-warning/30 bg-warning/10 p-3 text-sm text-warning">
          {point.sourceKind === "config"
            ? "Entries missing from this backup will be removed. Managed files are updated; running Services load the restored values on their next Deploy."
            : "This overwrites current data and temporarily stops Services using this target. Previously running Services restart after Restore finishes."}
        </p>
        {needsIdentity && (
          <div className="space-y-2">
            <Label htmlFor={identityId}>
              Encryption identity for era {point.keyEra}
            </Label>
            <Input
              id={identityId}
              type="password"
              autoComplete="off"
              spellCheck={false}
              value={identity}
              disabled={busy}
              onChange={(event) => {
				if (retained.current) return;
                setIdentity(event.target.value);
				setReview(null); setAcknowledged(false);
                key.current = newULID();
              }}
            />
            <p className="text-xs text-muted-foreground">
              Use the identity exported before key rotation. GP retains it only
              for this attempt.
            </p>
          </div>
        )}
        {error && (
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
        )}
        <DialogFooter>
          <Button variant="outline" onClick={close}>
            Cancel
          </Button>
          <Button
            variant="destructive"
            disabled={busy || reviewing || (needsIdentity && !identity.trim()) || point.sourceKind === "attach" && (!review?.database || review.database.version_difference && !acknowledged)}
            onClick={() => void restore()}
          >
            {busy ? "Starting Restore…" : "Overwrite and restore"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
