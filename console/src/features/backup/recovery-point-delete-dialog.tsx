import { useRef } from "react";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { useStore } from "@/lib/store";
import { newULID } from "@/lib/utils";
import type { Environment } from "@/lib/types";
import type { RecoveryPoint } from "./types";
import { backupSourceLabel } from "./environment-backup-projection";

export function RecoveryPointDeleteDialog({
  env,
  point,
  onClose,
}: {
  env: Environment;
  point: RecoveryPoint;
  onClose: () => void;
}) {
  const store = useStore();
  const idempotencyKey = useRef(newULID());
  const target = backupSourceLabel(store, env, {
    id: point.sourceId,
    kind: point.sourceKind,
    targetId: point.targetId,
  });

  return (
    <TaskRunnerDialog
      open
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
      title="Delete Recovery Point?"
      description="This permanently deletes the selected backup archive and its Recovery Point record. It does not delete or modify the source data."
      type="remove"
      target={point.id}
      workspace={env.name}
      destructive
      abortable={false}
      confirmText={point.id}
      startLabel="Permanently delete archive"
      executionCopy="The Controller will delete and verify absence of only this selected archive before releasing its Recovery Point record:"
      steps={[
        {
          label: "Delete and verify the selected archive",
          state: "pending",
        },
      ]}
      review={
        <dl className="grid gap-3 rounded-lg border border-border bg-surface p-4 text-sm">
          <div>
            <dt className="text-muted-foreground">Recovery Point</dt>
            <dd className="break-all font-mono text-xs">{point.id}</dd>
          </div>
          <div>
            <dt className="text-muted-foreground">Captured</dt>
            <dd>{new Date(point.createdAt).toLocaleString()}</dd>
          </div>
          <div>
            <dt className="text-muted-foreground">Source data retained</dt>
            <dd>{target}</dd>
            <dd className="break-all font-mono text-xs text-muted-foreground">
              {point.targetId}
            </dd>
          </div>
          <div>
            <dt className="text-muted-foreground">Archive destination</dt>
            <dd className="break-all text-xs">
              s3://{point.connectorBucket}/{point.connectorPrefix}
            </dd>
          </div>
        </dl>
      }
      onDispatch={() =>
        store.deleteRecoveryPoint(env.id, point.id, idempotencyKey.current)
      }
      onSettled={() => store.loadRecoveryPoints(env.id)}
    />
  );
}
