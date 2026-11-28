import { useEffect } from "react";
import { Link } from "react-router-dom";
import { useStore } from "@/lib/store";

export function FirstDeploymentRecoveryReview({
  environmentId,
  backupsHref,
  addsDataSources,
  runsMigration,
}: {
  environmentId: string;
  backupsHref?: string;
  addsDataSources: boolean;
  runsMigration: boolean;
}) {
  const { getBackupPolicyState, loadBackupPolicy, loadRecoveryPoints } =
    useStore();
  const state = getBackupPolicyState(environmentId);
  const points = state.recoveryPoints;
  useEffect(() => {
    void Promise.allSettled([
      loadBackupPolicy(environmentId),
      loadRecoveryPoints(environmentId),
    ]);
  }, [environmentId, loadBackupPolicy, loadRecoveryPoints]);

  return (
    <section className="space-y-2 rounded-md border border-border p-3 text-sm">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h4 className="font-medium">Backup and restore readiness</h4>
        {backupsHref && (
          <Link
            className="text-primary underline underline-offset-4"
            to={backupsHref}
            target="_blank"
            rel="noopener"
          >
            Open Backups &amp; restore
          </Link>
        )}
      </div>
      <div className="space-y-1 text-muted-foreground">
        <p>
          {state.loadError
            ? "The Environment’s backup policy could not be read."
            : !state.loaded
              ? "Checking the Environment’s backup policy…"
              : state.policy.connectorId
                ? `Backup destination configured. Scheduled backups ${state.policy.enabled ? "on" : "off"}.`
                : "No backup destination configured."}
        </p>
        <p>
          {points.loadError
            ? "Recovery Points could not be read."
            : !points.loaded
              ? "Checking for verified Recovery Points…"
              : points.items.length > 0
                ? "Verified Recovery Points are available for existing backup sources. Check the source before relying on a restore."
                : "No verified Recovery Points are available for this Environment."}
        </p>
        {addsDataSources && (
          <p>
            After deployment, review backup coverage for the new volumes and any
            database connections.
          </p>
        )}
        <p>
          {runsMigration &&
            "Before applying a migration to existing data, verify a Recovery Point for that data. "}
          Deployment rollback restores an earlier image. Restoring data is a
          separate operation.
        </p>
      </div>
    </section>
  );
}
