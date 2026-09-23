"use client";

import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "@/components/ui/table";
import { useEffect, useState } from "react";
import { RefreshCw, Terminal } from "lucide-react";
import { useStore } from "@/lib/store";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { EnvironmentConnectorManager } from "@/features/connectors/environment-connector-manager";
import { cn } from "@/lib/utils";
import type { Environment } from "@/lib/types";
import {
  backupPolicyConfigured,
  deriveStrategy,
  backupSourceLabel,
  backupSourceStrategy,
  adapterLabel,
  adapterSteps,
} from "@/features/backup/environment-backup-projection";
import { BackupPolicyDialog } from "@/features/backup/backup-policy-dialog";

// ---- Backups ----

export function BackupsCard({ env }: { env: Environment }) {
  const store = useStore();
  const policyState = store.getBackupPolicyState(env.id);
  const backup = policyState.policy;
  const points = policyState.recoveryPoints;
  const [policyOpen, setPolicyOpen] = useState(false);
  const [runTaskId, setRunTaskId] = useState<string | null>(null);
  const [runError, setRunError] = useState<string | null>(null);
  useEffect(() => {
    void store.loadBackupPolicy(env.id).catch(() => undefined);
  }, [env.id, store.loadBackupPolicy]);
  useEffect(() => {
    void store.loadRecoveryPoints(env.id).catch(() => undefined);
  }, [env.id, store.loadRecoveryPoints]);
  const activeConnector = store.connectors.find(
    (connector) =>
      connector.scopeRef === env.id && connector.id === backup.connectorId,
  );
  const configured = backupPolicyConfigured(backup);
  return (
    <>
      <Card>
        <CardHeader
          aria-busy={policyState.loading || policyState.saving}
          className="flex-col items-stretch gap-3 sm:flex-row sm:items-center sm:justify-between"
        >
          <CardTitle className="flex items-start gap-2 leading-snug">
            <RefreshCw className="mt-0.5 size-4 shrink-0 text-muted-foreground" />{" "}
            Backup policy · one policy, selected sources
          </CardTitle>
          <div className="flex flex-col gap-2 sm:flex-row sm:flex-wrap sm:items-center sm:justify-end">
            <span className="font-mono text-xs text-muted-foreground">
              Next: {backup.nextRunAt ?? "not scheduled"}
            </span>
            <Button
              variant="ghost"
              size="sm"
              disabled={policyState.loading || policyState.saving}
              onClick={() =>
                void store.loadBackupPolicy(env.id).catch(() => undefined)
              }
            >
              <RefreshCw
                className={cn(
                  "size-3.5",
                  policyState.loading && "animate-spin",
                )}
              />{" "}
              Refresh
            </Button>
            <Button
              variant="outline"
              size="sm"
              disabled={
                !configured ||
                !backup.enabled ||
                policyState.loading ||
                policyState.saving ||
                Boolean(policyState.loadError)
              }
              onClick={() => {
                setRunError(null);
                void store
                  .runBackup(env.id)
                  .then((taskId) => setRunTaskId(taskId))
                  .catch((error) =>
                    setRunError(
                      error instanceof Error
                        ? error.message
                        : "Unable to run backup",
                    ),
                  );
              }}
            >
              <Terminal className="size-3.5" /> Backup now
            </Button>
            <Button
              variant="outline"
              size="sm"
              disabled={
                !policyState.loaded ||
                policyState.loading ||
                policyState.saving ||
                Boolean(policyState.loadError)
              }
              onClick={() => setPolicyOpen(true)}
            >
              Edit policy + sources
            </Button>
          </div>
          {(runTaskId || runError) && (
            <div className="flex flex-wrap items-center gap-2 text-xs">
              {runTaskId && (
                <span className="text-muted-foreground">
                  Task published: <code>{runTaskId}</code>
                </span>
              )}
              {runError && <span className="text-destructive">{runError}</span>}
            </div>
          )}
        </CardHeader>
        {policyState.loadError && (
          <p className="mx-4 rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs text-destructive">
            {policyState.loadError}. The last loaded policy remains visible.
          </p>
        )}
        {!backup.enabled && (
          <p className="mx-4 my-3 rounded-lg border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning">
            Backups are <span className="font-medium">off</span> for this
            environment — nothing is scheduled or backed up.
            {configured
              ? " The retained policy can be enabled again."
              : " This policy is not configured yet."}
          </p>
        )}
        <CardContent className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          <PolicyCell
            label="Status"
            value={backup.enabled ? "enabled" : "off"}
          />
          <PolicyCell label="Strategy" value={deriveStrategy(store, env)} />
          <PolicyCell
            label="Frequency · UTC"
            value={backup.frequency ?? "not configured"}
          />
          <PolicyCell
            label="Retention"
            value={
              backup.keep !== undefined
                ? `keep ${backup.keep} backups`
                : "not configured"
            }
          />
          <PolicyCell
            label="Encryption"
            value={`${backup.encryption ?? "not configured"}${backup.ageRecipient ? ` · age1…${backup.ageRecipient.slice(-8)}` : backup.encryption === "age" ? " · key generated on first enable" : ""}`}
          />
          <PolicyCell
            label="Target"
            value={
              activeConnector
                ? `s3://${activeConnector.bucket}/${activeConnector.prefix}`
                : backup.connectorId
                  ? `missing : ${backup.connectorId}`
                  : "not selected"
            }
          />
          <PolicyCell
            label="Key era"
            value={backup.keyEra ? `era ${backup.keyEra}` : "—"}
          />
        </CardContent>
        <CardContent className="flex flex-col gap-1.5 border-t border-border pt-3">
          <span className="text-xs font-medium text-muted-foreground">
            Sources · one run backs up every selected source
          </span>
          <div className="flex flex-wrap gap-1.5">
            {backup.sources.map((src) => (
              <span
                key={src.id}
                className="inline-flex items-center gap-1.5 rounded-full bg-primary/10 px-2.5 py-1 font-mono text-xs text-primary"
              >
                <RefreshCw className="size-3" />
                {backupSourceLabel(store, env, src)}
              </span>
            ))}
            {backup.sources.length === 0 && (
              <span className="text-xs text-muted-foreground">
                no sources selected
              </span>
            )}
          </div>
        </CardContent>
      </Card>

      <EnvironmentConnectorManager env={env} />

      {backup.sources.map((src) => {
        const strategy = backupSourceStrategy(store, env, src);
        const sourceLabel = backupSourceLabel(store, env, src);
        return (
          <Card key={src.id}>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <RefreshCw className="size-4 text-muted-foreground" /> Source ·{" "}
                {adapterLabel(strategy)}
                <Badge variant="default">{sourceLabel}</Badge>
              </CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-2">
              <p className="text-xs text-muted-foreground">
                {src.kind === "volume"
                  ? `volume ${sourceLabel}`
                  : src.kind === "config"
                    ? `environment config — env vars, files & secrets (values included, age-encrypted). Does NOT include backing environments or platform state.`
                    : `database ${sourceLabel}`}
                {" · "}backed up and restored by the same{" "}
                <span className="font-mono">{strategy}</span> adapter.
              </p>
              <div className="flex flex-col gap-1 border-b border-border pb-2">
                {adapterSteps(strategy).map((s, i) => (
                  <div key={i} className="flex items-baseline gap-2.5 text-xs">
                    <span className="size-1.5 shrink-0 translate-y-[-2px] rounded-full bg-success" />
                    <span className="w-32 shrink-0 font-mono text-primary">
                      {s.op}
                    </span>
                    <span className="break-all font-mono text-muted-foreground">
                      {s.detail}
                    </span>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        );
      })}

      <Card>
        <CardHeader
          aria-busy={points.loading || points.loadingMore}
          className="flex-col items-stretch gap-3 sm:flex-row sm:items-center sm:justify-between"
        >
          <CardTitle className="flex items-center gap-2">
            <RefreshCw className="size-4 text-muted-foreground" /> Recovery
            Points
          </CardTitle>
          <div className="flex items-center gap-2">
            <Button
              variant="ghost"
              size="sm"
              disabled={points.loading || points.loadingMore}
              onClick={() =>
                void store.loadRecoveryPoints(env.id).catch(() => undefined)
              }
            >
              <RefreshCw
                className={cn("size-3.5", points.loading && "animate-spin")}
              />{" "}
              Refresh
            </Button>
          </div>
        </CardHeader>
        {points.loadError && (
          <div className="mx-4 flex items-center justify-between gap-3 rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs text-destructive">
            <span>{points.loadError}</span>
            <Button
              variant="outline"
              size="sm"
              disabled={points.loading || points.loadingMore}
              onClick={() =>
                void store
                  .loadRecoveryPoints(env.id, points.failedCursor ?? undefined)
                  .catch(() => undefined)
              }
            >
              Retry
            </Button>
          </div>
        )}
        <CardContent aria-busy={points.loading || points.loadingMore}>
          {points.loading && !points.loaded && (
            <p className="text-xs text-muted-foreground">
              Loading verified Recovery Points...
            </p>
          )}
          {!points.loading &&
            !points.loadError &&
            points.loaded &&
            points.items.length === 0 && (
              <p className="rounded-lg border border-border bg-surface px-3 py-2 text-xs text-muted-foreground">
                No verified Recovery Points for this Environment.
              </p>
            )}
          {points.items.length > 0 && (
            <div className="overflow-x-auto rounded-lg border border-border">
              <Table className="w-full min-w-[760px] text-left text-xs">
                <TableHeader className="bg-surface text-muted-foreground">
                  <TableRow>
                    <TableHead className="px-3 py-2 font-medium">
                      Point
                    </TableHead>
                    <TableHead className="px-3 py-2 font-medium">
                      Source
                    </TableHead>
                    <TableHead className="px-3 py-2 font-medium">
                      Target
                    </TableHead>
                    <TableHead className="px-3 py-2 font-medium">
                      Created
                    </TableHead>
                    <TableHead className="px-3 py-2 font-medium">
                      Size
                    </TableHead>
                    <TableHead className="px-3 py-2 font-medium">
                      Encryption
                    </TableHead>
                    <TableHead className="px-3 py-2 font-medium">
                      Status
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody className="divide-y divide-border">
                  {points.items.map((point) => (
                    <TableRow key={point.id}>
                      <TableCell className="px-3 py-2 font-mono text-primary">
                        {point.id}
                      </TableCell>
                      <TableCell className="px-3 py-2 font-mono">
                        {point.sourceKind} · {point.sourceId}
                      </TableCell>
                      <TableCell className="px-3 py-2 font-mono">
                        {point.targetId}
                      </TableCell>
                      <TableCell className="px-3 py-2 whitespace-nowrap">
                        {point.createdAt}
                      </TableCell>
                      <TableCell className="px-3 py-2 font-mono">
                        {point.sizeBytes.toLocaleString()} B
                      </TableCell>
                      <TableCell className="px-3 py-2">
                        {point.encrypted ? `age · era ${point.keyEra}` : "none"}
                      </TableCell>
                      <TableCell className="px-3 py-2 text-success">
                        {point.status}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
          {points.nextCursor && (
            <div className="mt-3 flex justify-center">
              <Button
                variant="outline"
                size="sm"
                disabled={points.loadingMore}
                onClick={() =>
                  void store
                    .loadRecoveryPoints(env.id, points.nextCursor ?? undefined)
                    .catch(() => undefined)
                }
              >
                {points.loadingMore ? "Loading..." : "Load more"}
              </Button>
            </div>
          )}
        </CardContent>
      </Card>

      <BackupPolicyDialog
        env={env}
        open={policyOpen}
        onOpenChange={setPolicyOpen}
      />
    </>
  );
}

export function PolicyCell({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface p-3">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className="break-all font-mono text-sm">{value}</span>
    </div>
  );
}
