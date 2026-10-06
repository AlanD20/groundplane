import { ResourcePanel } from "@/components/common/resource-panel";
import { TaskJournalItem } from "@/components/common/task-journal-item";
import { Button } from "@/components/ui/button";
import { useStore } from "@/lib/store";
import type { Environment, TaskJournalScope } from "@/lib/types";
import { Activity, ArrowUpRight, Shield } from "lucide-react";
import { useEffect, useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { ConnectedServiceBoard } from "./connected-service-board";
import { ServicesList } from "./services-list";
import { useBackupRefresh } from "@/features/backup/use-backup-refresh";

export function EnvironmentOverview({
  env,
  now,
}: {
  env: Environment;
  now: number;
}) {
  const [search, setSearch] = useSearchParams();
  const listView = search.get("layout") === "list";
  const store = useStore();
  const scope = useMemo<TaskJournalScope>(
    () => ({ kind: "environment", environmentId: env.id }),
    [env.id],
  );
  const journal = store.getTaskJournal(scope);
  const backupState = store.getBackupPolicyState(env.id);
  const backup = backupState.policy;
  useBackupRefresh(env.id, false);
  useEffect(() => {
    void store.loadTaskJournal("tasks", scope).catch(() => undefined);
  }, [scope, store.loadTaskJournal]);
  const open = (view: string, panel: string) => setSearch({ view, panel });
  const connector = store.connectors.find(
    (c) => c.scopeRef === env.id && c.id === backup.connectorId,
  );
  return (
    <div className="space-y-7">
      <section className="space-y-4">
        <div className="flex items-center justify-between gap-3">
          <h2 className="text-base font-medium">Services</h2>
          <div className="flex gap-1" role="group" aria-label="Service display">
            {(["cards", "list"] as const).map((layout) => (
              <Button
                key={layout}
                size="sm"
                variant={
                  listView === (layout === "list") ? "secondary" : "ghost"
                }
                aria-pressed={listView === (layout === "list")}
                onClick={() =>
                  setSearch((current) => {
                    const next = new URLSearchParams(current);
                    next.set("layout", layout);
                    return next;
                  })
                }
              >
                {layout === "cards" ? "Cards" : "Table"}
              </Button>
            ))}
          </div>
        </div>
        {listView ? (
          <ServicesList env={env} now={now} />
        ) : (
          <ConnectedServiceBoard env={env} now={now} />
        )}
      </section>
      <div className="grid items-start gap-4 xl:grid-cols-2">
        <ResourcePanel
          title={
            <span className="flex items-center gap-2">
              <Activity className="size-4 text-primary" />
              Latest activity
            </span>
          }
          actions={
            <Button
              variant="ghost"
              size="sm"
              className="text-[10px] text-primary"
              onClick={() => open("operations", "tasks")}
            >
              All Tasks
              <ArrowUpRight className="size-3" />
            </Button>
          }
        >
          {journal.loadError ? (
            <p role="alert" className="text-xs text-destructive">
              {journal.loadError}
            </p>
          ) : !journal.loaded ? (
            <p role="status" className="text-xs text-muted-foreground">
              Loading activity…
            </p>
          ) : journal.entries.length ? (
            journal.entries
              .slice(0, 3)
              .map((entry) => (
                <TaskJournalItem
                  key={entry.id}
                  entry={entry}
                  scope={scope}
                  surface="tasks"
                />
              ))
          ) : (
            <p className="text-xs text-muted-foreground">No Tasks yet.</p>
          )}
        </ResourcePanel>
        <ResourcePanel
          title={
            <span className="flex items-center gap-2">
              <Shield className="size-4 text-primary" />
              Backup protection
            </span>
          }
          actions={
            <Button
              variant="ghost"
              size="sm"
              className="text-[10px] text-primary"
              onClick={() => open("operations", "backups")}
            >
              Manage
              <ArrowUpRight className="size-3" />
            </Button>
          }
        >
          {backupState.loadError ? (
            <p role="alert" className="text-xs text-destructive">
              {backupState.loadError}
            </p>
          ) : !backupState.loaded ? (
            <p role="status" className="text-xs text-muted-foreground">
              Loading backup protection…
            </p>
          ) : (
            <dl className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-6 gap-y-3 text-[11px]">
              <dt className="text-muted-foreground">Schedule</dt>
              <dd>{backup.enabled ? backup.frequency : "Disabled"}</dd>
              <dt className="text-muted-foreground">Sources</dt>
              <dd>{backup.sources.length} configured</dd>
              <dt className="text-muted-foreground">Retention</dt>
              <dd>
                {backup.keep
                  ? `${backup.keep} points per source`
                  : "Not configured"}
              </dd>
              <dt className="text-muted-foreground">Storage</dt>
              <dd className="break-words">
                {connector?.name ?? "Not configured"}
              </dd>
            </dl>
          )}
        </ResourcePanel>
      </div>
    </div>
  );
}
