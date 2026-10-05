import { ResourcePanel } from "@/components/common/resource-panel";
import { TaskJournalItem } from "@/components/common/task-journal-item";
import { Button } from "@/components/ui/button";
import { useStore } from "@/lib/store";
import type { Environment, TaskJournalScope } from "@/lib/types";
import {
  Activity,
  ArrowUpRight,
  Boxes,
  ChevronRight,
  Database,
  FileCode2,
  HardDrive,
  KeyRound,
  Network,
  Settings,
  Shield,
  Terminal,
} from "lucide-react";
import { useEffect, useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { ConnectedServiceBoard } from "./connected-service-board";
import { useBackupRefresh } from "@/features/backup/use-backup-refresh";

export function EnvironmentOverview({
  env,
  now,
}: {
  env: Environment;
  now: number;
}) {
  const [, setSearch] = useSearchParams();
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
  const cards = [
    {
      view: "network",
      panel: "zones",
      title: "Zones",
      note: `${env.zones.length} networks`,
      icon: Network,
    },
    {
      view: "network",
      panel: "routes",
      title: "Routes",
      note: `${env.routes.length} HTTP Routes`,
      icon: ArrowUpRight,
    },
    {
      view: "network",
      panel: "attaches",
      title: "Backing connections",
      note: `${env.attaches.length} Attaches`,
      icon: Database,
    },
    {
      view: "network",
      panel: "router",
      title: "Router",
      note: "Caddy & Cloudflare Tunnel",
      icon: Network,
    },
    {
      view: "configuration",
      panel: "blueprint",
      title: "Blueprint",
      note: "Canonical desired state",
      icon: FileCode2,
    },
    {
      view: "configuration",
      panel: "entries",
      title: "Entries",
      note: `${env.entries.length} keys`,
      icon: KeyRound,
    },
    {
      view: "configuration",
      panel: "facts",
      title: "Attach facts",
      note: "Credentials & connection facts",
      icon: Database,
    },
    {
      view: "configuration",
      panel: "volumes",
      title: "Volumes",
      note: `${env.volumes.length} persistent stores`,
      icon: HardDrive,
    },
    {
      view: "configuration",
      panel: "scripts",
      title: "Scripts",
      note: `${env.scripts.length} scripts`,
      icon: Terminal,
    },
    {
      view: "operations",
      panel: "tasks",
      title: "Tasks",
      note: "Execution & recovery",
      icon: Activity,
    },
    {
      view: "operations",
      panel: "releases",
      title: "Releases",
      note: "Immutable deployment history",
      icon: Boxes,
    },
    {
      view: "operations",
      panel: "release-groups",
      title: "Release groups",
      note: `${env.releaseGroups.length} ordered groups`,
      icon: Boxes,
    },
    {
      view: "operations",
      panel: "backups",
      title: "Backups & connectors",
      note: "Recovery points & storage",
      icon: Shield,
    },
    {
      view: "settings",
      panel: "settings",
      title: "Settings",
      note: "Identity · pool · encryption",
      icon: Settings,
    },
  ];
  const connector = store.connectors.find(
    (c) => c.scopeRef === env.id && c.id === backup.connectorId,
  );
  return (
    <div className="space-y-7">
      <ConnectedServiceBoard env={env} now={now} />
      <section>
        <div className="mb-4 flex flex-wrap items-center justify-between gap-2">
          <h2 className="text-base font-medium">
            Everything in this Environment
          </h2>
          <span className="text-[11px] text-muted-foreground">
            Configuration, connectivity and operations
          </span>
        </div>
        <div className="grid gap-2.5 sm:grid-cols-2 xl:grid-cols-4">
          {cards.map((card) => (
            <Button
              key={card.panel}
              variant="outline"
              size="content"
              onClick={() => open(card.view, card.panel)}
              className="items-start justify-start gap-2.5 rounded-lg px-3.5 py-4 text-left hover:border-primary"
            >
              <card.icon className="mt-0.5 size-4 text-primary" />
              <span className="min-w-0 flex-1">
                <strong className="block text-[11px] font-medium">
                  {card.title}
                </strong>
                <span className="mt-1.5 block text-[10px] font-normal text-muted-foreground">
                  {card.note}
                </span>
              </span>
              <ChevronRight className="mt-1 size-3 text-muted-foreground" />
            </Button>
          ))}
        </div>
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
