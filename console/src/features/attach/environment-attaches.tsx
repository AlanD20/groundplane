"use client";

import { AttachFormDialog } from "@/features/environment/attach-form-dialog";
import { useState } from "react";
import { Link } from "react-router-dom";
import { useRequiredParams } from "@/lib/router";
import { Pencil, Plug, Plus, Trash2 } from "lucide-react";
import { useStore } from "@/lib/store";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import type { Attach, Environment } from "@/lib/types";

// ---- Attaches ----

export function AttachesCard({ env }: { env: Environment }) {
  const [open, setOpen] = useState(false);
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <Plug className="size-4 text-muted-foreground" /> Attached backing
          services
        </CardTitle>
        <Button variant="outline" size="sm" onClick={() => setOpen(true)}>
          <Plus className="size-3.5" /> Attach backing
        </Button>
      </CardHeader>
      <CardContent className="flex flex-col gap-1.5">
        {env.attaches.map((a) => (
          <div
            key={a.id}
            className="flex items-center justify-between gap-2 rounded-lg border border-border bg-surface px-3 py-2"
          >
            <Link
              to={`/platform/backing-services/${a.projectId}`}
              className="flex min-w-0 flex-1 items-center justify-between gap-2 transition-colors hover:border-ring/50"
            >
              <span className="font-mono text-sm">{a.projectId}</span>
              <span className="truncate font-mono text-xs text-muted-foreground">
                {a.database !== "—"
                  ? `database ${a.database} · role ${a.role}`
                  : "attached"}
                {a.service ? ` · for ${a.service}` : ""}
              </span>
            </Link>
            <DetachAttach env={env} attach={a} />
          </div>
        ))}
        {env.attaches.length === 0 && (
          <div className="text-xs text-muted-foreground">
            no backing service attached — connect a Service to a shared backing
            service; provisioning depends on its adapter
          </div>
        )}
      </CardContent>
      <AttachFormDialog env={env} open={open} onOpenChange={setOpen} />
    </Card>
  );
}

// ---- Deploys ----

// Detach is a task, like attach: the adapter deprovisions (revoke grants →
// drop role → optionally drop database) and the desired-state record is
// removed as part of that task — never a plain record delete.
export function RenameAttach({
  env,
  attach,
}: {
  env: Environment;
  attach: Attach;
}) {
  const store = useStore();
  const [open, setOpen] = useState(false);
  const [name, setName] = useState(attach.name);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string>();
  return (
    <>
      <Button
        variant="ghost"
        size="icon-xs"
        title={`Rename ${attach.name}`}
        onClick={() => {
          setName(attach.name);
          setError(undefined);
          setOpen(true);
        }}
      >
        <Pencil className="size-3.5" />
      </Button>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Rename Attach</DialogTitle>
            <DialogDescription>
              Changes the Environment-scoped spec key. Stable identity, facts,
              grants, and network membership do not change.
            </DialogDescription>
          </DialogHeader>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor={`attach-rename-${attach.id}`}>Attach name</Label>
            <Input
              id={`attach-rename-${attach.id}`}
              value={name}
              onChange={(event) => setName(event.target.value)}
              className="font-mono"
              autoFocus
            />
            {error ? <p className="text-xs text-destructive">{error}</p> : null}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button
              disabled={saving || !name.trim() || name.trim() === attach.name}
              onClick={() => {
                setSaving(true);
                setError(undefined);
                void store
                  .renameAttach(env.id, attach.id, name.trim())
                  .then(() => setOpen(false))
                  .catch((cause: unknown) => {
                    setError(
                      cause instanceof Error
                        ? cause.message
                        : "Unable to rename Attach",
                    );
                  })
                  .finally(() => setSaving(false));
              }}
            >
              {saving ? "Saving…" : "Rename"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}

export function DetachAttach({
  env,
  attach,
}: {
  env: Environment;
  attach: Attach;
}) {
  const store = useStore();
  const params = useRequiredParams("tenant");
  const [open, setOpen] = useState(false);
  const g = store.getBackingProject(attach.projectId);
  const custom = g
    ? store.adapters.find(
        (a) => a.key === g.environments?.[0]?.services[0]?.adapter,
      )?.custom
    : false;
  const detachHook =
    custom &&
    attach.credential.mode === "new" &&
    !!g?.environments?.[0]?.services[0]?.hooks?.detach;
  return (
    <>
      <Button
        variant="ghost"
        size="content"
        type="button"
        onClick={() => setOpen(true)}
        className="rounded-md p-1 text-muted-foreground outline-none transition-colors hover:bg-muted hover:text-destructive focus-visible:ring-1 focus-visible:ring-ring"
        title={`Detach ${g?.name ?? attach.projectId}`}
        aria-label="Detach"
      >
        <Trash2 className="size-3.5" />
      </Button>
      <TaskRunnerDialog
        open={open}
        onOpenChange={setOpen}
        title={`Detach ${g?.name ?? attach.projectId}`}
        description={
          custom
            ? detachHook
              ? "Runs the custom detach hook with the saved consumer facts, then removes the network attachment. A hook failure blocks detachment."
              : "Removes this consumer’s network attachment. No deprovisioning hook runs."
            : "Runs the adapter's deprovision: revoke grants → drop role → optionally drop database. The desired-state record is removed as part of the task, never by a plain delete."
        }
        type="detach"
        target={env.id}
        workspace={params.tenant}
        destructive
        confirmText={g?.name ?? "detach"}
        startLabel="Detach"
        steps={
          custom
            ? [
                ...(detachHook
                  ? [
                      {
                        label: "run custom detach hook",
                        state: "pending" as const,
                      },
                    ]
                  : []),
                { label: "remove network join", state: "pending" },
              ]
            : [
                {
                  label: `revoke grants on ${attach.database}`,
                  state: "pending",
                },
                { label: `drop role ${attach.role}`, state: "pending" },
                {
                  label:
                    attach.database !== "—"
                      ? `drop database ${attach.database}`
                      : "no database to drop",
                  state: "pending",
                },
                { label: "remove desired-state record", state: "pending" },
              ]
        }
        onDispatch={() => store.removeAttach(env.id, attach.id)}
      />
    </>
  );
}
