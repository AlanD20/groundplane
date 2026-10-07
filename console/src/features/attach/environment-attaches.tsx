import {
  ResourceForm,
  ResourceFormHeader as DialogHeader,
  ResourceFormTitle as DialogTitle,
  ResourceFormDescription as DialogDescription,
  ResourceFormFooter as DialogFooter,
} from "@/components/common/resource-form";
("use client");

import { workspaceSectionClassName } from "@/components/common/workspace-section";

import {
  ListToolbar,
  TablePagination,
  useTableView,
} from "@/components/common/table-controls";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ConnectionValues } from "./connection-values";
import { StatusBadge } from "@/components/common/status-badge";
import { AttachFormDialog } from "@/features/environment/attach-form-dialog";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import type { Attach, Environment, Service } from "@/lib/types";
import { Pencil, Plug, Plus, Trash2 } from "lucide-react";
import { useState } from "react";
import { ContextLink } from "@/components/common/context-link";

// ---- Attaches ----

export function AttachesCard({
  env,
  service,
}: {
  env: Environment;
  service?: Service;
}) {
  const store = useStore();
  const attaches = env.attaches.filter(
    (attach) =>
      !service ||
      attach.serviceId === service.id ||
      attach.service === service.name,
  );
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const table = useTableView(
    attaches.filter((attach) =>
      `${attach.name} ${attach.service} ${attach.database} ${store.getBackingProject(attach.projectId)?.name}`
        .toLowerCase()
        .includes(query.trim().toLowerCase()),
    ),
    {
      name: (attach) => attach.name,
      service: (attach) => attach.service,
      backing: (attach) =>
        store.getBackingProject(attach.projectId)?.name ?? "",
    },
    "name",
    "asc",
    `${env.id}/${query}`,
  );
  return (
    <Card>
      <CardHeader className="flex-col items-start gap-3 sm:flex-row sm:items-center sm:justify-between">
        <CardTitle className="flex items-center gap-2">
          <Plug className="size-4 text-muted-foreground" /> Backing connections
        </CardTitle>
        <Button variant="outline" size="sm" onClick={() => setOpen(true)}>
          <Plus className="size-3.5" /> Connect backing service
        </Button>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <p className="text-xs text-muted-foreground">
          {service
            ? `Connections used by ${service.name}.`
            : `Connections owned by Services in ${env.name}.`}{" "}
          Connection values must be explicitly mapped to variables or files.
        </p>
        <ListToolbar
          label="Connections"
          query={query}
          onQueryChange={setQuery}
          sort={table}
          fields={[
            { value: "name", label: "Name" },
            { value: "service", label: "Service" },
            { value: "backing", label: "Backing Service" },
          ]}
        />
        {table.rows.map((a) => (
          <div
            key={a.id}
            className={workspaceSectionClassName(false, "space-y-3")}
          >
            <div className="flex flex-wrap items-center gap-3">
              <ContextLink
                returnLabel={`${service?.name ?? env.name} · Backing connections`}
                to={`/platform/backing-services/${a.projectId}`}
                className="flex min-w-0 basis-full flex-col items-start gap-1 transition-colors hover:border-ring/50 sm:flex-1"
              >
                <span className="text-sm font-medium [overflow-wrap:anywhere]">
                  {a.name} ·{" "}
                  {store.getBackingProject(a.projectId)?.name ?? a.projectId}
                </span>
                <span className="text-xs text-muted-foreground [overflow-wrap:anywhere]">
                  {a.database !== "—"
                    ? `database ${a.database} · role ${a.role}`
                    : "attached"}
                  {a.service ? ` · for ${a.service}` : ""}
                </span>
              </ContextLink>
              <RenameAttach env={env} attach={a} inline={!service} />
              <StatusBadge status={a.status} />
              <DetachAttach env={env} attach={a} />
            </div>
            <ConnectionValues attach={a} env={env} />
          </div>
        ))}
        {attaches.length === 0 && (
          <div className="text-xs text-muted-foreground">
            No connections yet. Connect a database or another backing service to
            get started.
          </div>
        )}
        {attaches.length > 0 && !table.total && (
          <p className="p-6 text-center text-xs text-muted-foreground">
            No connections match your search.
          </p>
        )}
        <TablePagination table={table} label="Connections" />
      </CardContent>
      <AttachFormDialog
        inline={!service}
        initialServiceId={service?.id}
        env={env}
        open={open}
        onOpenChange={setOpen}
      />
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
  inline = false,
}: {
  env: Environment;
  attach: Attach;
  inline?: boolean;
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
      <ResourceForm
        className="order-last"
        inline={inline}
        open={open}
        onOpenChange={setOpen}
      >
        <DialogHeader>
          <DialogTitle>Rename connection</DialogTitle>
          <DialogDescription>
            Choose the connection name used in your Blueprint. Existing
            credentials and network access stay the same.
          </DialogDescription>
        </DialogHeader>
        <div className="flex flex-col gap-1.5">
          <Label htmlFor={`attach-rename-${attach.id}`}>Connection name</Label>
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
      </ResourceForm>
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
