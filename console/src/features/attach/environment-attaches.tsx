import { CreationButton } from "@/components/common/creation-button";
import {
  ResourceForm,
  ResourceFormHeader as DialogHeader,
  ResourceFormTitle as DialogTitle,
  ResourceFormDescription as DialogDescription,
  ResourceFormFooter as DialogFooter,
} from "@/components/common/resource-form";
("use client");

import { ConnectionInspection } from "./connection-inspection";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ConnectionValues } from "./connection-values";
import { BackingAccessCheck } from "./backing-check-panel";
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
  const attaches = env.attaches.filter(
    (attach) =>
      !service ||
      attach.serviceId === service.id ||
      attach.service === service.name,
  );
  const [open, setOpen] = useState(false);
  return (
    <Card>
      <CardHeader className="flex-col items-start gap-3 sm:flex-row sm:items-center sm:justify-between">
        <CardTitle className="flex items-center gap-2">
          <Plug className="size-4 text-muted-foreground" /> Backing connections
        </CardTitle>
        <CreationButton
          active={open}
          variant="outline"
          size="sm"
          onOpenChange={setOpen}
        >
          <Plus className="size-3.5" /> Connect backing service
        </CreationButton>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <AttachFormDialog
          inline
          initialServiceId={service?.id}
          env={env}
          open={open}
          onOpenChange={setOpen}
        />
        <p className="text-xs text-muted-foreground">
          {service
            ? `Connections used by ${service.name}.`
            : `Connections owned by Services in ${env.name}.`}{" "}
          Connection values must be explicitly mapped to variables or files.
        </p>
        <ConnectionInspection scope={service?.id ?? env.id} attaches={attaches}>
          {(attach, backingName) => (
            <>
              <header className="flex flex-wrap items-start gap-2">
                <div className="min-w-0 flex-1 space-y-1">
                  <h3 className="text-sm font-semibold [overflow-wrap:anywhere]">
                    {attach.name}
                  </h3>
                  <ContextLink
                    returnLabel={`${service?.name ?? env.name} · Backing connections`}
                    to={`/platform/backing-services/${attach.projectId}`}
                    className="text-xs text-primary hover:underline [overflow-wrap:anywhere]"
                  >
                    {backingName}
                  </ContextLink>
                  <p className="text-xs text-muted-foreground [overflow-wrap:anywhere]">
                    {attach.database !== "—" &&
                      `Database ${attach.database} · Role ${attach.role}`}
                    {attach.database !== "—" && attach.service && " · "}
                    {attach.service}
                  </p>
                </div>
                <StatusBadge status={attach.status} />
                <RenameAttach env={env} attach={attach} inline />
                <DetachAttach env={env} attach={attach} />
              </header>
              {attach.factLoadError && (
                <p
                  role="alert"
                  className="text-sm text-destructive [overflow-wrap:anywhere]"
                >
                  Connection values unavailable · {attach.factLoadError}
                </p>
              )}
              <ConnectionValues attach={attach} env={env} />
              <BackingAccessCheck
                environmentId={env.id}
                attachId={attach.id}
                disabled={attach.status !== "healthy"}
                grants={(attach.grantAttachIds ?? []).map((id) => ({
                  id,
                  name:
                    env.attaches.find((candidate) => candidate.id === id)
                      ?.name ?? id,
                }))}
              />
            </>
          )}
        </ConnectionInspection>
      </CardContent>
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
        aria-label={`Rename ${attach.name}`}
        aria-expanded={open}
        disabled={saving}
        onClick={() => {
          if (open) {
            setOpen(false);
            return;
          }
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
          <DialogTitle>Rename connection · {attach.name}</DialogTitle>
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
            : "Runs the adapter's detach operation, then removes this connection. Credential and database cleanup follows the connection's ownership and removal settings."
        }
        type="detach"
        target={env.id}
        workspace={params.tenant}
        destructive
        confirmText={g?.name ?? "detach"}
        startLabel="Detach"
        runAfter
        onDispatch={(after) => store.removeAttach(env.id, attach.id, after)}
      />
    </>
  );
}
