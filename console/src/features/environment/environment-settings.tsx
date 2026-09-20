"use client";

import { useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useRequiredParams } from "@/lib/router";
import {
  FileCode2,
  Layers,
  Pencil,
  RotateCw,
  ShieldCheck,
  Trash2,
} from "lucide-react";
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
import { Drawer, DrawerContent } from "@/components/ui/drawer";
import { CopyButton } from "@/components/common/copy-button";
import { cn } from "@/lib/utils";
import type { Environment } from "@/lib/types";

// ---- Shared-infra facts ----

// ---- Environment settings: identity, encryption key, rename, delete ----

export function SettingsCard({ env }: { env: Environment }) {
  const store = useStore();
  const params = useRequiredParams("tenant", "project");
  const navigate = useNavigate();
  const [renameOpen, setRenameOpen] = useState(false);
  const [networkPoolOpen, setNetworkPoolOpen] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [newName, setNewName] = useState(env.name);
  const [networkPool, setNetworkPool] = useState(env.networkPool);
  const [networkPoolSaving, setNetworkPoolSaving] = useState(false);
  const [networkPoolError, setNetworkPoolError] = useState<string | null>(null);
  const [renameSaving, setRenameSaving] = useState(false);
  const [renameError, setRenameError] = useState<string | null>(null);
  const [deleteSaving, setDeleteSaving] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [confirmTyped, setConfirmTyped] = useState("");
  const [keyAction, setKeyAction] = useState<"rotate" | "export" | null>(null);
  const [keyError, setKeyError] = useState<string | null>(null);
  const keyExportController = useRef<AbortController | null>(null);
  const deletionFailure = store.getEnvironmentDeletionFailure(env.id);

  useEffect(() => {
    setNewName(env.name);
    setNetworkPool(env.networkPool);
  }, [env.name, env.networkPool]);

  useEffect(
    () => () => {
      keyExportController.current?.abort();
      keyExportController.current = null;
    },
    [],
  );

  return (
    <>
      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <Layers className="size-4 text-muted-foreground" /> Environment
          </CardTitle>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                setNetworkPool(env.networkPool);
                setNetworkPoolError(null);
                setNetworkPoolOpen(true);
              }}
            >
              <Pencil className="size-3.5" /> Edit pool
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setRenameOpen(true)}
            >
              <FileCode2 className="size-3.5" /> Rename
            </Button>
          </div>
        </CardHeader>
        <CardContent className="flex flex-col gap-1.5">
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
            <span className="font-mono text-[11px] text-muted-foreground">
              id (static — every reference keys off this)
            </span>
            <span className="flex items-center gap-2">
              <span className="truncate font-mono text-xs text-foreground">
                {env.id}
              </span>
              <CopyButton value={env.id} />
            </span>
          </div>
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
            <span className="font-mono text-[11px] text-muted-foreground">
              name (label only)
            </span>
            <span className="truncate font-mono text-xs text-foreground">
              {env.name}
            </span>
          </div>
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
            <span className="font-mono text-[11px] text-muted-foreground">
              network pool (globally reserved IPv4 CIDR)
            </span>
            <span className="truncate font-mono text-xs text-foreground">
              {env.networkPool}
            </span>
          </div>
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
            <span className="font-mono text-[11px] text-muted-foreground">
              allocation (Zone CIDR addresses)
            </span>
            <span className="truncate font-mono text-xs text-foreground">
              {env.networkCapacity.allocatedAddresses.toLocaleString()} /{" "}
              {env.networkCapacity.totalAddresses.toLocaleString()}
              {" · "}
              {env.networkCapacity.availableAddresses.toLocaleString()}{" "}
              available
              {" · "}
              {env.networkCapacity.zoneCount} Zones
            </span>
          </div>
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
            <span className="font-mono text-[11px] text-muted-foreground">
              volume folder (id-derived, never renamed)
            </span>
            <span className="truncate font-mono text-xs text-muted-foreground">
              {env.volumeDir}
            </span>
          </div>
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
            <span className="font-mono text-[11px] text-muted-foreground">
              deterministic env file
            </span>
            <span className="truncate font-mono text-xs text-muted-foreground">
              secrets/.env.{env.id}
            </span>
          </div>
        </CardContent>
      </Card>

      <Drawer
        open={networkPoolOpen}
        onOpenChange={(open) => {
          if (networkPoolSaving) return;
          setNetworkPoolOpen(open);
          if (!open) setNetworkPoolError(null);
        }}
      >
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>Edit network pool · {env.name}</DialogTitle>
            <DialogDescription>
              The replacement must be a canonical IPv4 CIDR, contain every
              existing Zone subnet, and overlap no other Environment allocation.
            </DialogDescription>
          </DialogHeader>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="environment-network-pool">Network pool</Label>
            <Input
              id="environment-network-pool"
              value={networkPool}
              onChange={(event) => setNetworkPool(event.target.value)}
              placeholder="10.40.0.0/16"
              autoFocus
            />
            {networkPoolError && (
              <p className="text-xs text-destructive">{networkPoolError}</p>
            )}
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              disabled={networkPoolSaving}
              onClick={() => setNetworkPoolOpen(false)}
            >
              Cancel
            </Button>
            <Button
              disabled={
                networkPoolSaving ||
                !networkPool.trim() ||
                networkPool.trim() === env.networkPool
              }
              onClick={async () => {
                setNetworkPoolSaving(true);
                setNetworkPoolError(null);
                try {
                  await store.editEnvironment(env.id, networkPool.trim());
                  setNetworkPoolOpen(false);
                } catch (error) {
                  setNetworkPoolError(
                    error instanceof Error
                      ? error.message
                      : "Unable to edit Environment network pool",
                  );
                } finally {
                  setNetworkPoolSaving(false);
                }
              }}
            >
              {networkPoolSaving ? "Saving…" : "Save pool"}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>

      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <ShieldCheck className="size-4 text-muted-foreground" /> Encryption
            key · age
          </CardTitle>
          <Button
            variant="outline"
            size="sm"
            disabled={!env.age || keyAction !== null}
            onClick={async () => {
              if (
                !env.age ||
                !window.confirm(
                  "Rotate this Environment age key? Previous recovery points require the previously exported identity.",
                )
              )
                return;
              setKeyAction("rotate");
              setKeyError(null);
              try {
                const taskID = await store.rotateBackupKey(env.id);
                setKeyError(`Rotation task ${taskID} dispatched.`);
              } catch (error) {
                setKeyError(
                  error instanceof Error
                    ? error.message
                    : "Unable to rotate the backup age key",
                );
              } finally {
                setKeyAction(null);
              }
            }}
          >
            <RotateCw
              className={cn(
                "size-3.5",
                keyAction === "rotate" && "animate-spin",
              )}
            />{" "}
            {keyAction === "rotate" ? "Rotating…" : "Rotate key"}
          </Button>
        </CardHeader>
        <CardContent className="flex flex-col gap-1.5">
          {env.age ? (
            <>
              <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
                <span className="font-mono text-[11px] text-muted-foreground">
                  recipient (public — encrypts backups)
                </span>
                <span className="flex items-center gap-2">
                  <span className="truncate font-mono text-xs text-foreground">
                    {env.age.recipient}
                  </span>
                  <CopyButton value={env.age.recipient} />
                </span>
              </div>
              <div className="flex flex-col gap-2 rounded-lg border border-border bg-surface px-3 py-2 sm:flex-row sm:items-center sm:justify-between">
                <span className="font-mono text-[11px] text-muted-foreground">
                  identity export (private — decrypts, wrapped at rest)
                </span>
                <span className="flex min-w-0 items-center gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={keyAction !== null}
                    onClick={async () => {
                      keyExportController.current?.abort();
                      const controller = new AbortController();
                      keyExportController.current = controller;
                      setKeyAction("export");
                      setKeyError(null);
                      try {
                        await store.exportBackupKey(env.id, controller.signal);
                      } catch (error) {
                        if (!controller.signal.aborted) {
                          setKeyError(
                            error instanceof Error
                              ? error.message
                              : "Unable to export the backup age identity",
                          );
                        }
                      } finally {
                        if (keyExportController.current === controller) {
                          keyExportController.current = null;
                          setKeyAction(null);
                        }
                      }
                    }}
                  >
                    {keyAction === "export" ? "Exporting…" : "Export identity"}
                  </Button>
                </span>
              </div>
              <p className="text-xs text-muted-foreground">
                Backups encrypt with the public recipient (safe in desired
                state). The Controller keeps the private identity wrapped
                outside the ordinary Console store; export is a repeatable
                transient no-store response to keep off-host for disaster
                recovery. Rotating affects new backups only; previous recovery
                points need the previously exported identity.
              </p>
              {env.age.lastRotatedAt && (
                <p className="text-xs text-muted-foreground">
                  generated {env.age.generatedAt} · last rotated{" "}
                  {env.age.lastRotatedAt}
                </p>
              )}
              {keyError && (
                <p className="text-xs text-muted-foreground">{keyError}</p>
              )}
            </>
          ) : (
            <p className="text-xs text-muted-foreground">
              Not generated yet — the keypair is created lazily when backups are
              first enabled on the Backups tab. A staging/dev environment that
              never backs up gets no key at all.
            </p>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-destructive">
            <Trash2 className="size-4" /> Delete environment
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-2">
          <p className="text-xs text-muted-foreground">
            Removes this environment and everything scoped to it: its env
            entries, wrapped backup key, backups, and recovery points. The
            project and other environments are untouched.
          </p>
          <div>
            <Button
              variant="destructive"
              size="sm"
              onClick={() => setDeleteOpen(true)}
            >
              <Trash2 className="size-3.5" /> Delete {env.name}
            </Button>
          </div>
        </CardContent>
      </Card>

      <Drawer
        open={renameOpen}
        onOpenChange={(open) => {
          if (renameSaving) return;
          setRenameOpen(open);
          if (!open) setRenameError(null);
        }}
      >
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>Rename environment · {env.name}</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="ren-name">Name (label only)</Label>
              <Input
                id="ren-name"
                value={newName}
                onChange={(e) => setNewName(e.target.value)}
                placeholder="production"
                autoFocus
              />
              <p className="text-xs text-muted-foreground">
                The id <span className="font-mono">{env.id}</span> stays fixed —
                every reference (secrets, age identity, backups, volume folder)
                keys off it, so renaming never breaks anything. The URL,
                deterministic env file, and activity labels follow the new name.
              </p>
              {renameError && (
                <p className="text-xs text-destructive" role="alert">
                  {renameError}
                </p>
              )}
            </div>
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              disabled={renameSaving}
              onClick={() => setRenameOpen(false)}
            >
              Cancel
            </Button>
            <Button
              disabled={
                renameSaving || !newName.trim() || newName.trim() === env.name
              }
              onClick={async () => {
                setRenameSaving(true);
                setRenameError(null);
                try {
                  const renamed = await store.renameEnvironment(
                    env.id,
                    newName.trim(),
                  );
                  setRenameOpen(false);
                  navigate(
                    `/t/${params.tenant}/${params.project}/${encodeURIComponent(renamed.name)}`,
                  );
                } catch (error) {
                  setRenameError(
                    error instanceof Error
                      ? error.message
                      : "Unable to rename Environment",
                  );
                } finally {
                  setRenameSaving(false);
                }
              }}
            >
              {renameSaving ? "Renaming…" : "Rename"}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>

      <Dialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete {env.name}?</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <p className="text-xs text-muted-foreground">
              This permanently removes the environment{" "}
              <span className="font-mono">{env.id}</span> and its env-scoped
              secrets, backups, and recovery points. Cannot be undone.
            </p>
            <div className="flex flex-col gap-1.5">
              <div className="flex items-center justify-between gap-2">
                <Label htmlFor="del-confirm">
                  Type the required value to confirm
                </Label>
                <CopyButton value={env.name} label="copy required value" />
              </div>
              <code className="select-all rounded-md border border-border bg-surface px-2.5 py-1.5 font-mono text-xs text-foreground">
                {env.name}
              </code>
              <Input
                id="del-confirm"
                placeholder={env.name}
                onChange={(e) => setConfirmTyped(e.target.value)}
                autoFocus
              />
            </div>
            {(deleteError ?? deletionFailure?.message) && (
              <p className="text-xs text-destructive" role="alert">
                {deleteError ?? deletionFailure?.message}
              </p>
            )}
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              disabled={deleteSaving}
              onClick={() => setDeleteOpen(false)}
            >
              Cancel
            </Button>
            <Button
              variant="destructive"
              disabled={deleteSaving || confirmTyped !== env.name}
              onClick={async () => {
                setDeleteSaving(true);
                setDeleteError(null);
                try {
                  await store.deleteEnvironment(env.id);
                  navigate(`/t/${params.tenant}/${params.project}`);
                } catch (error) {
                  setDeleteError(
                    error instanceof Error
                      ? error.message
                      : "Unable to delete Environment",
                  );
                  setDeleteSaving(false);
                }
              }}
            >
              <Trash2 className="size-4" />{" "}
              {deleteSaving ? "Deleting…" : "Delete environment"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
