import {
  workspaceSectionClassName,
  editorFooterClassName,
} from "@/components/common/workspace-section";
import { useRef, useState } from "react";
import { HardDrive, Pencil, Plus, Trash2 } from "lucide-react";
import { ResourcePanel } from "@/components/common/resource-panel";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { useStore } from "@/lib/store";
import type { Environment, Service } from "@/lib/types";

type Mount = { volume: string; mount: string; ro: boolean };
type Draft = { original: Mount | null; value: Mount; removing: boolean };
const sameMount = (a: Mount, b: Mount) =>
  a.volume === b.volume && a.mount === b.mount && a.ro === b.ro;

export function ServiceStorage({
  env,
  service,
  onSaved,
  onFiles,
  onVolumes,
}: {
  env: Environment;
  service: Service;
  onSaved?: () => void;
  onFiles?: () => void;
  onVolumes?: () => void;
}) {
  const store = useStore();
  const [draft, setDraft] = useState<Draft | null>(null);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saved, setSaved] = useState<{ path: string; action: string } | null>(
    null,
  );
  const editButtons = useRef<Record<string, HTMLButtonElement | null>>({});
  const mounts = service.mounts
    .filter((mount) => mount.type === "volume")
    .map((mount) => ({
      volume: mount.volume,
      mount: mount.mount,
      ro: mount.ro ?? false,
    }));
  const available = env.volumes.filter(
    (volume) => !volume.state || volume.state === "active",
  );
  function edit(original: Mount | null, removing = false) {
    setError(null);
    setSaved(null);
    setDraft({
      original,
      removing,
      value: original ?? {
        volume: available[0]?.id ?? "",
        mount: "",
        ro: false,
      },
    });
  }
  function close() {
    setDraft(null);
    setError(null);
    const path = draft?.original?.mount;
    requestAnimationFrame(() =>
      (editButtons.current[path ?? "add"] ?? editButtons.current.add)?.focus({
        preventScroll: true,
      }),
    );
  }
  async function save() {
    if (!draft) return;
    const value = { ...draft.value, mount: draft.value.mount.trim() };
    if (!draft.removing && (!value.volume || !value.mount.startsWith("/"))) {
      setError("Choose a Volume and an absolute container path.");
      return;
    }
    setSaving(true);
    setError(null);
    try {
      const current = await store.getService(service.id);
      const latest = current.mounts
        .filter((mount) => mount.type === "volume")
        .map((mount) => ({
          volume: mount.volume,
          mount: mount.mount,
          ro: mount.ro ?? false,
        }));
      const index = draft.original
        ? latest.findIndex((mount) => sameMount(mount, draft.original!))
        : -1;
      if (draft.original && index < 0)
        throw new Error(
          "This mount changed while you were editing. Cancel and reopen it to use its current settings.",
        );
      const next = latest.filter((_, i) => i !== index);
      if (!draft.removing) {
        if (
          current.mounts.some(
            (mount) =>
              mount.mount === value.mount &&
              (mount.type !== "volume" ||
                !draft.original ||
                !sameMount(
                  { ...mount, ro: mount.ro ?? false },
                  draft.original,
                )),
          )
        )
          throw new Error("Another mount already uses this container path.");
        next.splice(index < 0 ? next.length : index, 0, value);
      }
      await store.updateService(env.id, service.id, {
        ...current,
        volumeMounts: next,
      });
      setSaved({
        path: draft.removing ? draft.original!.mount : value.mount,
        action: draft.removing
          ? "removed"
          : draft.original
            ? "updated"
            : "added",
      });
      close();
      onSaved?.();
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "Unable to save mount.",
      );
    } finally {
      setSaving(false);
    }
  }
  const editor = draft && (
    <form
      aria-label={
        draft.removing
          ? "Remove mount"
          : draft.original
            ? "Edit mount"
            : "New mount"
      }
      className={workspaceSectionClassName(true, "space-y-4")}
      onSubmit={(event) => {
        event.preventDefault();
        void save();
      }}
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-medium">
          {draft.removing
            ? "Remove mount"
            : draft.original
              ? "Edit mount"
              : "New mount"}
        </h3>
        <Badge variant="warning">Unsaved</Badge>
      </div>
      {draft.removing ? (
        <p className="text-sm [overflow-wrap:anywhere]">
          Remove the mount at <code>{draft.original?.mount}</code>? The Volume
          and its data are kept. Deploy to apply this change.
        </p>
      ) : (
        <fieldset
          disabled={saving}
          className="grid min-w-0 gap-4 sm:grid-cols-2"
        >
          <div className="min-w-0 space-y-1.5">
            <Label htmlFor="mount-volume">Volume</Label>
            <Select
              searchable
              id="mount-volume"
              value={draft.value.volume}
              onValueChange={(volume) =>
                setDraft({ ...draft, value: { ...draft.value, volume } })
              }
              options={[
                ...available.map((volume) => ({
                  value: volume.id,
                  label: volume.slug,
                })),
                ...(draft.value.volume &&
                !available.some((volume) => volume.id === draft.value.volume)
                  ? [
                      {
                        value: draft.value.volume,
                        label: `${env.volumes.find((volume) => volume.id === draft.value.volume)?.slug ?? draft.value.volume} (unavailable)`,
                      },
                    ]
                  : []),
              ]}
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="mount-path">Container path</Label>
            <Input
              id="mount-path"
              autoFocus
              required
              placeholder="/app/data"
              value={draft.value.mount}
              onChange={(event) =>
                setDraft({
                  ...draft,
                  value: { ...draft.value, mount: event.target.value },
                })
              }
            />
          </div>
          <label className="flex items-center gap-2 text-sm sm:col-span-2">
            <Checkbox
              checked={draft.value.ro}
              onChange={(event) =>
                setDraft({
                  ...draft,
                  value: { ...draft.value, ro: event.target.checked },
                })
              }
            />{" "}
            Read only
          </label>
        </fieldset>
      )}
      {!available.length && !draft.removing && (
        <div className="space-y-2 text-sm text-muted-foreground">
          <p>Create a Volume in this Environment before adding a mount.</p>
          {onVolumes && (
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={onVolumes}
            >
              Open Environment Volumes
            </Button>
          )}
        </div>
      )}
      {error && (
        <p
          role="alert"
          className="text-sm text-destructive [overflow-wrap:anywhere]"
        >
          {error}
        </p>
      )}
      <div
        className={`${editorFooterClassName} flex flex-wrap items-center justify-between gap-3`}
      >
        <p className="text-xs text-muted-foreground">
          Saved changes take effect on the next deploy.
        </p>
        <div className="flex gap-2">
          <Button
            type="button"
            variant="outline"
            disabled={saving}
            onClick={close}
          >
            Cancel
          </Button>
          <Button
            type="submit"
            variant={draft.removing ? "destructive" : "default"}
            disabled={saving || (!draft.removing && !draft.value.volume)}
          >
            {saving
              ? "Saving…"
              : draft.removing
                ? "Remove mount"
                : draft.original
                  ? "Save mount"
                  : "Add mount"}
          </Button>
        </div>
      </div>
    </form>
  );
  return (
    <ResourcePanel
      title="Storage"
      actions={
        <Button
          variant="outline"
          size="sm"
          disabled={!!draft}
          ref={(node) => {
            editButtons.current.add = node;
          }}
          onClick={() => edit(null)}
        >
          <Plus className="size-3.5" /> Add mount
        </Button>
      }
    >
      <p className="text-sm text-muted-foreground">
        Mount Environment Volumes at paths inside this Service. Each mount is
        saved independently.
      </p>
      {draft && !draft.original && editor}
      <div className="space-y-2">
        {mounts.map((mount) =>
          draft?.original && sameMount(mount, draft.original) ? (
            <div key={mount.mount}>{editor}</div>
          ) : (
            <div
              key={mount.mount}
              className={workspaceSectionClassName(
                false,
                "flex flex-wrap items-center justify-between gap-3",
              )}
            >
              <div className="flex min-w-0 flex-1 items-start gap-3">
                <HardDrive
                  aria-hidden
                  className="mt-1 size-4 shrink-0 text-muted-foreground"
                />
                <div className="min-w-0 space-y-1">
                  <p className="text-sm font-medium [overflow-wrap:anywhere]">
                    {env.volumes.find((volume) => volume.id === mount.volume)
                      ?.slug ?? mount.volume}
                  </p>
                  <p className="break-all font-mono text-sm">{mount.mount}</p>
                  <p className="text-xs text-muted-foreground">
                    {mount.ro ? "Read only" : "Read & write"}
                    {saved?.path === mount.mount && (
                      <span className="ml-2 text-success">
                        Just {saved.action}
                      </span>
                    )}
                  </p>
                </div>
              </div>
              <div className="flex shrink-0 gap-1">
                <Button
                  variant="ghost"
                  size="sm"
                  disabled={!!draft}
                  aria-label={`Edit mount ${mount.mount}`}
                  ref={(node) => {
                    editButtons.current[mount.mount] = node;
                  }}
                  onClick={() => edit(mount)}
                >
                  <Pencil className="size-3.5" /> Edit
                </Button>
                <Button
                  variant="ghost"
                  size="icon-sm"
                  disabled={!!draft}
                  aria-label={`Remove mount ${mount.mount}`}
                  onClick={() => edit(mount, true)}
                >
                  <Trash2 className="size-3.5" />
                </Button>
              </div>
            </div>
          ),
        )}
        {draft?.original &&
          !mounts.some((mount) => sameMount(mount, draft.original!)) &&
          editor}
      </div>
      {!mounts.length && !draft && (
        <p className="text-sm text-muted-foreground">
          No Volumes mounted. Add a mount to give this Service persistent
          storage.
        </p>
      )}
      {saved && (
        <p
          role="status"
          className="text-sm text-success [overflow-wrap:anywhere]"
        >
          Mount at {saved.path} {saved.action}. Deploy this Service to apply the
          change.
        </p>
      )}
      {onFiles && (
        <Button variant="link" size="sm" onClick={onFiles}>
          Manage file Entries
        </Button>
      )}
    </ResourcePanel>
  );
}
