import { useState } from "react";
import { Plus, Trash2 } from "lucide-react";
import { ResourcePanel } from "@/components/common/resource-panel";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { useStore } from "@/lib/store";
import type { Environment, Service } from "@/lib/types";

type MountDraft = { volume: string; mount: string; ro: boolean };

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
  const [draft, setDraft] = useState<MountDraft[] | null>(null);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);
  const mounts = service.mounts.filter((mount) => mount.type === "volume");
  const available = env.volumes.filter(
    (volume) => !volume.state || volume.state === "active",
  );
  function edit(add = false) {
    setError(null);
    setSaved(false);
    setDraft([
      ...mounts.map((mount) => ({
        volume: mount.volume,
        mount: mount.mount,
        ro: mount.ro ?? false,
      })),
      ...(add
        ? [{ volume: available[0]?.id ?? "", mount: "", ro: false }]
        : []),
    ]);
  }
  function update(index: number, patch: Partial<MountDraft>) {
    setDraft(
      (current) =>
        current?.map((mount, i) =>
          i === index ? { ...mount, ...patch } : mount,
        ) ?? null,
    );
  }
  async function save() {
    if (!draft) return;
    const paths = draft.map((mount) => mount.mount.trim());
    if (
      draft.some((mount) => !mount.volume) ||
      paths.some((path) => !path.startsWith("/"))
    ) {
      setError(
        "Choose a Volume and an absolute container path for every mount.",
      );
      return;
    }
    if (new Set(paths).size !== paths.length) {
      setError("Each mount needs a different container path.");
      return;
    }
    setSaving(true);
    setError(null);
    try {
      const current = await store.getService(service.id);
      await store.updateService(env.id, service.id, {
        ...current,
        volumeMounts: draft.map((mount, index) => ({
          ...mount,
          mount: paths[index],
        })),
      });
      setDraft(null);
      setSaved(true);
      onSaved?.();
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "Unable to save mounts.",
      );
    } finally {
      setSaving(false);
    }
  }
  return (
    <>
      <ResourcePanel
        title="Storage"
        actions={
          <div className="flex flex-wrap gap-2">
            {mounts.length > 0 && (
              <Button
                variant="outline"
                size="sm"
                disabled={draft !== null}
                onClick={() => edit()}
              >
                Edit mounts
              </Button>
            )}
            <Button
              variant="outline"
              size="sm"
              disabled={draft !== null}
              onClick={() => edit(true)}
            >
              <Plus className="size-3.5" /> Add mount
            </Button>
          </div>
        }
      >
        {draft === null &&
          (mounts.length ? (
            <div className="space-y-3">
              {mounts.map((mount, index) => (
                <div
                  key={index}
                  className="grid min-w-0 gap-3 rounded-lg border border-border bg-muted/20 p-4 sm:grid-cols-[1fr_2fr]"
                >
                  <div className="min-w-0 text-sm [overflow-wrap:anywhere]">
                    <p className="font-medium">
                      {env.volumes.find((volume) => volume.id === mount.volume)
                        ?.slug ?? mount.volume}
                    </p>
                    <p className="font-mono text-xs text-muted-foreground">
                      {mount.mount} · {mount.ro ? "Read only" : "Read & write"}
                    </p>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">
              No Volumes mounted. Add a mount to give this Service persistent
              storage.
            </p>
          ))}
        {saved && (
          <p role="status" className="text-sm text-success">
            Mounts saved. Deploy this Service to apply them.
          </p>
        )}
        {onFiles && (
          <Button variant="link" size="sm" onClick={onFiles}>
            Manage file Entries
          </Button>
        )}
        {draft !== null && (
          <section aria-label="Edit storage mounts" className="space-y-4">
            <p className="text-sm text-muted-foreground">
              Choose a Volume and where it appears inside this container.
              Removing a mount keeps the Volume and its data. Deploy to apply
              saved changes.
            </p>
            <form
              className="min-w-0 space-y-4"
              onSubmit={(event) => {
                event.preventDefault();
                void save();
              }}
            >
              {draft?.map((mount, index) => (
                <fieldset
                  key={index}
                  disabled={saving}
                  className="grid min-w-0 gap-4 rounded-xl border border-primary/25 bg-muted/10 p-4 md:grid-cols-2"
                >
                  <legend className="px-1 text-xs text-muted-foreground">
                    Mount {index + 1}
                  </legend>
                  <div className="space-y-1.5">
                    <Label htmlFor={`mount-volume-${index}`}>Volume</Label>
                    <Select
                      searchable
                      id={`mount-volume-${index}`}
                      value={mount.volume}
                      onValueChange={(volume) => update(index, { volume })}
                      options={[
                        ...available.map((volume) => ({
                          value: volume.id,
                          label: volume.slug,
                        })),
                        ...(mount.volume &&
                        !available.some((volume) => volume.id === mount.volume)
                          ? [
                              {
                                value: mount.volume,
                                label: `${env.volumes.find((volume) => volume.id === mount.volume)?.slug ?? mount.volume} (unavailable)`,
                              },
                            ]
                          : []),
                      ]}
                    />
                  </div>
                  <div className="space-y-1.5">
                    <Label htmlFor={`mount-path-${index}`}>
                      Container path
                    </Label>
                    <Input
                      id={`mount-path-${index}`}
                      required
                      placeholder="/app/data"
                      value={mount.mount}
                      onChange={(event) =>
                        update(index, { mount: event.target.value })
                      }
                    />
                  </div>
                  <div className="flex items-center justify-between gap-3 md:col-span-2">
                    <label className="flex items-center gap-2 text-sm">
                      <Checkbox
                        checked={mount.ro}
                        onChange={(event) =>
                          update(index, { ro: event.target.checked })
                        }
                      />{" "}
                      Read only
                    </label>
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      onClick={() =>
                        setDraft(
                          (current) =>
                            current?.filter((_, i) => i !== index) ?? null,
                        )
                      }
                    >
                      <Trash2 className="size-3.5" /> Remove mount
                    </Button>
                  </div>
                </fieldset>
              ))}
              {!available.length && (
                <div className="space-y-2">
                  <p className="text-sm text-muted-foreground">
                    Create a Volume in this Environment’s Volumes page, then
                    select it here.
                  </p>
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
              {draft?.length === 0 && (
                <p className="text-sm text-muted-foreground">
                  This Service will have no Volume mounts after its next deploy.
                </p>
              )}
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={saving || !available.length}
                onClick={() =>
                  setDraft((current) => [
                    ...(current ?? []),
                    { volume: available[0]?.id ?? "", mount: "", ro: false },
                  ])
                }
              >
                <Plus className="size-3.5" /> Add another mount
              </Button>
              {error && (
                <p
                  role="alert"
                  className="text-sm text-destructive [overflow-wrap:anywhere]"
                >
                  {error}
                </p>
              )}
              <div className="sticky bottom-0 flex justify-end gap-2 border-t border-border bg-card py-3">
                <Button
                  type="button"
                  variant="outline"
                  disabled={saving}
                  onClick={() => setDraft(null)}
                >
                  Cancel
                </Button>
                <Button type="submit" disabled={saving}>
                  {saving ? "Saving…" : "Save mounts"}
                </Button>
              </div>
            </form>
          </section>
        )}
      </ResourcePanel>
    </>
  );
}
