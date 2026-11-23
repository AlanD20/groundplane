import { InspectionWorkspace } from "@/components/common/inspection-workspace";
import { TaskRunAfterSelector } from "@/features/task/task-run-after-selector";
import { EmptyState } from "@/components/common/empty-state";
import { CreationButton } from "@/components/common/creation-button";
import {
  ResourceForm,
  ResourceFormHeader as DialogHeader,
  ResourceFormTitle as DialogTitle,
  ResourceFormFooter as DialogFooter,
} from "@/components/common/resource-form";
("use client");

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
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import type { Environment, VolumeDeletionImpactPage } from "@/lib/types";
import { Database, Pencil, Plus, Trash2 } from "lucide-react";
import { useEffect, useEffectEvent, useState } from "react";

export function EnvironmentVolumeManager({ env }: { env: Environment }) {
  const store = useStore();
  const params = useRequiredParams("tenant");
  const [open, setOpen] = useState(false);
  const [selectedId, setSelectedId] = useState<string>();
  const [detailError, setDetailError] = useState<string>();
  const [detailLoading, setDetailLoading] = useState(false);
  const [slug, setSlug] = useState("");
  const [key, setKey] = useState("");
  const [createAfter, setCreateAfter] = useState<string[]>([]);
  const [editAfter, setEditAfter] = useState<string[]>([]);
  const [afterLoading, setAfterLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [editing, setEditing] = useState<Environment["volumes"][number] | null>(
    null,
  );
  const [editSlug, setEditSlug] = useState("");
  const [resolvedVolume, setResolvedVolume] = useState<
    Environment["volumes"][number] | null
  >(null);
  const [removing, setRemoving] = useState<
    Environment["volumes"][number] | null
  >(null);
  const [impactPages, setImpactPages] = useState<VolumeDeletionImpactPage[]>(
    [],
  );
  const [impactError, setImpactError] = useState<string | null>(null);
  const [impactLoading, setImpactLoading] = useState(false);
  const [query, setQuery] = useState("");
  const table = useTableView(
    env.volumes.filter((volume) =>
      `${volume.slug} ${volume.key}`
        .toLowerCase()
        .includes(query.trim().toLowerCase()),
    ),
    { slug: (volume) => volume.slug, key: (volume) => volume.key },
    "slug",
    "asc",
    `${env.id}/${query}`,
  );

  const selected =
    table.rows.find((volume) => volume.id === selectedId) ?? table.rows[0];
  const volume =
    selected && resolvedVolume?.id === selected.id ? resolvedVolume : selected;
  const loadVolume = useEffectEvent((id: string) => store.getVolume(id));
  useEffect(() => {
    const id = selected?.id;
    setResolvedVolume(null);
    setDetailError(undefined);
    setDetailLoading(!!id);
    if (!id) return;
    let current = true;
    void loadVolume(id)
      .then(
        (detail) => {
          if (current) setResolvedVolume(detail);
        },
        (cause: unknown) => {
          if (current)
            setDetailError(
              cause instanceof Error ? cause.message : "Unable to load volume",
            );
        },
      )
      .finally(() => {
        if (current) setDetailLoading(false);
      });
    return () => {
      current = false;
    };
  }, [selected?.id, selected?.slug, selected?.state, selected?.currentTaskId]);

  useEffect(() => {
    if (!removing) {
      setImpactPages([]);
      setImpactError(null);
      setImpactLoading(false);
      return;
    }
    let cancelled = false;
    setImpactPages([]);
    setImpactError(null);
    setImpactLoading(true);
    void (async () => {
      try {
        const pages: VolumeDeletionImpactPage[] = [];
        const seen = new Set<string>();
        let runningCount = 0;
        let cursor = "";
        let identity = "";
        let revision = 0;
        let head = "";
        for (let pageNumber = 0; pageNumber < 1024; pageNumber += 1) {
          const page = await store.getVolumeDeletionImpact(
            removing.id,
            cursor,
            40,
          );
          if (
            page.volume_id !== removing.id ||
            page.environment_id !== removing.environmentId ||
            page.slug !== removing.slug ||
            page.key !== removing.key ||
            page.rolling_digest === "" ||
            page.data_handling !== "recursive_destroy"
          )
            throw new Error(
              "Controller returned an invalid volume deletion-impact page",
            );
          runningCount += page.items.length;
          if (page.item_count !== runningCount)
            throw new Error("Volume deletion impact count does not join");
          for (const item of page.items) {
            if (seen.has(item.id))
              throw new Error("Volume deletion impact item repeated");
            seen.add(item.id);
          }
          if (!identity) {
            identity = page.volume_id;
            revision = page.revision;
            head = page.environment_head;
          } else if (
            page.volume_id !== identity ||
            page.revision !== revision ||
            page.environment_head !== head
          ) {
            throw new Error(
              "Volume deletion impact changed while it was being reviewed",
            );
          }
          if (page.complete) {
            if (page.next_cursor)
              throw new Error("Final impact page unexpectedly has a cursor");
            if (!page.impact_token)
              throw new Error(
                "Final impact page did not include an impact token",
              );
            pages.push(page);
            const itemCount = pages.reduce(
              (total, itemPage) => total + itemPage.items.length,
              0,
            );
            if (itemCount !== page.item_count)
              throw new Error("Volume deletion impact count is inconsistent");
            break;
          }
          if (!page.next_cursor || page.impact_token)
            throw new Error("Incomplete impact page has invalid pagination");
          pages.push(page);
          cursor = page.next_cursor;
          if (pageNumber === 1023)
            throw new Error("Volume deletion impact exceeded the page limit");
        }
        if (!pages.at(-1)?.complete)
          throw new Error("Volume deletion impact did not reach a final page");
        if (!cancelled) setImpactPages(pages);
      } catch (cause: unknown) {
        if (!cancelled)
          setImpactError(
            cause instanceof Error
              ? cause.message
              : "Unable to load deletion impact",
          );
      } finally {
        if (!cancelled) setImpactLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [removing, store]);

  const finalImpact = impactPages.at(-1);
  const impactItems = impactPages.flatMap((page) => page.items);
  const createForm = (
    <ResourceForm open={open} onOpenChange={setOpen}>
      <DialogHeader>
        <DialogTitle>Add volume · {env.name}</DialogTitle>
      </DialogHeader>
      <div className="grid max-w-4xl gap-4 sm:grid-cols-2">
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="v-slug">Slug</Label>
          <Input
            id="v-slug"
            value={slug}
            onChange={(e) => setSlug(e.target.value)}
            placeholder="app-data"
            autoFocus
          />
        </div>
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="v-key">Compose key (optional)</Label>
          <Input
            id="v-key"
            value={key}
            onChange={(e) => setKey(e.target.value)}
            placeholder="app_data"
          />
        </div>

        {saveError && <p className="text-xs text-destructive">{saveError}</p>}
      </div>
      <TaskRunAfterSelector
        value={createAfter}
        onChange={setCreateAfter}
        onBusyChange={setAfterLoading}
      />
      <DialogFooter>
        <Button variant="outline" onClick={() => setOpen(false)}>
          Cancel
        </Button>
        <Button
          disabled={saving || afterLoading || !slug.trim()}
          onClick={async () => {
            setSaveError(null);
            setSaving(true);
            try {
              await store.addVolume(env.id, {
                slug: slug.trim(),
                after: createAfter,
                key: key.trim() || undefined,
              });
              setOpen(false);
              setCreateAfter([]);
              setSlug("");
              setKey("");
            } catch (cause: unknown) {
              setSaveError(
                cause instanceof Error ? cause.message : "Unable to add volume",
              );
            } finally {
              setSaving(false);
            }
          }}
        >
          Add volume
        </Button>
      </DialogFooter>
    </ResourceForm>
  );
  const editor = (
    <ResourceForm
      open={!!editing}
      onOpenChange={(next) => !next && setEditing(null)}
    >
      <DialogHeader>
        <DialogTitle>Edit volume · {editing?.slug}</DialogTitle>
      </DialogHeader>
      <div className="flex max-w-lg flex-col gap-4">
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="volume-edit-slug">Slug</Label>
          <Input
            id="volume-edit-slug"
            value={editSlug}
            onChange={(event) => setEditSlug(event.target.value)}
            autoFocus
          />
        </div>
        <p className="text-xs text-muted-foreground">
          The Compose key and storage path stay unchanged.
        </p>
        {saveError && <p className="text-xs text-destructive">{saveError}</p>}
      </div>
      <TaskRunAfterSelector
        value={editAfter}
        onChange={setEditAfter}
        onBusyChange={setAfterLoading}
      />
      <DialogFooter>
        <Button variant="outline" onClick={() => setEditing(null)}>
          Cancel
        </Button>
        <Button
          disabled={saving || afterLoading || !editSlug.trim()}
          onClick={async () => {
            if (!editing) return;
            setSaveError(null);
            setSaving(true);
            try {
              await store.updateVolume(env.id, editing.id, {
                slug: editSlug.trim(),
                after: editAfter,
              });
              setEditing(null);
            } catch (cause: unknown) {
              setSaveError(
                cause instanceof Error
                  ? cause.message
                  : "Unable to edit volume",
              );
            } finally {
              setSaving(false);
            }
          }}
        >
          Save slug
        </Button>
      </DialogFooter>
    </ResourceForm>
  );
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <Database className="size-4 text-muted-foreground" /> Named volumes
        </CardTitle>
        <CreationButton
          active={open}
          variant="outline"
          size="sm"
          onOpenChange={setOpen}
        >
          <Plus className="size-3.5" /> Add Volume
        </CreationButton>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        {createForm}
        {env.volumes.length ? (
          <InspectionWorkspace
            title="Volume list"
            selectionLabel="Select volume"
            detailsLabel="Selected volume"
            emptyMessage="No volumes match your search."
            selectedId={selected?.id}
            onSelect={setSelectedId}
            items={table.rows.map((item) => ({
              id: item.id,
              label: item.slug,
              description: item.key === item.slug ? undefined : item.key,
              status: (
                <span className="shrink-0 text-xs text-muted-foreground">
                  {item.state ?? "Unknown"}
                </span>
              ),
            }))}
            toolbar={
              <ListToolbar
                label="volumes"
                query={query}
                onQueryChange={setQuery}
                sort={table}
                fields={[
                  { value: "slug", label: "Name" },
                  { value: "key", label: "Compose key" },
                ]}
              />
            }
            footer={
              table.pages > 1 && (
                <TablePagination table={table} label="Volumes" />
              )
            }
          >
            {volume && (
              <div className="space-y-4">
                <header className="flex flex-wrap items-start justify-between gap-3 border-b border-border pb-3">
                  <h3 className="text-sm font-semibold [overflow-wrap:anywhere]">
                    {volume.slug}
                  </h3>
                  <div className="flex flex-wrap gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      aria-label={`Edit volume ${volume.slug}`}
                      aria-expanded={editing?.id === volume.id}
                      disabled={saving}
                      onClick={() => {
                        if (editing?.id === volume.id) {
                          setEditing(null);
                          return;
                        }
                        setSaveError(null);
                        setEditAfter([]);
                        setEditSlug(volume.slug);
                        setEditing(volume);
                      }}
                    >
                      <Pencil className="size-3.5" /> Edit
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      disabled={editing?.id === volume.id}
                      onClick={() => setRemoving(volume)}
                    >
                      <Trash2 className="size-3.5" /> Remove volume
                    </Button>
                  </div>
                </header>
                {editing?.id === volume.id && editor}
                {detailLoading && (
                  <p role="status" className="text-xs text-muted-foreground">
                    Refreshing volume details…
                  </p>
                )}
                {detailError && (
                  <p role="alert" className="text-sm text-destructive">
                    {detailError}
                  </p>
                )}
                {volume && (
                  <div className="grid gap-3 sm:grid-cols-2 text-xs">
                    <VolumeDetailCell
                      label="Immutable key"
                      value={volume.key}
                    />
                    <VolumeDetailCell
                      label="State"
                      value={volume.state ?? "unknown"}
                    />
                    <VolumeDetailCell
                      label="Path"
                      value={volume.path ?? "not assigned"}
                    />
                    <VolumeDetailCell label="ID" value={volume.id} />
                    <VolumeDetailCell
                      label="Environment"
                      value={volume.environmentId}
                    />
                    {volume.currentTaskId && (
                      <VolumeDetailCell
                        label="Current task"
                        value={volume.currentTaskId}
                      />
                    )}
                  </div>
                )}
              </div>
            )}
          </InspectionWorkspace>
        ) : (
          <EmptyState
            embedded
            title="No volumes yet"
            description="Create a volume to keep Service data between deployments."
          />
        )}
      </CardContent>

      <TaskRunnerDialog
        open={!!removing}
        onOpenChange={(next) => !next && setRemoving(null)}
        title={`Remove volume · ${removing?.slug ?? ""}`}
        description="Review every affected mount, backup source, policy, and recovery point before permanently removing this volume."
        type="destroy"
        target={removing?.id ?? env.id}
        workspace={params.tenant}
        destructive
        confirmText={removing?.key ?? ""}
        startDisabled={
          impactLoading || Boolean(impactError) || !finalImpact?.impact_token
        }
        startLabel="Remove volume"
        runAfter
        review={
          <div className="flex flex-col gap-2">
            {impactLoading && (
              <p className="text-sm text-muted-foreground">
                Loading all deletion consequences…
              </p>
            )}
            {impactError && (
              <p className="text-sm text-destructive">{impactError}</p>
            )}
            {!impactLoading && !impactError && (
              <>
                <p className="text-sm">
                  {impactItems.length} consequence
                  {impactItems.length === 1 ? "" : "s"} at revision{" "}
                  {finalImpact?.revision}.
                </p>
                <p className="text-xs text-warning">
                  Data handling: recursive destroy. The managed volume directory
                  and its contents will be removed.
                </p>
                <div className="max-h-64 overflow-y-auto rounded-lg border border-border bg-surface p-2">
                  {impactItems.length === 0 && (
                    <p className="p-2 text-xs text-muted-foreground">
                      No dependent records were found.
                    </p>
                  )}
                  {impactItems.map((item) => (
                    <div
                      key={item.id}
                      className="border-b border-border px-2 py-1.5 text-xs last:border-0"
                    >
                      <span className="font-mono">{item.kind}</span>
                      {item.service_name && (
                        <span className="text-muted-foreground">
                          {" "}
                          · {item.service_name}
                        </span>
                      )}
                      {item.mount_target && (
                        <span className="text-muted-foreground">
                          {" "}
                          · {item.mount_target}
                        </span>
                      )}
                      {item.source_id && (
                        <span className="text-muted-foreground">
                          {" "}
                          · source {item.source_id}
                        </span>
                      )}
                      {item.recovery_point_count !== undefined && (
                        <span className="text-muted-foreground">
                          {" "}
                          · {item.recovery_point_count} recovery points
                        </span>
                      )}
                    </div>
                  ))}
                </div>
              </>
            )}
          </div>
        }
        onDispatch={(after) => {
          if (!removing || !finalImpact?.impact_token)
            return Promise.reject(new Error("Deletion impact is not ready"));
          return store.removeVolume(
            env.id,
            removing.id,
            finalImpact.impact_token,
            removing.key,
            after,
          );
        }}
      />
    </Card>
  );
}

function VolumeDetailCell({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface px-3 py-2">
      <span className="text-xs uppercase tracking-wider text-muted-foreground">
        {label}
      </span>
      <span className="break-all font-mono text-xs">{value}</span>
    </div>
  );
}
