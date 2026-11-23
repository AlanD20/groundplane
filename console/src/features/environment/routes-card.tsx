import { InspectionWorkspace } from "@/components/common/inspection-workspace";
import {
  ListToolbar,
  TablePagination,
  useTableView,
} from "@/components/common/table-controls";
import { readRoute } from "./network-api";
import { RouteFormDialog } from "./route-form";
import { TaskRunAfterSelector } from "@/features/task/task-run-after-selector";
import { EmptyState } from "@/components/common/empty-state";
import { CreationButton } from "@/components/common/creation-button";
import { RouteAccessCheck } from "./edge-check-panel";
import {
  ResourceForm,
  ResourceFormHeader as DialogHeader,
  ResourceFormTitle as DialogTitle,
  ResourceFormFooter as DialogFooter,
} from "@/components/common/resource-form";
("use client");

import { CompactReference } from "@/components/common/compact-reference";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import type { Environment, Route } from "@/lib/types";
import { Pencil, Plus, Router as RouterIcon, Trash2 } from "lucide-react";
import { useEffect, useState } from "react";

// ---- Routes ----

export function RoutesCard({ env }: { env: Environment }) {
  const store = useStore();
  const params = useRequiredParams("tenant");
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [selectedId, setSelectedId] = useState<string>();
  const [removing, setRemoving] = useState<Route | null>(null);

  const rows = env.routes.map((route) => ({
    route,
    address: `${route.host || "internal"}${route.path}`,
    service:
      env.services.find((item) => item.id === route.targetServiceId)?.name ??
      route.targetServiceId,
  }));
  const table = useTableView(
    rows.filter(({ route, address, service }) =>
      `${address} ${service} ${route.targetPort} ${route.exposure} ${route.status}`
        .toLowerCase()
        .includes(query.trim().toLowerCase()),
    ),
    {
      address: ({ address }) => address,
      service: ({ service }) => service,
      exposure: ({ route }) => route.exposure,
    },
    "address",
    "asc",
    `${env.id}/${query}`,
  );
  const selected =
    table.rows.find(({ route }) => route.id === selectedId) ?? table.rows[0];
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <RouterIcon className="size-4 text-muted-foreground" /> Routes
        </CardTitle>
        <CreationButton
          active={open}
          variant="outline"
          size="sm"
          onOpenChange={setOpen}
        >
          <Plus className="size-3.5" /> Add Route
        </CreationButton>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <RouteFormDialog env={env} open={open} onOpenChange={setOpen} />
        {!env.routes.length && (
          <EmptyState
            embedded
            title="No Routes yet"
            description="Add a Route to direct requests to a Service."
          />
        )}
        {env.routes.length > 0 && (
          <InspectionWorkspace
            title="Route list"
            selectionLabel="Select route"
            detailsLabel="Selected route"
            actionId="route.show"
            emptyMessage="No routes match your search."
            selectedId={selected?.route.id}
            onSelect={setSelectedId}
            items={table.rows.map(({ route, address, service }) => ({
              id: route.id,
              label: address,
              description: `${service}:${route.targetPort}`,
              status: <ExposurePill exposure={route.exposure} />,
            }))}
            toolbar={
              <ListToolbar
                label="routes"
                query={query}
                onQueryChange={setQuery}
                sort={table}
                fields={[
                  { value: "address", label: "Host and path" },
                  { value: "service", label: "Destination" },
                  { value: "exposure", label: "Exposure" },
                ]}
              />
            }
            footer={
              table.pages > 1 && (
                <TablePagination table={table} label="Routes" />
              )
            }
          >
            {selected && (
              <SelectedRoute
                key={selected.route.id}
                env={env}
                route={selected.route}
                onRemove={() => setRemoving(selected.route)}
              />
            )}
          </InspectionWorkspace>
        )}
      </CardContent>
      <TaskRunnerDialog
        runAfter
        open={!!removing}
        onOpenChange={(next) => !next && setRemoving(null)}
        title={`Remove route · ${removing ? `${removing.host || "internal"}${removing.path}` : ""}`}
        description="Traffic will no longer use this route."
        type="destroy"
        target={removing?.id ?? env.id}
        workspace={params.tenant}
        destructive
        startLabel="Remove route"
        onDispatch={async (after) => {
          if (!removing) throw new Error("No Route selected for removal");
          return store.removeRoute(env.id, removing.id, after);
        }}
      />
    </Card>
  );
}

function SelectedRoute({
  env,
  route,
  onRemove,
}: {
  env: Environment;
  route: Route;
  onRemove: () => void;
}) {
  const { updateRoute } = useStore();
  const [editing, setEditing] = useState(false);
  const [exposure, setExposure] = useState(route.exposure);
  const [after, setAfter] = useState<string[]>([]);
  const [afterLoading, setAfterLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [editError, setEditError] = useState<string>();
  const [detail, setDetail] = useState<Route>();
  const [error, setError] = useState<string>();
  useEffect(() => {
    const controller = new AbortController();
    setError(undefined);
    void readRoute(route.id, controller.signal)
      .then((next) => {
        if (!controller.signal.aborted) setDetail(next);
      })
      .catch((cause: unknown) => {
        if (!controller.signal.aborted)
          setError(
            cause instanceof Error
              ? cause.message
              : "Unable to load route details",
          );
      });
    return () => controller.abort();
  }, [route.id, route.status, route.exposure]);
  const detailRoute = detail ?? route;

  function toggleEditor() {
    if (saving) return;
    if (editing) {
      setEditing(false);
      return;
    }
    setExposure(detailRoute.exposure);
    setAfter([]);
    setEditError(undefined);
    setEditing(true);
  }

  async function saveExposure() {
    setSaving(true);
    setEditError(undefined);
    try {
      await updateRoute(env.id, route.id, { exposure, after });
      setEditing(false);
    } catch (cause) {
      setEditError(
        cause instanceof Error ? cause.message : "Unable to edit route",
      );
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="space-y-4">
      <header className="flex flex-wrap items-start justify-between gap-3 border-b border-border pb-3">
        <h3 className="min-w-0 text-sm font-semibold [overflow-wrap:anywhere]">
          {route.host || "internal"}
          {route.path}
        </h3>
        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            aria-expanded={editing}
            disabled={saving}
            onClick={toggleEditor}
          >
            <Pencil className="size-3.5" /> Edit exposure
          </Button>
          <Button
            variant="ghost"
            size="sm"
            disabled={editing}
            onClick={onRemove}
          >
            <Trash2 className="size-3.5" /> Remove route
          </Button>
        </div>
      </header>
      <ResourceForm
        open={editing}
        onOpenChange={(open) => {
          if (!open && !saving) setEditing(false);
        }}
      >
        <DialogHeader>
          <DialogTitle>Edit exposure</DialogTitle>
        </DialogHeader>
        <div className="space-y-1.5">
          <Label htmlFor="route-edit-exposure">Exposure</Label>
          <Select
            id="route-edit-exposure"
            value={exposure}
            disabled={saving}
            onValueChange={(value) => setExposure(value as Route["exposure"])}
            options={[
              {
                value: "public",
                label: "Public · requires an ingress component",
              },
              { value: "internal", label: "Internal · no host port" },
            ]}
          />
        </div>
        {editError && (
          <p role="alert" className="text-sm text-destructive">
            {editError}
          </p>
        )}
        <fieldset disabled={saving}>
          <TaskRunAfterSelector
            value={after}
            onChange={setAfter}
            onBusyChange={setAfterLoading}
          />
        </fieldset>
        <DialogFooter>
          <Button
            variant="outline"
            disabled={saving}
            onClick={() => setEditing(false)}
          >
            Cancel
          </Button>
          <Button
            data-action-id="route.edit"
            disabled={saving || afterLoading}
            onClick={() => void saveExposure()}
          >
            {saving ? "Saving…" : "Save exposure"}
          </Button>
        </DialogFooter>
      </ResourceForm>
      {error && (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      )}
      <div className="grid max-w-4xl gap-4 sm:grid-cols-2">
        <div className="min-w-0 space-y-2 rounded-lg border border-border p-3">
          <p className="text-xs font-medium text-muted-foreground">
            Incoming request
          </p>
          <p className="break-all font-mono text-sm">
            {detailRoute.host || "Any internal host"}
            {detailRoute.path}
          </p>
          <ExposurePill exposure={detailRoute.exposure} />
        </div>
        <div className="min-w-0 space-y-2 rounded-lg border border-border p-3">
          <p className="text-xs font-medium text-muted-foreground">
            Destination Service
          </p>
          <p className="break-all font-mono text-sm">
            {env.services.find(
              (service) => service.id === detailRoute.targetServiceId,
            )?.name ?? detailRoute.targetServiceId}
          </p>
          <p className="text-xs text-muted-foreground">
            Port{" "}
            <span className="font-mono text-foreground">
              {detailRoute.targetPort}
            </span>
          </p>
        </div>
      </div>
      <p className="mt-4 text-sm">
        <span className="text-muted-foreground">Serving status: </span>
        {detailRoute.status}
      </p>
      {detailRoute.exposure === "public" && (
        <div className="mt-4">
          <RouteAccessCheck
            key={detailRoute.id}
            environmentId={env.id}
            route={detailRoute}
            components={env.components}
            open
          />
        </div>
      )}
      <div className="mt-4 flex flex-wrap gap-3 border-t border-border pt-3">
        <CompactReference label="Route ID" value={detailRoute.id} />
        <CompactReference
          label="Service ID"
          value={detailRoute.targetServiceId}
        />
        <CompactReference
          label="Environment ID"
          value={detailRoute.environmentId}
        />
      </div>
    </div>
  );
}

export function ExposurePill({ exposure }: { exposure: Route["exposure"] }) {
  if (exposure === "public")
    return (
      <span className="inline-flex items-center gap-1.5 rounded-full bg-success/10 px-2 py-0.5 text-xs text-success">
        <span className="size-1.5 rounded-full bg-current" /> public
      </span>
    );
  if (exposure === "internal")
    return (
      <span className="inline-flex items-center gap-1.5 rounded-full bg-warning/10 px-2 py-0.5 text-xs text-warning">
        <span className="size-1.5 rounded-full bg-current" /> internal
      </span>
    );
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full bg-muted px-2 py-0.5 text-xs text-muted-foreground">
      <span className="size-1.5 rounded-full bg-current" /> loopback
    </span>
  );
}
