import { AnimatedSection } from "@/components/common/animated-section";
import { RouteFormDialog } from "./route-form";
import { TaskRunAfterSelector } from "@/features/task/task-run-after-selector";
import { EmptyState } from "@/components/common/empty-state";
import { CreationButton } from "@/components/common/creation-button";
import { InlineEditorRegion } from "@/components/common/inline-editor-region";
import { RouteAccessCheck } from "./edge-check-panel";
import { workspaceSectionClassName } from "@/components/common/workspace-section";
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
import {
  ChevronRight,
  Pencil,
  Plus,
  Router as RouterIcon,
  Trash2,
} from "lucide-react";
import { useRef, useState } from "react";

// ---- Routes ----

export function RoutesCard({ env }: { env: Environment }) {
  const store = useStore();
  const params = useRequiredParams("tenant");
  const [open, setOpen] = useState(false);
  const [detailTarget, setDetailTarget] = useState<Route | null>(null);
  const [detailRoute, setDetailRoute] = useState<Route>();
  const [detailError, setDetailError] = useState<string>();
  const detailRequest = useRef(0);
  const [editing, setEditing] = useState<Route | null>(null);
  const [removing, setRemoving] = useState<Route | null>(null);
  const [exposure, setExposure] = useState<Route["exposure"]>("internal");
  const [editAfter, setEditAfter] = useState<string[]>([]);
  const [editAfterLoading, setEditAfterLoading] = useState(false);
  const [editSubmitting, setEditSubmitting] = useState(false);
  const [editError, setEditError] = useState<string>();

  const saveExposure = async () => {
    if (!editing) return;
    setEditSubmitting(true);
    setEditError(undefined);
    try {
      await store.updateRoute(env.id, editing.id, {
        exposure,
        after: editAfter,
      });
      setEditing(null);
    } catch (error) {
      setEditError(
        error instanceof Error ? error.message : "Unable to edit Route",
      );
    } finally {
      setEditSubmitting(false);
    }
  };
  const details = (
    <ResourceForm
      editing={false}
      open={detailTarget !== null}
      onOpenChange={(next) => {
        if (!next) {
          detailRequest.current++;
          setDetailTarget(null);
        }
      }}
    >
      <DialogHeader>
        <DialogTitle>
          Route details ·{" "}
          {detailTarget
            ? `${detailTarget.host || "internal"}${detailTarget.path}`
            : ""}
        </DialogTitle>
      </DialogHeader>
      {!detailRoute && !detailError ? (
        <p role="status" className="text-sm text-muted-foreground">
          Loading Route details...
        </p>
      ) : null}
      {detailError ? (
        <p role="alert" className="text-sm text-destructive">
          {detailError}
        </p>
      ) : null}
      {detailRoute ? (
        <div>
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
                open={detailTarget !== null}
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
      ) : null}
      <DialogFooter>
        <Button
          variant="outline"
          onClick={() => {
            detailRequest.current++;
            setDetailTarget(null);
          }}
        >
          Close
        </Button>
      </DialogFooter>
    </ResourceForm>
  );
  const editor = (
    <ResourceForm
      open={!!editing}
      onOpenChange={(next) => {
        if (!next) {
          setEditing(null);
          setEditError(undefined);
        }
      }}
    >
      <DialogHeader>
        <DialogTitle>
          Edit route ·{" "}
          {editing ? `${editing.host || "internal"}${editing.path}` : ""}
        </DialogTitle>
      </DialogHeader>
      <div className="flex max-w-lg flex-col gap-4">
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="route-edit-exposure">Exposure</Label>
          <Select
            id="route-edit-exposure"
            value={exposure}
            onValueChange={(value) => setExposure(value as Route["exposure"])}
            options={[
              {
                value: "public",
                label: "public — needs ingress component",
              },
              { value: "internal", label: "internal — no host port" },
            ]}
          />
        </div>

        {editError ? (
          <p role="alert" className="text-sm text-destructive">
            {editError}
          </p>
        ) : null}
      </div>
      <TaskRunAfterSelector
        value={editAfter}
        onChange={setEditAfter}
        onBusyChange={setEditAfterLoading}
      />
      <DialogFooter>
        <Button variant="outline" onClick={() => setEditing(null)}>
          Cancel
        </Button>
        <Button
          data-action-id="route.edit"
          disabled={editSubmitting || editAfterLoading}
          onClick={() => void saveExposure()}
        >
          {editSubmitting ? "Saving..." : "Save exposure"}
        </Button>
      </DialogFooter>
    </ResourceForm>
  );
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
          onClick={() => setOpen(true)}
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
        {env.routes.map((r) => (
          <InlineEditorRegion key={r.id} editing={false}>
            <div className={workspaceSectionClassName()}>
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div className="flex min-w-0 items-center gap-3">
                  <ExposurePill exposure={r.exposure} />
                  <span className="truncate font-mono text-sm">
                    {r.host || "internal"}
                    {r.path}
                  </span>
                </div>
                <div className="flex min-w-0 items-center gap-1">
                  <span className="truncate font-mono text-xs text-muted-foreground">
                    →{" "}
                    {env.services.find(
                      (service) => service.id === r.targetServiceId,
                    )?.name ?? r.targetServiceId}
                    :{r.targetPort}
                  </span>
                  <Button
                    variant="ghost"
                    size="icon-xs"
                    data-action-id="route.show"
                    aria-label={`Show Route ${r.host || "internal"}${r.path}`}
                    title="Toggle route details"
                    aria-expanded={detailTarget?.id === r.id}
                    onClick={() => {
                      const request = ++detailRequest.current;
                      if (detailTarget?.id === r.id) {
                        setDetailTarget(null);
                        return;
                      }
                      setDetailTarget(r);
                      setDetailRoute(undefined);
                      setDetailError(undefined);
                      void store
                        .getRoute(r.id)
                        .then((route) => {
                          if (request === detailRequest.current)
                            setDetailRoute(route);
                        })
                        .catch((error: unknown) => {
                          if (request !== detailRequest.current) return;
                          setDetailError(
                            error instanceof Error
                              ? error.message
                              : "Unable to load Route details",
                          );
                        });
                    }}
                  >
                    <ChevronRight
                      className={`size-3.5 transition-transform ${detailTarget?.id === r.id ? "rotate-90" : ""}`}
                    />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon-xs"
                    title="Edit route exposure"
                    onClick={() => {
                      detailRequest.current++;
                      setDetailTarget(null);
                      setEditAfter([]);
                      setExposure(r.exposure);
                      setEditError(undefined);
                      setEditing(r);
                    }}
                  >
                    <Pencil className="size-3.5" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon-xs"
                    className="text-muted-foreground hover:text-destructive"
                    title="Remove route"
                    onClick={() => setRemoving(r)}
                  >
                    <Trash2 className="size-3.5" />
                  </Button>
                </div>
              </div>
              <AnimatedSection open={detailTarget?.id === r.id}>
                {detailTarget?.id === r.id && details}
              </AnimatedSection>
              <AnimatedSection open={editing?.id === r.id}>
                {editing?.id === r.id && editor}
              </AnimatedSection>
            </div>
          </InlineEditorRegion>
        ))}
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
