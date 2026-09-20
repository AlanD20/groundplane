"use client";

import { useState } from "react";
import { useRequiredParams } from "@/lib/router";
import {
  ChevronRight,
  Pencil,
  Plus,
  Router as RouterIcon,
  Trash2,
} from "lucide-react";
import { useStore } from "@/lib/store";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select } from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import {
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Drawer, DrawerContent } from "@/components/ui/drawer";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { DetailRow } from "@/features/environment/service-details-drawer";
import type { Environment, Route } from "@/lib/types";

// ---- Routes ----

export function RoutesCard({ env }: { env: Environment }) {
  const store = useStore();
  const params = useRequiredParams("tenant");
  const [open, setOpen] = useState(false);
  const [detailTarget, setDetailTarget] = useState<Route | null>(null);
  const [detailRoute, setDetailRoute] = useState<Route>();
  const [detailError, setDetailError] = useState<string>();
  const [editing, setEditing] = useState<Route | null>(null);
  const [removing, setRemoving] = useState<Route | null>(null);
  const [exposure, setExposure] = useState<Route["exposure"]>("internal");
  const [editSubmitting, setEditSubmitting] = useState(false);
  const [editError, setEditError] = useState<string>();

  const saveExposure = async () => {
    if (!editing) return;
    setEditSubmitting(true);
    setEditError(undefined);
    try {
      await store.updateRoute(env.id, editing.id, { exposure });
      setEditing(null);
    } catch (error) {
      setEditError(
        error instanceof Error ? error.message : "Unable to edit Route",
      );
    } finally {
      setEditSubmitting(false);
    }
  };
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <RouterIcon className="size-4 text-muted-foreground" /> Routes
        </CardTitle>
        <Button variant="outline" size="sm" onClick={() => setOpen(true)}>
          <Plus className="size-3.5" /> Route
        </Button>
      </CardHeader>
      <CardContent className="flex flex-col gap-1.5">
        {env.routes.map((r) => (
          <div
            key={r.id}
            className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2"
          >
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
                title="Show route details"
                onClick={() => {
                  setDetailTarget(r);
                  setDetailRoute(undefined);
                  setDetailError(undefined);
                  void store
                    .getRoute(r.id)
                    .then(setDetailRoute)
                    .catch((error: unknown) => {
                      setDetailError(
                        error instanceof Error
                          ? error.message
                          : "Unable to load Route details",
                      );
                    });
                }}
              >
                <ChevronRight className="size-3.5" />
              </Button>
              <Button
                variant="ghost"
                size="icon-xs"
                title="Edit route exposure"
                onClick={() => {
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
        ))}
        {env.routes.length === 0 && (
          <div className="text-xs text-muted-foreground">
            no routes — add one; public routes need an ingress component (Router
            tab) to be served
          </div>
        )}
      </CardContent>
      <RouteFormDialog env={env} open={open} onOpenChange={setOpen} />
      <Drawer
        open={detailTarget !== null}
        onOpenChange={(next) => !next && setDetailTarget(null)}
      >
        <DrawerContent>
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
            <div className="flex flex-col">
              <DetailRow label="ID" value={detailRoute.id} mono />
              <DetailRow
                label="Environment"
                value={detailRoute.environmentId}
                mono
              />
              <DetailRow
                label="Host"
                value={detailRoute.host || "hostless internal"}
                mono
              />
              <DetailRow label="Path" value={detailRoute.path} mono />
              <DetailRow label="Exposure" value={detailRoute.exposure} />
              <DetailRow
                label="Target Service"
                value={detailRoute.targetServiceId}
                mono
              />
              <DetailRow
                label="Target port"
                value={String(detailRoute.targetPort)}
                mono
              />
            </div>
          ) : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setDetailTarget(null)}>
              Close
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>
      <Drawer
        open={!!editing}
        onOpenChange={(next) => {
          if (!next) {
            setEditing(null);
            setEditError(undefined);
          }
        }}
      >
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>
              Edit route ·{" "}
              {editing ? `${editing.host || "internal"}${editing.path}` : ""}
            </DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="route-edit-exposure">Exposure</Label>
              <Select
                id="route-edit-exposure"
                value={exposure}
                onValueChange={(value) =>
                  setExposure(value as Route["exposure"])
                }
                options={[
                  {
                    value: "public",
                    label: "public — needs ingress component",
                  },
                  { value: "internal", label: "internal — no host port" },
                ]}
              />
            </div>
            <p className="text-xs text-muted-foreground">
              The route host, path, target service, and target port remain
              unchanged.
            </p>
            {editError ? (
              <p role="alert" className="text-sm text-destructive">
                {editError}
              </p>
            ) : null}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setEditing(null)}>
              Cancel
            </Button>
            <Button
              data-action-id="route.edit"
              disabled={editSubmitting}
              onClick={() => void saveExposure()}
            >
              {editSubmitting ? "Saving..." : "Save exposure"}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>
      <TaskRunnerDialog
        open={!!removing}
        onOpenChange={(next) => !next && setRemoving(null)}
        title={`Remove route · ${removing ? `${removing.host || "internal"}${removing.path}` : ""}`}
        description="Removes this route from the environment and reconciles the router configuration."
        type="destroy"
        target={removing?.id ?? env.id}
        workspace={params.tenant}
        destructive
        confirmText={
          removing ? `${removing.host || "internal"}${removing.path}` : ""
        }
        startLabel="Remove route"
        steps={[
          { label: "Validate the route record", state: "pending" },
          { label: "Remove the route", state: "pending" },
          { label: "Reconcile router configuration", state: "pending" },
        ]}
        onDispatch={async () => {
          if (!removing) throw new Error("No Route selected for removal");
          return store.removeRoute(env.id, removing.id);
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

export function routeHostError(
  host: string,
  exposure: Route["exposure"],
): string | null {
  if (!host)
    return exposure === "public"
      ? "Public Routes require a DNS hostname."
      : null;
  if (host.length > 253 || host.endsWith(".") || host !== host.toLowerCase()) {
    return "Use a lowercase ASCII DNS hostname without a trailing dot.";
  }
  if (/^\d+(?:\.\d+){3}$/.test(host))
    return "Use a DNS hostname, not an IP address.";
  const labels = host.split(".");
  if (
    labels.some(
      (label) => !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label),
    )
  ) {
    return "Each DNS label must use letters, numbers, or internal hyphens.";
  }
  return null;
}

export function routePathError(path: string): string | null {
  if (path.length > 2048) return "Route paths cannot exceed 2048 bytes.";
  if (!path.startsWith("/")) return "Route paths must start with /.";
  if (/[?#]/.test(path))
    return "Route paths cannot contain a query or fragment.";
  const firstWildcard = path.indexOf("*");
  if (
    firstWildcard >= 0 &&
    (firstWildcard !== path.length - 1 ||
      path.lastIndexOf("*") !== firstWildcard)
  ) {
    return "A Route path may use one wildcard only, at the end.";
  }
  if (/%(?![0-9A-Fa-f]{2})/u.test(path))
    return "Percent escapes must use exactly two hexadecimal digits.";
  if (/[^A-Za-z0-9/%\-._~:@!$&()+,;=*]/u.test(path)) {
    return "Route paths contain a character that cannot be rendered safely.";
  }
  return null;
}

export function RouteFormDialog({
  env,
  open,
  onOpenChange,
}: {
  env: Environment;
  open: boolean;
  onOpenChange: (v: boolean) => void;
}) {
  const store = useStore();
  const [host, setHost] = useState("");
  const [path, setPath] = useState("/");
  const [target, setTarget] = useState(env.services[0]?.id ?? "");
  const [targetPort, setTargetPort] = useState("");
  const [exposure, setExposure] = useState<"public" | "internal">("internal");
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string>();
  const normalizedHost = host.trim();
  const normalizedPath = path.trim();
  const hostValidationError = routeHostError(normalizedHost, exposure);
  const pathValidationError = routePathError(normalizedPath);
  const validTargetPort =
    /^\d+$/.test(targetPort) &&
    Number(targetPort) >= 1 &&
    Number(targetPort) <= 65535;

  const submit = async () => {
    setSubmitting(true);
    setSubmitError(undefined);
    try {
      await store.addRoute(env.id, {
        host: normalizedHost,
        path: normalizedPath,
        exposure,
        targetServiceId: target,
        targetPort: Number(targetPort),
      });
      onOpenChange(false);
      setHost("");
      setPath("/");
      setTargetPort("");
    } catch (error) {
      setSubmitError(
        error instanceof Error ? error.message : "Unable to create Route",
      );
    } finally {
      setSubmitting(false);
    }
  };
  return (
    <Drawer open={open} onOpenChange={onOpenChange}>
      <DrawerContent>
        <DialogHeader>
          <DialogTitle>Add route · {env.name}</DialogTitle>
        </DialogHeader>
        <div className="flex flex-col gap-4">
          <p className="text-xs text-muted-foreground">
            A Route sends traffic to a Service. Public Routes require separately
            managed ingress components to be served; creating a Route never
            enables them.
          </p>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="r-host">Host</Label>
            <Input
              id="r-host"
              value={host}
              onChange={(event) => setHost(event.target.value)}
              placeholder="app.example.com"
              aria-invalid={hostValidationError !== null}
              autoFocus
            />
            <p
              className={
                hostValidationError
                  ? "text-xs text-destructive"
                  : "text-xs text-muted-foreground"
              }
            >
              {hostValidationError ??
                "Required for public Routes; optional for hostless internal Routes."}
            </p>
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="r-path">Path</Label>
            <Input
              id="r-path"
              value={path}
              onChange={(event) => setPath(event.target.value)}
              placeholder="/api/*"
              aria-invalid={pathValidationError !== null}
            />
            <p
              className={
                pathValidationError
                  ? "text-xs text-destructive"
                  : "text-xs text-muted-foreground"
              }
            >
              {pathValidationError ??
                "Absolute path with one optional terminal wildcard."}
            </p>
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="r-target">Target service</Label>
            <Select
              id="r-target"
              value={target}
              onValueChange={setTarget}
              options={
                env.services.length > 0
                  ? env.services.map((service) => ({
                      value: service.id,
                      label: service.name,
                    }))
                  : [{ value: "", label: "— no services yet —" }]
              }
            />
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="r-target-port">Target port</Label>
            <Input
              id="r-target-port"
              inputMode="numeric"
              value={targetPort}
              onChange={(event) => setTargetPort(event.target.value)}
              placeholder="8080"
            />
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="r-exposure">Exposure</Label>
            <Select
              id="r-exposure"
              value={exposure}
              onValueChange={(v) => setExposure(v as "public" | "internal")}
              options={[
                { value: "public", label: "public — needs ingress component" },
                { value: "internal", label: "internal — no host port" },
              ]}
            />
          </div>
          {submitError ? (
            <p role="alert" className="text-sm text-destructive">
              {submitError}
            </p>
          ) : null}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
            data-action-id="route.create"
            disabled={
              hostValidationError !== null ||
              pathValidationError !== null ||
              !target ||
              !validTargetPort ||
              submitting
            }
            onClick={() => void submit()}
          >
            {submitting ? "Adding..." : "Add route"}
          </Button>
        </DialogFooter>
      </DrawerContent>
    </Drawer>
  );
}
