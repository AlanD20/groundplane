"use client";

import { ImageReference } from "@/components/common/image-reference";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useStore } from "@/lib/store";
import type { Environment, Service } from "@/lib/types";
import { Ban, CirclePlay, CircleStop, Trash2 } from "lucide-react";
import {
  currentServiceObservation,
  replicaTotal,
  type ServiceObservationState,
} from "./service-observation";

export type ServiceOperation = "start" | "stop" | "destroy" | "remove";

function observationBadgeVariant(
  state: ServiceObservationState,
): "success" | "warning" | "danger" | "muted" | "primary" {
  if (state === "healthy") return "success";
  if (state === "failed") return "danger";
  if (state === "degraded" || state === "starting") return "warning";
  if (state === "running") return "primary";
  return "muted";
}

export function ServiceStateBadges({
  service,
  compact = false,
  now = Date.now(),
}: {
  service: Service;
  compact?: boolean;
  now?: number;
}) {
  const observation = currentServiceObservation(service.observation, now);
  return (
    <span className="flex flex-wrap items-center gap-2">
      <Badge
        variant={observationBadgeVariant(observation.state)}
        className={compact ? "text-[10px]" : ""}
      >
        {observation.state === "running"
          ? "Running · no healthcheck"
          : observation.state.charAt(0).toUpperCase() +
            observation.state.slice(1)}
      </Badge>
      {observation.state !== "unavailable" && (
        <span className="text-[11px] text-muted-foreground">
          {replicaTotal(observation.replicas)}/{observation.expectedReplicas}{" "}
          observed
        </span>
      )}
      {service.runtimeIntent !== "running" && (
        <span className="text-[11px] text-muted-foreground">
          Desired: {service.runtimeIntent}
        </span>
      )}
    </span>
  );
}

export function ServiceObservationDetails({
  service,
  now = Date.now(),
}: {
  service: Service;
  now?: number;
}) {
  const observation = currentServiceObservation(service.observation, now);
  if (observation.state === "unavailable")
    return (
      <section className="rounded-lg border border-border p-4">
        <h3 className="text-sm font-medium">
          {service.releaseLedger?.length === 0
            ? "Not deployed yet"
            : "Runtime report unavailable"}
        </h3>
        <p className="mt-2 text-xs text-muted-foreground">
          {service.releaseLedger?.length === 0
            ? "This Service has configuration but no Release. Fetch its image in Host images, then choose Deploy."
            : "GP has no current container report. Configuration and previous Tasks cannot confirm whether this workload is running."}
        </p>
      </section>
    );
  const release = service.releaseLedger?.find(
    (release) => release.id === observation.servingReleaseId,
  );
  const counts: [string, number][] = [
    ["Running without healthcheck", observation.replicas.running],
    ["Healthy", observation.replicas.healthy],
    ["Healthcheck starting", observation.replicas.starting],
    ["Unhealthy", observation.replicas.unhealthy],
    ["Transitional", observation.replicas.transitional],
    ["Stopped", observation.replicas.stopped],
    ["Failed", observation.replicas.failed],
  ];
  return (
    <section
      className="space-y-4 rounded-lg border border-border p-4"
      aria-label="Serving runtime"
    >
      <div className="flex items-start justify-between gap-4">
        <div>
          <h3 className="text-sm font-medium">Serving runtime</h3>
          <p className="mt-1 text-xs text-muted-foreground">
            Reported by the Agent, separately from desired configuration.
          </p>
        </div>
        <Badge variant={observationBadgeVariant(observation.state)}>
          {observation.state === "running"
            ? "Running · no healthcheck"
            : observation.state}
        </Badge>
      </div>
      <div className="grid gap-4 sm:grid-cols-2">
        <div>
          <p className="text-2xl font-medium tabular-nums">
            {replicaTotal(observation.replicas)}{" "}
            <span className="text-sm text-muted-foreground">
              / {observation.expectedReplicas} replicas
            </span>
          </p>
          <p className="mt-1 text-xs text-muted-foreground">
            {counts
              .filter(([, value]) => value > 0)
              .map(([label, value]) => `${value} ${label.toLowerCase()}`)
              .join(", ") || "No containers observed"}
          </p>
        </div>
        <div>
          <p className="mb-1 text-[11px] text-muted-foreground">
            Serving image
          </p>
          {release ? (
            <ImageReference
              value={release.tag || release.digest || release.id}
            />
          ) : (
            <p className="text-xs text-muted-foreground">
              Release image not available in this response.
            </p>
          )}
        </div>
      </div>
      <details className="border-t border-border pt-3">
        <summary className="text-xs font-medium text-muted-foreground">
          Observation details
        </summary>
        <dl className="mt-3 grid gap-3 text-xs sm:grid-cols-2">
          <div>
            <dt className="text-muted-foreground">Reported at</dt>
            <dd>{new Date(observation.observedAt).toLocaleString()}</dd>
          </div>
          <div>
            <dt className="text-muted-foreground">Report expires</dt>
            <dd>{new Date(observation.expiresAt).toLocaleString()}</dd>
          </div>
          {observation.servingReleaseId && (
            <div className="sm:col-span-2">
              <dt className="text-muted-foreground">Serving Release ID</dt>
              <dd className="break-all font-mono">
                {observation.servingReleaseId}
              </dd>
            </div>
          )}
          {counts.map(([label, count]) => (
            <div key={label}>
              <dt className="text-muted-foreground">{label}</dt>
              <dd>{count}</dd>
            </div>
          ))}
        </dl>
        <p className="mt-3 text-xs text-muted-foreground">
          Container and healthcheck reports do not prove application or Route
          reachability.
        </p>
      </details>
    </section>
  );
}

export function ServiceRuntimeActions({
  service,
  onAction,
}: {
  service: Service;
  onAction: (action: ServiceOperation) => void;
}) {
  return (
    <div className="rounded-lg border border-border p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-col gap-1">
          <p className="text-sm font-medium">Runtime controls</p>
          <p className="text-xs text-muted-foreground">
            Desired runtime: {service.runtimeIntent}
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button
            size="sm"
            onClick={() => onAction("start")}
            disabled={service.runtimeIntent === "running"}
          >
            <CirclePlay className="size-3.5" /> Start
          </Button>
          <Button
            size="sm"
            variant="outline"
            onClick={() => onAction("stop")}
            disabled={service.runtimeIntent === "stopped"}
          >
            <CircleStop className="size-3.5" /> Stop
          </Button>
          <Button
            size="sm"
            variant="destructive"
            onClick={() => onAction("destroy")}
            disabled={service.runtimeIntent === "absent"}
          >
            <Ban className="size-3.5" /> Destroy runtime
          </Button>
        </div>
      </div>
      <p className="mt-2 text-xs text-muted-foreground">
        Start, Stop, and Destroy preserve desired state. Reconciliation follows
        this Controller-owned intent.
      </p>
    </div>
  );
}

export function ServiceOperationDialog({
  env,
  service,
  operation,
  workspace,
  onOpenChange,
  onRemoved,
}: {
  env: Environment;
  service: Service;
  operation: ServiceOperation | null;
  workspace: string;
  onOpenChange: (open: boolean) => void;
  onRemoved: () => void;
}) {
  const store = useStore();
  if (!operation) return null;

  const runtimeIntent =
    operation === "start"
      ? "running"
      : operation === "stop"
        ? "stopped"
        : "absent";
  const removing = operation === "remove";
  const title = removing
    ? `Remove desired service · ${service.name}`
    : `${operation[0].toUpperCase()}${operation.slice(1)} ${service.name}`;
  const description = removing
    ? "Delete the desired service and its Controller-owned runtime intent. This is not the same as destroying its runtime."
    : operation === "destroy"
      ? "Remove the service runtime while retaining its desired service record. A later Start recreates it."
      : `${operation === "start" ? "Run" : "Stop"} the service through durable Controller-owned runtime intent.`;
  const steps = removing
    ? [
        {
          label: "Validate desired-state references",
          state: "pending" as const,
        },
        {
          label: "Remove desired service and runtime-intent record",
          state: "pending" as const,
        },
        {
          label: "Reconcile the environment without the service",
          state: "pending" as const,
        },
      ]
    : [
        {
          label: `Persist runtime intent ${runtimeIntent}`,
          state: "pending" as const,
        },
        {
          label:
            operation === "start"
              ? "Reconcile the service runtime"
              : operation === "stop"
                ? "Stop service containers"
                : "Remove service containers",
          state: "pending" as const,
        },
        { label: "Record observed runtime state", state: "pending" as const },
      ];

  return (
    <TaskRunnerDialog
      open
      onOpenChange={onOpenChange}
      title={title}
      description={description}
      type={operation}
      target={service.id}
      workspace={workspace}
      destructive={operation === "destroy" || removing}
      confirmText={
        operation === "destroy" || removing ? service.name : undefined
      }
      startLabel={
        removing
          ? "Remove desired service"
          : operation === "destroy"
            ? "Destroy runtime"
            : `${operation[0].toUpperCase()}${operation.slice(1)} service`
      }
      steps={steps}
      {...(removing
        ? {
            onDispatch: () => store.deleteService(env.id, service.id),
            onCommit: onRemoved,
          }
        : {
            onDispatch: () =>
              store.runServiceRuntimeAction(env.id, service.id, operation),
          })}
    />
  );
}

export function RemoveDesiredServiceButton({
  onClick,
}: {
  onClick: () => void;
}) {
  return (
    <Button variant="destructive" onClick={onClick}>
      <Trash2 className="size-4" /> Remove desired service
    </Button>
  );
}
