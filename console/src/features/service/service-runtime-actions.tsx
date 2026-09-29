"use client";

import { HelpHint, ResourcePanel } from "@/components/common/resource-panel";
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

export function ServiceRuntimeActions({
  service,
  onAction,
}: {
  service: Service;
  onAction: (action: ServiceOperation) => void;
}) {
  return (
    <ResourcePanel
      title={
        <span className="flex items-center gap-2">
          Runtime{" "}
          <HelpHint label="About runtime controls">
            Start, Stop and Destroy runtime preserve configuration and durable
            data. Remove deletes the Service definition.
          </HelpHint>
        </span>
      }
      actions={
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
      }
    />
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
    ? `Remove Service · ${service.name}`
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
          ? "Remove Service"
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
      <Trash2 className="size-4" /> Remove Service
    </Button>
  );
}
