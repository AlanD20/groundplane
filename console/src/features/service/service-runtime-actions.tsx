"use client";

import { HelpHint, ResourcePanel } from "@/components/common/resource-panel";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { DNSProtectedRemoval } from "@/features/platform-component/dns-protected-removal";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { useStore } from "@/lib/store";
import type { Environment, Service } from "@/lib/types";
import { Ban, CirclePlay, CircleStop, Trash2 } from "lucide-react";
import { currentServiceObservation, replicaTotal } from "./service-observation";

export type ServiceOperation = "start" | "stop" | "destroy" | "remove";

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
      <StatusBadge
        status={observation.state}
        className={compact ? "text-xs" : ""}
        label={
          observation.state === "running"
            ? "Running · no healthcheck"
            : observation.state.charAt(0).toUpperCase() +
              observation.state.slice(1)
        }
      />
      {observation.state !== "unavailable" && (
        <span className="text-xs text-muted-foreground">
          {replicaTotal(observation.replicas)}/{observation.expectedReplicas}{" "}
          observed
        </span>
      )}
      {service.runtimeIntent !== "running" && (
        <span className="text-xs text-muted-foreground">
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
            Remove containers keeps the Service definition and persistent
            Volumes. Delete Service removes its containers and definition. Files
            stored only inside removed containers are lost.
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
            <Ban className="size-3.5" /> Remove containers
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
    ? `Delete Service · ${service.name}`
    : operation === "destroy"
      ? `Remove containers · ${service.name}`
      : `${operation[0].toUpperCase()}${operation.slice(1)} ${service.name}`;
  const description = removing
    ? "Remove the containers, then delete this Service from the Environment and Blueprint. Persistent Volumes and Release history are kept. Files stored only inside the containers are lost."
    : operation === "destroy"
      ? "Remove the containers but keep this Service, its configuration and persistent Volumes. You can recreate the containers later. Files stored only inside the containers are lost."
      : `${operation === "start" ? "Run" : "Stop"} the service through durable Controller-owned runtime intent.`;
  const steps = removing
    ? [
        {
          label: "Validate desired-state references",
          state: "pending" as const,
        },
        {
          label: "Remove Service containers",
          state: "pending" as const,
        },
        {
          label: "Delete the Service definition after cleanup",
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
    <DNSProtectedRemoval
      open={removing}
      onOpenChange={onOpenChange}
      serviceId={service.id}
    >
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
            ? "Delete Service"
            : operation === "destroy"
              ? "Remove containers"
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
    </DNSProtectedRemoval>
  );
}

export function RemoveDesiredServiceButton({
  onClick,
}: {
  onClick: () => void;
}) {
  return (
    <Button variant="destructive" onClick={onClick}>
      <Trash2 className="size-4" /> Delete Service
    </Button>
  );
}
