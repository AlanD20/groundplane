"use client";

import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { Button } from "@/components/ui/button";
import {
  ResourceForm,
  ResourceFormHeader,
  ResourceFormTitle,
} from "@/components/common/resource-form";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { ImagePicker } from "@/features/image-delivery/image-picker";
import { deploymentReadinessError } from "@/features/service/deployment-readiness";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import type { Environment, Service } from "@/lib/types";
import { useDraftField } from "@/lib/use-draft-field";
import { ArrowUpCircle, History } from "lucide-react";
import { useState } from "react";

export function DeployControls({
  env,
  disabled,
  onDeploy,
  onRollback,
}: {
  env: Environment;
  disabled?: boolean;
  onDeploy: () => void;
  onRollback: () => void;
}) {
  return (
    <>
      <Button
        variant="outline"
        disabled={disabled || env.services.length === 0}
        onClick={onRollback}
      >
        <History className="size-4" /> Rollback
      </Button>
      <Button
        disabled={disabled || env.services.length === 0}
        onClick={onDeploy}
      >
        <ArrowUpCircle className="size-4" /> Deploy
      </Button>
    </>
  );
}

function MissingServiceDialog({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  return (
    <ResourceForm open={open} onOpenChange={onOpenChange} editing={false}>
      <ResourceFormHeader>
        <ResourceFormTitle>Service no longer available</ResourceFormTitle>
      </ResourceFormHeader>
      <p role="alert">
        The selected Service was removed. Close and reopen this action to select
        a current Service.
      </p>
      <Button onClick={() => onOpenChange(false)}>Close</Button>
    </ResourceForm>
  );
}

// ---- Deploy: per-service, tag + strategy chosen at deploy time ----

export function DeployDialog({
  env,
  open,
  onOpenChange,
  serviceId,
}: {
  env: Environment;
  open: boolean;
  onOpenChange: (v: boolean) => void;
  serviceId?: string;
}) {
  const store = useStore();
  const params = useRequiredParams("tenant");
  const defaultSvc = serviceId
    ? env.services.find((s) => s.id === serviceId)
    : (env.services.find((s) => s.strategy === "blue-green") ??
      env.services[0]);
  const [service, setService] = useState(defaultSvc?.id ?? "");
  const svc = env.services.find((s) => s.id === service);
  const [image, setImage] = useDraftField(svc?.image ?? "", service);
  const [strategy, setStrategy] = useDraftField<Service["strategy"]>(
    svc?.strategy ?? "recreate",
    service,
  );

  if (!svc)
    return <MissingServiceDialog open={open} onOpenChange={onOpenChange} />;
  const readinessError = deploymentReadinessError(svc, strategy);

  return (
    <TaskRunnerDialog
      open={open}
      onOpenChange={onOpenChange}
      variant="inline"
      title={`Deploy ${svc.name} · ${env.name}`}
      description="Use an image already on this host."
      type="deploy"
      target={env.id}
      workspace={params.tenant}
      startLabel="Deploy"
      review={
        <div className="flex flex-col gap-2">
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1">
              <Label htmlFor="dep-service">Service</Label>
              <Select
                searchable
                id="dep-service"
                value={service}
                onValueChange={setService}
                options={env.services.map((s) => ({
                  value: s.id,
                  label: s.name,
                }))}
              />
            </div>
            <div className="flex flex-col gap-1">
              <Label htmlFor="dep-strategy">Strategy</Label>
              <Select
                id="dep-strategy"
                value={strategy}
                onValueChange={(v) => setStrategy(v as Service["strategy"])}
                options={[
                  { value: "blue-green", label: "blue-green" },
                  { value: "recreate", label: "recreate" },
                  { value: "rolling", label: "rolling" },
                ]}
              />
            </div>
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="dep-image">Image for this Release</Label>
            <ImagePicker id="dep-image" value={image} onChange={setImage} />
          </div>
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2 font-mono text-xs">
            <span className="w-24 text-muted-foreground">Image</span>
            <span className="flex-1 truncate text-right text-muted-foreground">
              {svc.image}
            </span>
            <span className="mx-2 text-muted-foreground">→</span>
            <span className="flex-1 truncate text-foreground">{image}</span>
          </div>
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2 font-mono text-xs">
            <span className="w-24 text-muted-foreground">Strategy</span>
            <span className="flex-1 text-right text-muted-foreground">
              declared: {svc.strategy}
            </span>
            <span className="mx-2 text-muted-foreground">→</span>
            <span className="flex-1 text-foreground">{strategy}</span>
          </div>
          {readinessError && (
            <p role="alert" className="text-sm text-destructive">
              {readinessError}
            </p>
          )}
          {strategy === "rolling" && !readinessError && (
            <p className="text-xs text-muted-foreground">
              {svc.replicas} replicas, replaced one at a time. Each replacement
              has {svc.rollingReplicaTimeoutSeconds || 300} seconds. Old and new
              versions must support shared data.
            </p>
          )}
        </div>
      }
      startDisabled={!image.trim() || !!readinessError}
      onDispatch={() =>
        store.commitDeploy(
          env.id,
          svc.name,
          image.trim(),
          strategy,
          "switch_back",
        )
      }
      onSettled={async () => {
        await Promise.all([
          store.refreshEnvironmentReleases(env.id),
          store.refreshEnvironmentServices(env.id),
        ]);
      }}
    />
  );
}

// ---- Rollback: per-service, previous tag tracked and pre-selected ----

export function RollbackDialog({
  env,
  open,
  onOpenChange,
  serviceId,
}: {
  env: Environment;
  open: boolean;
  onOpenChange: (v: boolean) => void;
  serviceId?: string;
}) {
  const store = useStore();
  const params = useRequiredParams("tenant");
  const lastService = serviceId
    ? env.services.find((s) => s.id === serviceId)
    : (env.services.find((s) => s.name === env.deploys[0]?.service) ??
      env.services[0]);
  const [service, setService] = useState(lastService?.id ?? "");
  const svc = env.services.find((s) => s.id === service);
  const history = env.deploys.filter((d) => d.service === svc?.name);
  // Rollback target = the most recent SUCCESSFUL deploy whose tag differs
  // from the current one (failed/timed-out records are never selectable,
  // and redeploying the current tag never advances the rollback point).
  const currentTag = history.find((d) => d.status === "active")?.tag;
  const previousTag =
    history.find((d) => d.status === "superseded" && d.tag !== currentTag)
      ?.tag ?? "";
  const [tag, setTag] = useDraftField(previousTag, service);

  if (!svc)
    return <MissingServiceDialog open={open} onOpenChange={onOpenChange} />;

  return (
    <TaskRunnerDialog
      open={open}
      onOpenChange={onOpenChange}
      variant="inline"
      title={`Roll back ${svc.name} · ${env.name}`}
      description="Deploy a previous image using the current configuration, replicas and release strategy. Database migrations are not reversed."
      type="rollback"
      target={env.id}
      workspace={params.tenant}
      destructive
      startLabel="Roll back"
      review={
        <div className="flex flex-col gap-2">
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1">
              <Label>Service</Label>
              <Select
                searchable
                value={service}
                onValueChange={setService}
                options={env.services.map((s) => ({
                  value: s.id,
                  label: s.name,
                }))}
              />
            </div>
            <div className="flex flex-col gap-1">
              <Label htmlFor="rb-tag">Previously deployed image tag</Label>
              <Input
                id="rb-tag"
                value={tag}
                onChange={(e) => setTag(e.target.value)}
                placeholder="v1.2.2"
              />
            </div>
          </div>
          <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface px-3 py-2 text-xs text-muted-foreground">
            <span>
              history for{" "}
              <span className="font-mono text-foreground">{svc.name}</span>:{" "}
              {history.length === 0 ? (
                <span className="text-muted-foreground">
                  no prior deploys — nothing to roll back to
                </span>
              ) : (
                history.map((d) => (
                  <span key={d.id} className="mr-2 font-mono">
                    {d.tag} ({d.when}) {d.status === "active" ? "· active" : ""}
                  </span>
                ))
              )}
            </span>
            {!previousTag && history.length > 0 && (
              <span className="text-warning">
                no rollback target — no successful deploy with a tag different
                from the current one
              </span>
            )}
          </div>
          <p className="text-xs text-muted-foreground">
            Rollback changes the application image only; it does not reverse
            database migrations.
          </p>
        </div>
      }
      onDispatch={() => store.commitRollback(env.id, svc.name, tag)}
      onSettled={async () => {
        await Promise.all([
          store.refreshEnvironmentReleases(env.id),
          store.refreshEnvironmentServices(env.id),
        ]);
      }}
    />
  );
}

// The last ':' splits an image name and tag unless the remainder looks like a
// registry path. Untagged images use "latest".
export function imageName(image: string): string {
  const i = image.lastIndexOf(":");
  return i > -1 && !image.slice(i + 1).includes("/")
    ? image.slice(0, i)
    : image;
}

export function imageTag(image: string): string {
  const i = image.lastIndexOf(":");
  return i > -1 && !image.slice(i + 1).includes("/")
    ? image.slice(i + 1)
    : "latest";
}
