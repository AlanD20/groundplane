"use client";

import { useEffect, useState } from "react";
import { useRequiredParams } from "@/lib/router";
import { ArrowUpCircle, History } from "lucide-react";
import { useStore } from "@/lib/store";
import { Button } from "@/components/ui/button";
import { Select } from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import type { Environment, Service, TaskStep } from "@/lib/types";

// Deploy / Rollback live in their own component so opening a dialog only
// re-renders this small subtree, not the whole environment page.
export function DeployControls({
  env,
  disabled,
}: {
  env: Environment;
  disabled?: boolean;
}) {
  const [deployOpen, setDeployOpen] = useState(false);
  const [rollbackOpen, setRollbackOpen] = useState(false);
  return (
    <>
      <Button
        variant="outline"
        disabled={disabled}
        onClick={() => setRollbackOpen(true)}
      >
        <History className="size-4" /> Rollback
      </Button>
      <Button disabled={disabled} onClick={() => setDeployOpen(true)}>
        <ArrowUpCircle className="size-4" /> Deploy
      </Button>
      <DeployDialog env={env} open={deployOpen} onOpenChange={setDeployOpen} />
      <RollbackDialog
        env={env}
        open={rollbackOpen}
        onOpenChange={setRollbackOpen}
      />
    </>
  );
}

// ---- Deploy: per-service, tag + strategy chosen at deploy time ----

export function DeployDialog({
  env,
  open,
  onOpenChange,
}: {
  env: Environment;
  open: boolean;
  onOpenChange: (v: boolean) => void;
}) {
  const store = useStore();
  const params = useRequiredParams("tenant");
  const defaultSvc =
    env.services.find((s) => s.strategy === "blue-green") ?? env.services[0];
  const [service, setService] = useState(defaultSvc?.name ?? "");
  const svc = env.services.find((s) => s.name === service);
  // Default = the service's CURRENT TAG only (never the image name): redeploy
  // is the common case (spec / env / secret edits need re-application without
  // a new image). Re-sync on open so a freshly edited image is always the
  // default.
  const [tag, setTag] = useState(imageTag(defaultSvc?.image ?? ""));
  const [strategy, setStrategy] = useState<Service["strategy"]>(
    defaultSvc?.strategy ?? "recreate",
  );

  useEffect(() => {
    if (open && svc) {
      setTag(imageTag(svc.image));
      setStrategy(svc.strategy);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  if (!svc) return null;
  const steps = deploySteps(env, svc.name, strategy);

  return (
    <TaskRunnerDialog
      open={open}
      onOpenChange={onOpenChange}
      variant="drawer"
      title={`Deploy ${svc.name} · ${env.name}`}
      description="A deployment targets ONE service: pick the immutable image tag and the deploy strategy for this release. The tag defaults to the service's current tag — redeploy re-applies spec, env, and secret changes without a new image. The Controller sequences it; the Agent applies each step and only switches traffic after the healthcheck passes."
      type="deploy"
      target={env.id}
      workspace={params.tenant}
      startLabel="Deploy"
      review={
        <div className="flex flex-col gap-2">
          <div className="grid grid-cols-3 gap-3">
            <div className="flex flex-col gap-1">
              <Label htmlFor="dep-service">Service</Label>
              <Select
                id="dep-service"
                value={service}
                onValueChange={(v) => {
                  const next = env.services.find((s) => s.name === v);
                  setService(v);
                  if (next) setTag(imageTag(next.image)); // current tag of the newly selected service
                  setStrategy(next?.strategy ?? "recreate");
                }}
                options={env.services.map((s) => ({
                  value: s.name,
                  label: s.name,
                }))}
              />
            </div>
            <div className="flex flex-col gap-1">
              <Label htmlFor="dep-tag">Image tag</Label>
              <Input
                id="dep-tag"
                value={tag}
                onChange={(e) => setTag(e.target.value)}
                placeholder="sha-…"
              />
              <p className="text-xs text-muted-foreground">
                tag only — the image name ({imageName(svc.image)}) is taken from
                the service. Defaults to the current tag; redeploy applies spec
                / env / secret changes without a new image.
              </p>
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
                  { value: "rolling", label: "rolling (deferred)" },
                ]}
              />
            </div>
          </div>
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2 font-mono text-xs">
            <span className="w-24 text-muted-foreground">Image</span>
            <span className="flex-1 truncate text-right text-muted-foreground">
              {svc.image}
            </span>
            <span className="mx-2 text-muted-foreground">→</span>
            <span className="flex-1 truncate text-foreground">
              {imageName(svc.image)}:{tag}
            </span>
          </div>
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2 font-mono text-xs">
            <span className="w-24 text-muted-foreground">Strategy</span>
            <span className="flex-1 text-right text-muted-foreground">
              declared: {svc.strategy}
            </span>
            <span className="mx-2 text-muted-foreground">→</span>
            <span className="flex-1 text-foreground">
              {strategy}
              {strategy === "rolling" ? " (declared-deferred)" : ""}
            </span>
          </div>
          <p className="text-xs text-muted-foreground">
            A public Route is served only after the required ingress components
            are enabled through the future live Component surface. Creating a
            Route never enables them.
          </p>
        </div>
      }
      steps={steps}
      onDispatch={() => store.commitDeploy(env.id, svc.name, tag, strategy)}
    />
  );
}

// ---- Rollback: per-service, previous tag tracked and pre-selected ----

export function RollbackDialog({
  env,
  open,
  onOpenChange,
}: {
  env: Environment;
  open: boolean;
  onOpenChange: (v: boolean) => void;
}) {
  const store = useStore();
  const params = useRequiredParams("tenant");
  const lastService = env.deploys[0]?.service ?? env.services[0]?.name ?? "";
  const [service, setService] = useState(lastService);
  const svc = env.services.find((s) => s.name === service);
  const history = env.deploys.filter((d) => d.service === service);
  // Rollback target = the most recent SUCCESSFUL deploy whose tag differs
  // from the current one (failed/timed-out records are never selectable,
  // and redeploying the current tag never advances the rollback point).
  const currentTag = history.find((d) => d.status === "active")?.tag;
  const previousTag =
    history.find((d) => d.status === "superseded" && d.tag !== currentTag)
      ?.tag ?? "";
  const [tag, setTag] = useState(previousTag);

  if (!svc) return null;

  return (
    <TaskRunnerDialog
      open={open}
      onOpenChange={onOpenChange}
      variant="drawer"
      title={`Roll back ${svc.name} · ${env.name}`}
      description="Rollback targets the SAME service and tracks its previous tag from deploy history — pre-selected below (editable for an explicit tag). It is a traffic switch, never a cold start, and never reverses migrations."
      type="rollback"
      target={env.id}
      workspace={params.tenant}
      destructive
      confirmText={env.name}
      startLabel="Roll back"
      review={
        <div className="flex flex-col gap-2">
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1">
              <Label>Service</Label>
              <Select
                value={service}
                onValueChange={(v) => {
                  setService(v);
                  const h = env.deploys.filter((d) => d.service === v);
                  const cur = h.find((d) => d.status === "active")?.tag;
                  setTag(
                    h.find((d) => d.status === "superseded" && d.tag !== cur)
                      ?.tag ?? "",
                  );
                }}
                options={env.services.map((s) => ({
                  value: s.name,
                  label: s.name,
                }))}
              />
            </div>
            <div className="flex flex-col gap-1">
              <Label htmlFor="rb-tag">Previous tag (tracked)</Label>
              <Input
                id="rb-tag"
                value={tag}
                onChange={(e) => setTag(e.target.value)}
                placeholder="sha-…"
              />
            </div>
          </div>
          <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface px-3 py-2 text-xs text-muted-foreground">
            <span>
              history for{" "}
              <span className="font-mono text-foreground">{service}</span>:{" "}
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
      steps={[
        { label: "Run pre-rollback hooks", state: "pending" },
        { label: "Start previous image in inactive slot", state: "pending" },
        { label: "Wait for healthcheck", state: "pending" },
        { label: "Reload router (traffic switch)", state: "pending" },
        { label: "Run post-rollback hooks", state: "pending" },
      ]}
      onDispatch={() => store.commitRollback(env.id, svc.name, tag)}
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

export function deploySteps(
  env: Environment,
  serviceName: string,
  strategy: Service["strategy"],
): TaskStep[] {
  const serviceId = env.services.find(
    (service) => service.name === serviceName,
  )?.id;
  const hasPublic = env.routes.some(
    (route) =>
      route.exposure === "public" && route.targetServiceId === serviceId,
  );
  if (strategy === "blue-green")
    return [
      { label: "Run pre-deploy hooks", state: "pending" },
      { label: "Start inactive slot", state: "pending" },
      { label: "Wait for healthcheck", state: "pending" },
      ...(hasPublic
        ? [
            { label: "Render + validate Caddyfile", state: "pending" as const },
            {
              label: "Reload router (traffic switch)",
              state: "pending" as const,
            },
          ]
        : []),
      { label: "Recreate workers (singleton)", state: "pending" },
      { label: "Run post-deploy hooks", state: "pending" },
    ];
  if (strategy === "rolling")
    return [
      { label: "Run pre-deploy hooks", state: "pending" },
      { label: "Roll out replicas one by one", state: "pending" },
      { label: "Wait for healthcheck on each replica", state: "pending" },
      { label: "Run post-deploy hooks", state: "pending" },
    ];
  return [
    { label: "Run pre-deploy hooks", state: "pending" },
    { label: "Stop current container", state: "pending" },
    { label: "Start new container", state: "pending" },
    { label: "Wait for healthcheck", state: "pending" },
    { label: "Run post-deploy hooks", state: "pending" },
  ];
}
