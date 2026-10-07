import { CopyButton } from "@/components/common/copy-button";
import {
  AdvancedDetails,
  SummaryItem,
  SummaryStrip,
} from "@/components/common/resource-panel";
import { TaskJournalMetadata } from "@/components/common/task-journal-metadata";
import { TaskLink } from "@/components/common/task-link";
import { useStore } from "@/lib/store";
import {
  resolveTaskOperationSurface,
  type TaskNavigationContext,
} from "@/lib/task-navigation";
import type { ActivityEntry } from "@/lib/types";
import { ArrowUpRight, CircleAlert, CircleCheck, Clock3 } from "lucide-react";
import { Link } from "react-router-dom";
import { TaskExecutionTerminal } from "./task-execution-terminal";

export function taskPresentation(
  task: ActivityEntry,
  store: TaskNavigationContext,
) {
  const projects = [...store.tenantProjects, ...store.backingProjects];
  const project = projects.find(
    (project) =>
      project.id === task.projectId ||
      project.id === task.target ||
      project.environments?.some((env) => env.id === task.environmentId),
  );
  const env = project?.environments?.find(
    (env) => env.id === task.environmentId || env.id === task.target,
  );
  const service = env?.services.find(
    (service) => service.id === task.target || service.name === task.target,
  );
  const attach = env?.attaches.find(
    (attach) => attach.id === task.target || attach.name === task.target,
  );
  const zone = env?.zones.find(
    (zone) => zone.id === task.target || zone.name === task.target,
  );
  const volume = env?.volumes.find(
    (volume) =>
      volume.id === task.target ||
      volume.slug === task.target ||
      volume.key === task.target,
  );
  const route = env?.routes.find((route) => route.id === task.target);
  const script = env?.scripts.find(
    (script) => script.id === task.target || script.slug === task.target,
  );
  const releaseGroup = env?.releaseGroups.find(
    (group) => group.id === task.target || group.name === task.target,
  );
  const tenant = store.tenants.find(
    (tenant) =>
      tenant.id === (project?.tenantId ?? task.tenantId) ||
      tenant.id === task.target,
  );
  const agent = store.platform.agents.find((agent) => agent.id === task.target);
  const runner = store.runners.find((runner) => runner.id === task.target);
  const image = task.target.startsWith("sha256:");
  const resource =
    task.targetName ??
    service?.name ??
    attach?.name ??
    zone?.name ??
    volume?.slug ??
    (route
      ? `${route.host}${route.path === "/" ? "" : route.path}`
      : undefined) ??
    script?.slug ??
    releaseGroup?.name ??
    (env?.id === task.target ? env.name : undefined) ??
    (project?.id === task.target ? project.name : undefined) ??
    (tenant?.id === task.target ? tenant.name : undefined) ??
    agent?.host ??
    (runner ? "GitHub Runner" : undefined) ??
    (image
      ? "Host image"
      : /^[a-z]+_[A-Z0-9]+$/.test(task.target)
        ? "Resource details unavailable"
        : task.target);
  const scope =
    [tenant?.name, project?.name, env?.name].filter(Boolean).join(" / ") ||
    (task.workspaceType === "platform" ? "Platform" : "Workspace unavailable");
  let destination = resolveTaskOperationSurface(task, store);
  if (service && env && project && tenant)
    destination = {
      href: `/t/${encodeURIComponent(tenant.slug)}/${encodeURIComponent(project.slug)}/${encodeURIComponent(env.name)}?view=overview&service=${encodeURIComponent(service.id)}`,
      label: "Open Service",
      fallback: false,
    };
  if (image)
    destination = {
      href: "/platform/host/images",
      label: "Open images",
      fallback: false,
    };
  const resourceKind =
    task.resourceKind ??
    (service
      ? "service"
      : attach
        ? "attach"
        : zone
          ? "zone"
          : volume
            ? "volume"
            : route
              ? "route"
              : script
                ? "script"
                : releaseGroup
                  ? "release_group"
                  : env?.id === task.target
                    ? "environment"
                    : project?.id === task.target
                      ? "project"
                      : tenant?.id === task.target
                        ? "tenant"
                        : agent
                          ? "agent"
                          : runner
                            ? "runner"
                            : image
                              ? "image"
                              : undefined);
  return {
    resource,
    resourceKind: resourceKindLabel(resourceKind),
    scope,
    destination,
    title: `${task.title.split(" · ")[0]} · ${resource}`,
  };
}

export function TaskOverview({
  task,
  onClose,
}: {
  task: ActivityEntry;
  onClose: () => void;
}) {
  const store = useStore();
  const view = taskPresentation(task, store);
  const steps = task.steps ?? [];
  const done = steps.filter((step) => step.state === "done").length;
  const duration = task.startedAt
    ? Math.max(
        0,
        Math.floor(
          ((task.finishedAt ? Date.parse(task.finishedAt) : Date.now()) -
            Date.parse(task.startedAt)) /
            1000,
        ),
      )
    : null;
  return (
    <>
      <SummaryStrip>
        <SummaryItem label="Plan progress">
          {steps.length
            ? `${done} of ${steps.length} completed`
            : "No plan captured"}
        </SummaryItem>
        <SummaryItem label={task.finishedAt ? "Execution time" : "Elapsed"}>
          {duration === null ? "Not started" : durationLabel(duration)}
        </SummaryItem>
        <SummaryItem label="Executor">
          {task.executor ? executorLabel(task.executor) : "Not recorded"}
        </SummaryItem>
        <SummaryItem label="Time limit">
          {task.timeoutSeconds
            ? durationLabel(task.timeoutSeconds)
            : "Not recorded"}
        </SummaryItem>
      </SummaryStrip>
      <section className="space-y-3 rounded-lg border border-border bg-card p-4">
        <div className="space-y-1">
          <p className="text-xs text-muted-foreground">
            {view.resourceKind ?? "Affected resource"}
          </p>
          <h3 className="break-words text-sm font-medium [overflow-wrap:anywhere]">
            {view.resource}
          </h3>
          <p className="break-words text-xs text-muted-foreground">
            {view.scope}
          </p>
        </div>
        {view.destination && (
          <Link
            className="inline-flex items-center gap-1 text-sm text-primary hover:underline focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
            to={view.destination.href}
            onClick={onClose}
          >
            {view.destination.label}
            <ArrowUpRight className="size-3.5" aria-hidden />
          </Link>
        )}
      </section>
      <dl className="grid grid-cols-1 gap-3 text-xs sm:grid-cols-2">
        <Info label="Requested" value={timestamp(task.createdAt)} />
        <Info
          label="Requested by"
          value={task.actor === "system" ? "Groundplane" : "Operator"}
        />
        <Info
          label="Started"
          value={task.startedAt ? timestamp(task.startedAt) : "Not started"}
        />
        <Info
          label={task.finishedAt ? "Finished" : "Last update"}
          value={timestamp(task.finishedAt ?? task.updatedAt)}
        />
      </dl>
      <TaskOutcome task={task} />
      {task.reconciliationRequired && (
        <p
          role="alert"
          className="rounded-lg border border-warning/30 bg-warning/10 p-3 text-sm"
        >
          Restoration is not proven. The resolver stays locked to this Task;
          retry it to apply the same sealed configuration. GP has not reported a
          successful restore.
        </p>
      )}
      {task.note && (
        <section className="space-y-1 rounded-lg border border-border p-3 text-sm">
          <h3 className="text-xs font-medium text-muted-foreground">
            Task note
          </h3>
          <p className="break-words [overflow-wrap:anywhere]">{task.note}</p>
        </section>
      )}
      <TaskExecutionTerminal task={task} />
      <AdvancedDetails title="Task identifiers & execution metadata">
        <dl className="mt-3 space-y-3 text-xs">
          {[
            ["Task ID", task.id],
            ["Target reference", task.target],
            ["Operation ID", task.operationId],
            ["Plan hash", task.planHash],
          ].map(
            ([label, value]) =>
              value && (
                <div key={label}>
                  <dt className="mb-1 text-muted-foreground">{label}</dt>
                  <dd className="flex min-w-0 items-start gap-2">
                    <code className="min-w-0 flex-1 break-all">{value}</code>
                    <CopyButton value={value} label={`Copy ${label}`} />
                  </dd>
                </div>
              ),
          )}
          {task.retryOf && (
            <div>
              <dt>Retry of</dt>
              <dd>
                <TaskLink taskId={task.retryOf} onClick={onClose} />
              </dd>
            </div>
          )}
        </dl>
        <div className="mt-4">
          <TaskJournalMetadata entry={task} />
          {task.taskState && (
            <p className="mt-2 text-xs text-muted-foreground">
              Execution state: {task.taskState}
            </p>
          )}
        </div>
      </AdvancedDetails>
    </>
  );
}

function timestamp(value?: string | null) {
  return value ? new Date(value).toLocaleString() : "Not recorded";
}

function durationLabel(seconds: number) {
  if (seconds < 1) return "<1s";
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  if (minutes < 60)
    return remainder ? `${minutes}m ${remainder}s` : `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  const minuteRemainder = minutes % 60;
  return minuteRemainder ? `${hours}h ${minuteRemainder}m` : `${hours}h`;
}

function executorLabel(executor: NonNullable<ActivityEntry["executor"]>) {
  if (executor === "agent") return "Agent";
  if (executor === "blueprint") return "Blueprint coordinator";
  return "Controller";
}

function resourceKindLabel(kind?: string) {
  if (!kind) return undefined;
  return kind
    .replaceAll("_", " ")
    .replace(/^./, (letter) => letter.toUpperCase());
}

function TaskOutcome({ task }: { task: ActivityEntry }) {
  const active = task.status === "pending" || task.status === "running";
  const failed = ["failed", "timed_out", "aborted"].includes(task.status);
  const title = active
    ? "Current state"
    : task.status === "completed"
      ? "Result"
      : "Failure";
  const summary = active
    ? task.status === "pending"
      ? "Waiting for an executor."
      : "Execution is in progress."
    : task.status === "completed"
      ? (task.resultSummary ??
        "The Task is recorded as completed. No result summary was captured.")
      : (task.failureSummary ?? failureFallback(task.status));
  const Icon = active ? Clock3 : failed ? CircleAlert : CircleCheck;

  return (
    <section
      aria-live={active ? "polite" : undefined}
      role={failed ? "alert" : undefined}
      className={`space-y-2 rounded-lg border p-4 text-sm ${
        failed
          ? "border-destructive/30 bg-destructive/5"
          : task.status === "completed"
            ? "border-success/30 bg-success/5"
            : "border-border bg-surface"
      }`}
    >
      <h3 className="flex items-center gap-2 font-medium">
        <Icon className="size-4 shrink-0" aria-hidden />
        {title}
      </h3>
      <p className="break-words text-muted-foreground [overflow-wrap:anywhere]">
        {summary}
      </p>
    </section>
  );
}

function failureFallback(status: ActivityEntry["status"]) {
  if (status === "timed_out")
    return "The Task reached its time limit. No failure summary was captured.";
  if (status === "aborted")
    return "The Task was aborted. No failure summary was captured.";
  return "The Task failed. No failure summary was captured.";
}

function Info({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="mt-1 break-words">{value}</dd>
    </div>
  );
}
