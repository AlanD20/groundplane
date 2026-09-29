import { CopyButton } from "@/components/common/copy-button";
import { StatusBadge } from "@/components/common/status-badge";
import { TaskJournalMetadata } from "@/components/common/task-journal-metadata";
import { TaskLink } from "@/components/common/task-link";
import { useStore } from "@/lib/store";
import {
  resolveTaskOperationSurface,
  type TaskNavigationContext,
} from "@/lib/task-navigation";
import type { ActivityEntry, TaskType } from "@/lib/types";
import {
  ArrowUpRight,
  CheckCircle2,
  Circle,
  LoaderCircle,
  XCircle,
} from "lucide-react";
import { Link } from "react-router-dom";

const purposes: Record<TaskType, string> = {
  deploy: "Apply a Service release to its containers.",
  rollback: "Restore the selected previous Service release.",
  backup: "Create a backup of the selected sources.",
  backup_prune: "Remove backups selected by the retention policy.",
  restore: "Restore the selected backup.",
  attach: "Connect a consumer to a backing service.",
  detach: "Remove a backing-service connection.",
  run: "Execute the requested operation.",
  script: "Run the selected script.",
  provision: "Prepare the requested resource.",
  create: "Create the requested resource.",
  start: "Start the selected runtime.",
  stop: "Stop the selected runtime.",
  destroy: "Destroy the selected runtime.",
  remove: "Remove the selected resource after its safety checks.",
  update: "Apply the requested update.",
  rotate: "Rotate the selected credentials.",
  fetch: "Download and verify an image on the host.",
};

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
  const tenant = store.tenants.find(
    (tenant) =>
      tenant.id === (project?.tenantId ?? task.tenantId) ||
      tenant.id === task.target,
  );
  const agent = store.platform.agents.find((agent) => agent.id === task.target);
  const runner = store.runners.find((runner) => runner.id === task.target);
  const image = task.target.startsWith("sha256:");
  const resource =
    service?.name ??
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
      href: `/t/${encodeURIComponent(tenant.slug)}/${encodeURIComponent(project.slug)}/${encodeURIComponent(env.name)}?view=services&service=${encodeURIComponent(service.id)}`,
      label: "Open Service",
      fallback: false,
    };
  if (image)
    destination = {
      href: "/platform/host/images",
      label: "Open images",
      fallback: false,
    };
  return {
    resource,
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
  const failed = steps.filter((step) => step.state === "failed");
  const statusText = {
    pending: "Queued. Execution has not started yet.",
    running: "In progress. This view updates automatically.",
    completed: "This Task completed successfully.",
    failed: "This Task failed. It did not complete successfully.",
    aborted:
      "This Task was aborted. Check the resource before starting another operation.",
    timed_out:
      "This Task exceeded its time limit. Check the resource before retrying.",
  }[task.status];
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
      <section
        className="space-y-3 rounded-lg border border-border bg-surface p-4"
        aria-live="polite"
      >
        <StatusBadge status={task.status} />
        <p className="text-sm font-medium">{statusText}</p>
        <p className="text-sm text-muted-foreground">{purposes[task.type]}</p>
        {steps.length > 0 && (
          <p className="text-xs text-muted-foreground">
            {done} of {steps.length} recorded steps completed
            {failed.length ? `; ${failed.length} failed` : ""}.
          </p>
        )}
      </section>
      <section className="space-y-2">
        <h3 className="text-sm font-medium">Affected resource</h3>
        <p className="break-words text-sm">{view.resource}</p>
        <p className="break-words text-xs text-muted-foreground">
          {view.scope}
        </p>
        {view.destination && (
          <Link
            className="inline-flex items-center gap-1 text-sm text-primary hover:underline"
            to={view.destination.href}
            onClick={onClose}
          >
            {view.destination.label}
            <ArrowUpRight className="size-3.5" />
          </Link>
        )}
      </section>
      <dl className="grid grid-cols-1 gap-3 text-xs sm:grid-cols-2">
        <Info
          label="Requested by"
          value={
            task.actor === "system" ? "Groundplane (automatic)" : "Operator"
          }
        />
        <Info
          label={task.finishedAt ? "Execution time" : "Elapsed execution"}
          value={
            duration === null
              ? "Not started"
              : duration < 60
                ? `${duration}s`
                : `${Math.floor(duration / 60)}m ${duration % 60}s`
          }
        />
        <Info label="Requested" value={timestamp(task.createdAt)} />
        <Info
          label="Started"
          value={task.startedAt ? timestamp(task.startedAt) : "Not started"}
        />
        <Info
          label={task.finishedAt ? "Finished" : "Last update"}
          value={timestamp(task.finishedAt ?? task.updatedAt)}
        />
      </dl>
      {task.note && (
        <p className="rounded-lg border border-border p-3 text-sm">
          {task.note}
        </p>
      )}
      {task.status === "failed" && !task.note && (
        <p className="rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-sm">
          {failed.length
            ? `Failed step: ${failed.map((step) => step.label).join(", ")}. `
            : ""}
          No detailed failure message is available in this Task response. Use
          its ID below to find the diagnostic in the Controller or Agent logs.
        </p>
      )}
      <section className="space-y-2">
        <h3 className="text-sm font-medium">Execution steps</h3>
        {steps.length ? (
          <ol className="space-y-2">
            {steps.map((step, index) => {
              const Icon =
                step.state === "done"
                  ? CheckCircle2
                  : step.state === "failed"
                    ? XCircle
                    : step.state === "running"
                      ? LoaderCircle
                      : Circle;
              return (
                <li
                  key={`${index}/${step.label}`}
                  className="flex items-start gap-2 rounded-lg border border-border p-3 text-sm"
                >
                  <Icon
                    aria-hidden
                    className={`mt-0.5 size-4 shrink-0 ${step.state === "running" ? "animate-spin text-primary" : step.state === "failed" ? "text-destructive" : step.state === "done" ? "text-success" : "text-muted-foreground"}`}
                  />
                  <div className="min-w-0 flex-1">
                    <p className="break-words">
                      {step.label.replaceAll("_", " ")}
                    </p>
                    {step.detail && (
                      <p className="mt-1 break-words text-xs text-muted-foreground">
                        {step.detail}
                      </p>
                    )}
                  </div>
                  <span className="text-xs text-muted-foreground">
                    {step.state === "done" ? "Completed" : step.state}
                  </span>
                </li>
              );
            })}
          </ol>
        ) : (
          <p className="text-sm text-muted-foreground">
            No individual step updates are recorded for this Task. Its overall
            status is shown above.
          </p>
        )}
      </section>
      <details className="rounded-lg border border-border p-3">
        <summary className="cursor-pointer text-sm font-medium">
          Technical details
        </summary>
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
      </details>
    </>
  );
}

function timestamp(value?: string | null) {
  return value ? new Date(value).toLocaleString() : "Not recorded";
}
function Info({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="mt-1 break-words">{value}</dd>
    </div>
  );
}
