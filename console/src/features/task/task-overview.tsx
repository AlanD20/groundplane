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
import { ArrowUpRight } from "lucide-react";
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
        <SummaryItem label="Recorded steps">
          {steps.length ? `${done} / ${steps.length} steps` : "Not recorded"}
        </SummaryItem>
        <SummaryItem label="Failed steps">{failed.length}</SummaryItem>
        <SummaryItem label={task.finishedAt ? "Execution time" : "Elapsed"}>
          {duration === null
            ? "Not started"
            : duration < 60
              ? duration === 0
                ? "<1s"
                : `${duration}s`
              : `${Math.floor(duration / 60)}m ${duration % 60}s`}
        </SummaryItem>
        <SummaryItem label="Requested by">
          {task.actor === "system" ? "Groundplane" : "Operator"}
        </SummaryItem>
      </SummaryStrip>
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
        <p className="rounded-lg border border-border p-3 text-sm">
          {task.note}
        </p>
      )}
      {(task.status === "aborted" || task.status === "timed_out") && (
        <p
          role="status"
          className="rounded-lg border border-warning/30 bg-warning/5 p-3 text-xs text-warning"
        >
          {task.status === "aborted" ? "Task aborted." : "Task timed out."}{" "}
          Check the affected resource before starting another operation.
        </p>
      )}
      {task.status === "failed" && !task.note && (
        <p className="rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-sm">
          {failed.length
            ? `Failed action: ${failed.map((step) => step.action ?? "Execution details unavailable").join(", ")}. `
            : ""}
          No detailed failure message is available in this Task response. Use
          its ID below to find the diagnostic in the Controller or Agent logs.
        </p>
      )}
      <TaskExecutionTerminal task={task} />
      <AdvancedDetails>
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
function Info({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="mt-1 break-words">{value}</dd>
    </div>
  );
}
