import {
  resolveTaskOperationSurface,
  type TaskNavigationContext,
} from "@/lib/task-navigation";
import type { ActivityEntry } from "@/lib/types";

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

function resourceKindLabel(kind?: string) {
  return kind
    ?.replaceAll("_", " ")
    .replace(/^./, (letter) => letter.toUpperCase());
}
