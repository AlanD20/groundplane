"use client";

import { StatusDot } from "@/components/common/status-badge";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { useStore } from "@/lib/store";
import { cn } from "@/lib/utils";
import { Check, ChevronDown, ChevronRight, Server } from "lucide-react";
import { Fragment } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";

type Crumb = { label: string; href?: string };
type CrumbOption = { label: string; href: string };

function buildCrumbs(
  pathname: string,
  lookups: {
    tenantName: (s: string) => string;
    projectName: (tenantSlug: string, projectSlug: string) => string;
    backingProjectName: (s: string) => string;
    componentName: (s: string) => string;
    agentName: (s: string) => string;
  },
): Crumb[] {
  const parts = pathname.split("/").filter(Boolean);
  if (parts.length === 0) return [{ label: "Platform" }];

  if (parts[0] === "platform") {
    const map: Record<string, string> = {
      overview: "Overview",
      "backing-services": "Backing services",
      components: "Components",
      activity: "Activity",
      secrets: "Secret store",
      host: "Host",
      settings: "Settings",
    };
    const crumbs: Crumb[] = [{ label: "Platform", href: "/platform/overview" }];
    if (parts[1])
      crumbs.push({
        label: map[parts[1]] ?? parts[1],
        href: `/platform/${parts[1]}`,
      });
    if (parts[1] === "host") {
      if (parts[2] === "controller") crumbs.push({ label: "Controller" });
      if (parts[2] === "etcd") crumbs.push({ label: "etcd" });
      if (parts[2] === "images") crumbs.push({ label: "Images" });
      if (parts[2] === "agents" && parts[3])
        crumbs.push({ label: `Agent / ${lookups.agentName(parts[3])}` });
      return crumbs;
    }
    if (parts[2]) {
      crumbs.push({
        label:
          parts[1] === "components"
            ? lookups.componentName(parts[2])
            : lookups.backingProjectName(parts[2]),
      });
    }
    return crumbs;
  }

  if (parts[0] === "t") {
    const slug = parts[1];
    const crumbs: Crumb[] = [
      { label: lookups.tenantName(slug), href: `/t/${slug}` },
    ];
    if (parts[2] === "runners") crumbs.push({ label: "Runners" });
    else if (parts[2] === "activity") crumbs.push({ label: "Activity" });
    else if (parts[2] === "settings") crumbs.push({ label: "Settings" });
    else if (parts[2]) {
      crumbs.push({
        label: lookups.projectName(slug, parts[2]),
        href: `/t/${slug}/${parts[2]}`,
      });
      if (parts[3] === "secrets") crumbs.push({ label: "Secrets" });
      else if (parts[3] === "settings") crumbs.push({ label: "Settings" });
      else if (parts[3]) crumbs.push({ label: parts[3] });
    }
    return crumbs;
  }
  return [{ label: "Platform" }];
}

const PLATFORM_SECTIONS: CrumbOption[] = [
  { label: "Overview", href: "/platform/overview" },
  { label: "Backing services", href: "/platform/backing-services" },
  { label: "Components", href: "/platform/components" },
  { label: "Activity", href: "/platform/activity" },
  { label: "Secret store", href: "/platform/secrets" },
  { label: "Host", href: "/platform/host" },
  { label: "Settings", href: "/platform/settings" },
];

function tenantSections(slug: string): CrumbOption[] {
  return [
    { label: "Projects", href: `/t/${slug}` },
    { label: "Runners", href: `/t/${slug}/runners` },
    { label: "Activity", href: `/t/${slug}/activity` },
    { label: "Settings", href: `/t/${slug}/settings` },
  ];
}

function attachOptions(
  pathname: string,
  crumbs: Crumb[],
  store: ReturnType<typeof useStore>,
): (Crumb & { options: CrumbOption[] })[] {
  const parts = pathname.split("/").filter(Boolean);
  const workspaces: CrumbOption[] = [
    { label: "Platform", href: "/platform/overview" },
    ...store.tenants.map((t) => ({ label: t.name, href: `/t/${t.slug}` })),
  ];
  const backingProjects: CrumbOption[] = store.backingProjects.map((g) => ({
    label: g.name,
    href: `/platform/backing-services/${g.id}`,
  }));

  return crumbs.map((c, i) => {
    if (i === 0) return { ...c, options: workspaces };

    if (parts[0] === "platform") {
      if (i === 1) return { ...c, options: PLATFORM_SECTIONS };
      if (i === 2 && parts[1] === "host")
        return {
          ...c,
          options: [
            { label: "Controller", href: "/platform/host/controller" },
            { label: "etcd", href: "/platform/host/etcd" },
            { label: "Images", href: "/platform/host/images" },
            ...store.platform.agents.map((agent) => ({
              label: `Agent / ${agent.host}`,
              href: `/platform/host/agents/${agent.id}`,
            })),
          ],
        };
      if (i === 2 && parts[1] === "backing-services")
        return { ...c, options: backingProjects };
    }

    if (parts[0] === "t") {
      const slug = parts[1];
      const tenant = store.getTenant(slug);
      const isSection = ["runners", "activity", "settings"].includes(parts[2]);
      if (i === 1 && isSection) return { ...c, options: tenantSections(slug) };
      if (i === 1) {
        const projects = store.tenantProjects.filter(
          (p) => p.tenantId === tenant?.id,
        );
        return {
          ...c,
          options: projects.map((p) => ({
            label: p.name,
            href: `/t/${slug}/${p.slug}`,
          })),
        };
      }
      if (i === 2 && parts[2]) {
        const envs = store.getProject(slug, parts[2])?.environments ?? [];
        const envOptions = envs.map((e) => ({
          label: e.name,
          href: `/t/${slug}/${parts[2]}/${e.name}`,
        }));
        const sectionOptions: CrumbOption[] = [
          { label: "Secrets", href: `/t/${slug}/${parts[2]}/secrets` },
        ];
        return { ...c, options: [...envOptions, ...sectionOptions] };
      }
      if (i === 3 && parts[3]) {
        const environment = store.getEnvironment(
          slug,
          parts[2],
          decodeURIComponent(parts[3]),
        );
        return {
          ...c,
          options: (environment?.services ?? []).map((service) => ({
            label: service.name,
            href: `${pathname}?view=services&panel=overview&service=${service.id}`,
          })),
        };
      }
    }
    return { ...c, options: [] };
  });
}

export function Topbar({ pathname }: { pathname: string }) {
  const navigate = useNavigate();
  const [search] = useSearchParams();
  const store = useStore();
  const { host } = store;
  const baseCrumbs = buildCrumbs(pathname, {
    tenantName: (s) => store.tenants.find((t) => t.slug === s)?.name ?? s,
    projectName: (tenantSlug, projectSlug) =>
      store.getProject(tenantSlug, projectSlug)?.name ?? projectSlug,
    backingProjectName: (s) =>
      store.backingProjects.find((g) => g.id === s)?.name ?? s,
    componentName: (s) =>
      store.platform.components.find((component) => component.kind === s)
        ?.name ?? s,
    agentName: (s) =>
      store.platform.agents.find((agent) => agent.id === s)?.host ?? s,
  });
  const parts = pathname.split("/").filter(Boolean);
  const selectedService =
    parts[0] === "t" && parts[3]
      ? store
          .getEnvironment(parts[1], parts[2], decodeURIComponent(parts[3]))
          ?.services.find((service) => service.id === search.get("service"))
      : undefined;
  if (selectedService) {
    baseCrumbs[baseCrumbs.length - 1].href = pathname;
    baseCrumbs.push({ label: selectedService.name });
  }
  const crumbs = attachOptions(pathname, baseCrumbs, store);

  return (
    <header className="relative z-20 flex min-h-14 items-center justify-between gap-3 py-3">
      <nav
        aria-label="Breadcrumb"
        className="flex min-w-0 items-center gap-1 overflow-x-auto text-xs"
      >
        {crumbs.map((c, i) => (
          <Fragment key={i}>
            {i > 0 && (
              <ChevronRight className="size-3.5 shrink-0 text-muted-foreground/50" />
            )}
            <div className="flex items-center gap-0.5">
              {c.href && i < crumbs.length - 1 ? (
                <Link
                  to={c.href}
                  className={cn(
                    "whitespace-nowrap rounded-md px-1.5 py-0.5 outline-none transition-colors",
                    "hover:bg-muted hover:text-foreground focus-visible:ring-1 focus-visible:ring-ring",
                    i < crumbs.length - 1
                      ? "text-muted-foreground"
                      : "font-medium text-foreground",
                  )}
                >
                  {c.label}
                </Link>
              ) : (
                <span
                  className={cn(
                    "whitespace-nowrap px-1.5 py-0.5",
                    i < crumbs.length - 1
                      ? "text-muted-foreground"
                      : "font-medium text-foreground",
                  )}
                >
                  {c.label}
                </span>
              )}
              {c.options.length > 0 && (
                <DropdownMenu>
                  <DropdownMenuTrigger
                    aria-label={`Open ${c.label} menu`}
                    className="flex items-center rounded-md p-1 text-muted-foreground/60 outline-none transition-colors hover:bg-muted hover:text-foreground focus-visible:ring-1 focus-visible:ring-ring"
                  >
                    <ChevronDown className="size-3.5" />
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="start" className="max-w-64">
                    {c.options.map((o) => (
                      <DropdownMenuItem
                        key={o.href}
                        onClick={() => navigate(o.href)}
                      >
                        <span className="flex-1 truncate">{o.label}</span>
                        {o.href === pathname && (
                          <Check className="size-4 text-primary" />
                        )}
                      </DropdownMenuItem>
                    ))}
                  </DropdownMenuContent>
                </DropdownMenu>
              )}
            </div>
          </Fragment>
        ))}
      </nav>

      <div className="flex shrink-0 items-center gap-2">
        {host ? (
          <Link
            to="/platform/host"
            className="hidden items-center gap-3 rounded-md px-2 py-1.5 text-[11px] hover:bg-muted xl:flex"
            aria-label={`Host ${host.hostname}: Controller ${host.controller.status}, Agent ${host.agent.status}`}
          >
            <span className="flex items-center gap-1.5">
              <StatusDot status={host.controller.status} />
              <span className="text-muted-foreground">Controller</span>
            </span>
            <span className="flex items-center gap-1.5">
              <StatusDot status={host.agent.status} />
              <span className="text-muted-foreground">Agent</span>
            </span>
          </Link>
        ) : (
          <div className="hidden items-center gap-2 rounded-lg border border-border bg-surface px-2.5 py-1.5 text-xs text-muted-foreground sm:flex">
            <Server className="size-3.5" /> Host health unavailable
          </div>
        )}
      </div>
    </header>
  );
}
