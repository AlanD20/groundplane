import { StatusDot } from "@/components/common/status-badge";
import { currentServiceObservation } from "@/features/service/service-observation";
import { useStore } from "@/lib/store";
import { cn } from "@/lib/utils";
import {
  Activity,
  Boxes,
  ChevronDown,
  Database,
  GitBranch,
  KeyRound,
  Layers,
  Network,
  Package,
  Server,
  Settings,
} from "lucide-react";
import { useEffect, useState, type ReactNode } from "react";
import { Link, useSearchParams } from "react-router-dom";

type Item = { label: string; href: string; icon: ReactNode };
function NavigationGroup({
  title,
  items,
  pathname,
}: {
  title: string;
  items: Item[];
  pathname: string;
}) {
  return (
    <section className="space-y-1">
      <h2 className="px-2 pb-2 text-[10px] font-medium uppercase tracking-[.12em] text-muted-foreground">
        {title}
      </h2>
      {items.map((item) => (
        <Link
          key={item.href}
          to={item.href}
          aria-current={pathname === item.href ? "page" : undefined}
          className={cn(
            "flex min-w-0 items-center gap-2 rounded-md px-2 py-2 text-xs text-muted-foreground transition-colors hover:bg-sidebar-accent hover:text-foreground [&_svg]:size-3.5 [&_svg]:shrink-0",
            pathname === item.href && "bg-sidebar-accent text-foreground",
          )}
        >
          {item.icon}
          <span className="truncate">{item.label}</span>
        </Link>
      ))}
    </section>
  );
}
export function Sidebar({
  workspace,
  projectSlug,
  pathname,
}: {
  workspace: { kind: "platform" | "tenant"; slug: string };
  projectSlug?: string;
  pathname: string;
}) {
  const store = useStore();
  const [search] = useSearchParams();
  const parts = pathname.split("/").filter(Boolean);
  const tenant = store.getTenant(workspace.slug);
  const project = projectSlug
    ? store.getProject(workspace.slug, projectSlug)
    : undefined;
  const environment =
    project && parts[3]
      ? store.getEnvironment(
          workspace.slug,
          project.slug,
          decodeURIComponent(parts[3]),
        )
      : undefined;
  const [now, setNow] = useState(() => Date.now());
  const [expiryGeneration, setExpiryGeneration] = useState(0);
  const expiryKey = (environment?.services ?? [])
    .flatMap((service) =>
      service.observation.state === "unavailable"
        ? []
        : [service.observation.expiresAt],
    )
    .sort()
    .join(",");
  useEffect(() => {
    const current = Date.now();
    setNow(current);
    const expiry = expiryKey
      .split(",")
      .map(Date.parse)
      .filter((value) => Number.isFinite(value) && value > current)
      .sort((a, b) => a - b)[0];
    if (expiry === undefined) return;
    const timer = window.setTimeout(
      () => {
        setNow(Date.now());
        setExpiryGeneration((generation) => generation + 1);
      },
      expiry - current + 1,
    );
    return () => window.clearTimeout(timer);
  }, [expiryKey, expiryGeneration]);
  const base = project ? `/t/${workspace.slug}/${project.slug}` : undefined;
  const envBase =
    environment && base
      ? `${base}/${encodeURIComponent(environment.name)}`
      : undefined;
  let groups: { title: string; items: Item[] }[];
  if (workspace.kind === "tenant")
    groups = [
      {
        title: "Project resources",
        items: [
          {
            label: "All Projects",
            href: `/t/${workspace.slug}`,
            icon: <Layers />,
          },
          ...(base
            ? [
                {
                  label: "Project Secrets",
                  href: `${base}/secrets`,
                  icon: <KeyRound />,
                },
                {
                  label: "Project settings",
                  href: `${base}/settings`,
                  icon: <Settings />,
                },
              ]
            : []),
        ],
      },
      {
        title: "Tenant",
        items: [
          {
            label: "Runners",
            href: `/t/${workspace.slug}/runners`,
            icon: <GitBranch />,
          },
          {
            label: "Activity",
            href: `/t/${workspace.slug}/activity`,
            icon: <Activity />,
          },
          {
            label: "Tenant settings",
            href: `/t/${workspace.slug}/settings`,
            icon: <Settings />,
          },
        ],
      },
    ];
  else if (pathname.startsWith("/platform/backing-services"))
    groups = [
      {
        title: "Shared services",
        items: [
          {
            label: "Backing Services",
            href: "/platform/backing-services",
            icon: <Database />,
          },
          ...store.backingProjects.map((p) => ({
            label: p.name,
            href: `/platform/backing-services/${p.id}`,
            icon: <Database />,
          })),
        ],
      },
    ];
  else if (
    pathname.startsWith("/platform/host") ||
    pathname.startsWith("/platform/components")
  )
    groups = [
      {
        title: "Host",
        items: [
          { label: "Overview", href: "/platform/host", icon: <Server /> },
          {
            label: "Controller",
            href: "/platform/host/controller",
            icon: <Server />,
          },
          { label: "etcd", href: "/platform/host/etcd", icon: <Database /> },
          ...store.platform.agents.map((a) => ({
            label: `Agent · ${a.host}`,
            href: `/platform/host/agents/${a.id}`,
            icon: <Server />,
          })),
          { label: "Images", href: "/platform/host/images", icon: <Package /> },
          {
            label: "Components",
            href: "/platform/components",
            icon: <Network />,
          },
        ],
      },
    ];
  else if (pathname === "/platform/activity")
    groups = [
      {
        title: "Activity",
        items: [
          {
            label: "All Tasks",
            href: "/platform/activity",
            icon: <Activity />,
          },
        ],
      },
    ];
  else if (
    pathname.startsWith("/platform/settings") ||
    pathname === "/platform/secrets"
  )
    groups = [
      {
        title: "Settings",
        items: [
          { label: "Overview", href: "/platform/settings", icon: <Settings /> },
          {
            label: "Platform Secrets",
            href: "/platform/secrets",
            icon: <KeyRound />,
          },
        ],
      },
    ];
  else
    groups = [
      {
        title: "Projects",
        items: [
          { label: "Overview", href: "/platform/overview", icon: <Layers /> },
          ...store.tenants.map((t) => ({
            label: t.name,
            href: `/t/${t.slug}`,
            icon: <Layers />,
          })),
        ],
      },
    ];
  return (
    <nav
      aria-label="Workspace explorer"
      className="flex flex-col gap-6 px-4 py-5"
    >
      {workspace.kind === "tenant" && (
        <section className="space-y-2">
          <h2 className="px-2 text-[10px] font-medium uppercase tracking-[.12em] text-muted-foreground">
            Project explorer
          </h2>
          {store.tenantProjects
            .filter(
              (p) =>
                p.tenantId === tenant?.id && (!project || p.id === project.id),
            )
            .map((p) => (
              <div key={p.id}>
                <Link
                  to={`/t/${workspace.slug}/${p.slug}`}
                  className="flex items-center gap-2 px-2 py-2 text-xs font-medium"
                >
                  <ChevronDown className="size-3" />
                  <Layers className="size-3.5" />
                  <span className="truncate">{p.name}</span>
                </Link>
                {(p.environments || [])
                  .filter((e) => !environment || e.id === environment.id)
                  .map((e) => (
                    <div key={e.id} className="pl-4">
                      <Link
                        to={`/t/${workspace.slug}/${p.slug}/${encodeURIComponent(e.name)}`}
                        className="flex items-center gap-2 px-2 py-2 text-xs"
                      >
                        <ChevronDown className="size-3" />
                        <span className="size-1.5 rounded-full bg-primary" />
                        {e.name}
                      </Link>
                      {e.id === environment?.id && envBase && (
                        <div className="space-y-1 border-l border-border pl-2">
                          <span className="block px-2 py-2 text-[10px] text-muted-foreground">
                            Services
                          </span>
                          {e.services.map((s) => (
                            <Link
                              key={s.id}
                              to={`${envBase}?view=overview&service=${encodeURIComponent(s.id)}`}
                              aria-current={
                                search.get("service") === s.id
                                  ? "page"
                                  : undefined
                              }
                              className={cn(
                                "flex min-w-0 items-center gap-2 rounded-md px-2 py-2 text-xs text-muted-foreground hover:bg-sidebar-accent hover:text-foreground",
                                search.get("service") === s.id &&
                                  "bg-sidebar-accent text-foreground",
                              )}
                            >
                              <Boxes className="size-3.5 shrink-0" />
                              <span className="truncate">{s.name}</span>
                              <span
                                className="ml-auto inline-flex shrink-0"
                                role="img"
                                aria-label={`${s.name}: ${currentServiceObservation(s.observation, now).state}`}
                                title={`${s.name}: ${currentServiceObservation(s.observation, now).state}`}
                              >
                                <StatusDot
                                  status={
                                    currentServiceObservation(
                                      s.observation,
                                      now,
                                    ).state
                                  }
                                  className="size-1.5"
                                />
                              </span>
                            </Link>
                          ))}
                        </div>
                      )}
                    </div>
                  ))}
              </div>
            ))}
        </section>
      )}
      {groups.map((g) => (
        <NavigationGroup key={g.title} {...g} pathname={pathname} />
      ))}
    </nav>
  );
}
