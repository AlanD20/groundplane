"use client";

import {
  environmentNavigation,
  environmentSections,
} from "@/features/environment/workspace-navigation";
import { useStore } from "@/lib/store";
import { cn } from "@/lib/utils";
import {
  Activity,
  Boxes,
  Cpu,
  Database,
  KeyRound,
  Layers,
  LayoutDashboard,
  Package,
  Server,
  Settings,
  ShieldCheck,
} from "lucide-react";
import { Link, useSearchParams } from "react-router-dom";

type NavItem = { label: string; href: string; icon: React.ReactNode };

function platformNav(): { section: string; items: NavItem[] }[] {
  return [
    {
      section: "Platform",
      items: [
        {
          label: "Overview",
          href: "/platform/overview",
          icon: <LayoutDashboard />,
        },
        {
          label: "Backing services",
          href: "/platform/backing-services",
          icon: <Database />,
        },
        { label: "Components", href: "/platform/components", icon: <Server /> },
        { label: "Activity", href: "/platform/activity", icon: <Activity /> },
        {
          label: "Secret store",
          href: "/platform/secrets",
          icon: <ShieldCheck />,
        },
        { label: "Host", href: "/platform/host", icon: <Cpu /> },
        { label: "Images", href: "/platform/host/images", icon: <Package /> },
        { label: "Settings", href: "/platform/settings", icon: <Settings /> },
      ],
    },
  ];
}

function tenantNav(slug: string): { section: string; items: NavItem[] }[] {
  return [
    {
      section: "Tenant",
      items: [
        { label: "Projects", href: `/t/${slug}`, icon: <Boxes /> },
        { label: "Runners", href: `/t/${slug}/runners`, icon: <Cpu /> },
        { label: "Activity", href: `/t/${slug}/activity`, icon: <Activity /> },
        {
          label: "Tenant settings",
          href: `/t/${slug}/settings`,
          icon: <Settings />,
        },
      ],
    },
  ];
}

// Inside a project (/t/<tenant>/<project>/…), the project's own resources
// sit in their own section above the tenant-level items.
function projectNav(
  slug: string,
  projectSlug: string,
): { section: string; items: NavItem[] }[] {
  return [
    {
      section: "Project",
      items: [
        {
          label: "Environments",
          href: `/t/${slug}/${projectSlug}`,
          icon: <Layers />,
        },
        {
          label: "Secrets",
          href: `/t/${slug}/${projectSlug}/secrets`,
          icon: <KeyRound />,
        },
        {
          label: "Settings",
          href: `/t/${slug}/${projectSlug}/settings`,
          icon: <Settings />,
        },
      ],
    },
    ...tenantNav(slug),
  ];
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
  const environment =
    projectSlug && parts[3]
      ? store.getEnvironment(
          workspace.slug,
          projectSlug,
          decodeURIComponent(parts[3]),
        )
      : undefined;
  const base = environment
    ? `/t/${workspace.slug}/${projectSlug}/${encodeURIComponent(environment.name)}`
    : undefined;
  const { section: currentSection } = environmentNavigation(search);
  const sections =
    environment && base
      ? [
          {
            section: environment.name,
            items: environmentSections.map((section) => ({
              label: section.label,
              href: `${base}?view=${section.key}`,
              icon: <section.icon />,
            })),
          },
          {
            section: "Project",
            items: [
              {
                label: "Environments",
                href: `/t/${workspace.slug}/${projectSlug}`,
                icon: <Layers />,
              },
              {
                label: "Project secrets",
                href: `/t/${workspace.slug}/${projectSlug}/secrets`,
                icon: <KeyRound />,
              },
              {
                label: "Project settings",
                href: `/t/${workspace.slug}/${projectSlug}/settings`,
                icon: <Settings />,
              },
            ],
          },
          {
            section: "Shared infrastructure",
            items: [
              {
                label: "Backing services",
                href: "/platform/backing-services",
                icon: <Database />,
              },
              {
                label: "Host images",
                href: "/platform/host/images",
                icon: <Package />,
              },
              { label: "Host", href: "/platform/host", icon: <Cpu /> },
            ],
          },
          ...tenantNav(workspace.slug),
        ]
      : workspace.kind === "platform"
        ? platformNav()
        : projectSlug
          ? projectNav(workspace.slug, projectSlug)
          : tenantNav(workspace.slug);

  function isActive(href: string) {
    if (base && href.startsWith(`${base}?view=`)) {
      const section = pathname.startsWith(`${base}/`)
        ? "operations"
        : currentSection.key;
      return href === `${base}?view=${section}`;
    }
    if (href === "/platform/host")
      return (
        pathname === href ||
        pathname.startsWith("/platform/host/controller") ||
        pathname.startsWith("/platform/host/agents/")
      );
    // Section roots (tenant page, project page) highlight only on the exact
    // route — /t/<tenant>/<project>/secrets must not highlight Environments.
    if (
      href === `/t/${workspace.slug}` ||
      (projectSlug && href === `/t/${workspace.slug}/${projectSlug}`)
    ) {
      return pathname === href;
    }
    return pathname === href || pathname.startsWith(href + "/");
  }

  return (
    <nav
      aria-label="Workspace navigation"
      className="flex flex-col gap-6 px-3 pb-6 pt-2"
    >
      {sections.map((sec) => (
        <div key={sec.section} className="flex flex-col gap-1">
          <span className="px-2.5 pb-2 text-[10px] font-medium uppercase tracking-[0.12em] text-muted-foreground">
            {sec.section}
          </span>
          {sec.items.map((item) => {
            const active = isActive(item.href);
            return (
              <Link
                key={item.href}
                to={item.href}
                aria-current={active ? "page" : undefined}
                className={cn(
                  "flex items-center gap-2.5 rounded-md px-2.5 py-2 text-xs font-medium outline-none transition-colors",
                  "[&_svg]:size-4 [&_svg]:shrink-0 focus-visible:ring-1 focus-visible:ring-ring",
                  active
                    ? "bg-sidebar-accent text-sidebar-accent-foreground [&_svg]:text-primary"
                    : "text-muted-foreground hover:bg-sidebar-accent/60 hover:text-foreground [&_svg]:text-muted-foreground",
                )}
              >
                {item.icon}
                <span className="truncate">{item.label}</span>
              </Link>
            );
          })}
        </div>
      ))}
    </nav>
  );
}
