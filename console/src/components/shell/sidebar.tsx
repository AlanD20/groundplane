import { Link, useSearchParams } from "react-router-dom";
import {
  ArrowLeft,
  Layers,
  KeyRound,
  GitBranch,
  Settings,
  FolderKanban,
  Activity,
  Server,
  Cpu,
  Database,
  Package,
  Boxes,
  Building2,
  type LucideIcon,
} from "lucide-react";
import {
  WorkspaceNavigation,
  type NavigationGroup,
} from "@/components/common/workspace-navigation";
import {
  environmentNavigation,
  environmentSections,
} from "@/features/environment/workspace-navigation";
import {
  serviceDestination,
  serviceDestinations,
  backingDestinations,
} from "@/features/service/workspace-navigation";
import { useStore } from "@/lib/store";

// Global destinations with no local sections need only the Platform rail.
export function hasWorkspaceNavigation(pathname: string) {
  const [scope, section, resource] = pathname.split("/").filter(Boolean);
  return (
    scope === "t" ||
    (scope === "platform" &&
      (section === "overview" ||
        section === "host" ||
        section === "components" ||
        (section === "backing-services" && !!resource)))
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
  const env =
    project && parts[3]
      ? store.getEnvironment(
          workspace.slug,
          project.slug,
          decodeURIComponent(parts[3]),
        )
      : undefined;
  const service = env?.services.find(
    (entry) => entry.id === search.get("service"),
  );
  const tenantPath = `/t/${workspace.slug}`;
  const projectPath = project ? `${tenantPath}/${project.slug}` : "";
  const envPath = env ? `${projectPath}/${encodeURIComponent(env.name)}` : "";
  let parent: { label: string; href: string } | undefined;
  let groups: NavigationGroup[];
  const item = (label: string, href: string, icon: LucideIcon) => ({
    label,
    href,
    icon,
    active: pathname === href,
  });

  if (env) {
    parent = service
      ? {
          label: `${env.name} · Environment`,
          href: `${envPath}?${new URLSearchParams([...search].filter(([key]) => key !== "service" && key !== "serviceTab"))}`,
        }
      : { label: `${project!.name} · Project`, href: projectPath };
    if (service) {
      groups = [
        {
          label: `Service · ${service.name}`,
          items: serviceDestinations.map((entry) => {
            const next = new URLSearchParams(search);
            next.set("serviceTab", entry.key);
            return {
              label: entry.label,
              icon: entry.icon,
              href: `${envPath}?${next}`,
              active:
                pathname === envPath &&
                serviceDestination(search) === entry.key,
            };
          }),
        },
      ];
    } else {
      const current = environmentNavigation(search);
      groups = environmentSections.map((section) => ({
        label:
          section.key === "overview"
            ? `Environment · ${env.name}`
            : section.label,
        items: section.panels.map((panel) => ({
          label: panel.label,
          icon: panel.icon,
          href: `${envPath}?view=${section.key}&panel=${panel.key}`,
          active: pathname === envPath && current.panel.key === panel.key,
        })),
      }));
    }
  } else if (project) {
    parent = {
      label: `${tenant?.name ?? workspace.slug} · Tenant`,
      href: tenantPath,
    };
    groups = [
      {
        label: `Project · ${project.name}`,
        items: [
          item("Environments", projectPath, Layers),
          item("Secrets", `${projectPath}/secrets`, KeyRound),
          item("Settings", `${projectPath}/settings`, Settings),
        ],
      },
    ];
  } else if (workspace.kind === "tenant") {
    groups = [
      {
        label: `Tenant · ${tenant?.name ?? workspace.slug}`,
        items: [
          item("Projects", tenantPath, FolderKanban),
          item("Runners", `${tenantPath}/runners`, GitBranch),
          item("Tasks", `${tenantPath}/activity`, Activity),
          item("Settings", `${tenantPath}/settings`, Settings),
        ],
      },
    ];
  } else if (parts[1] === "backing-services" && parts[2]) {
    const backing = store.getBackingProject(parts[2]);
    parent = {
      label: "All backing services",
      href: "/platform/backing-services",
    };
    const selected =
      backingDestinations.find((entry) => entry.key === search.get("tab"))
        ?.key ?? "overview";
    groups = [
      {
        label: `Backing Service · ${backing?.name ?? parts[2]}`,
        items: backingDestinations.map((entry) => ({
          label: entry.label,
          icon: entry.icon,
          href: `${pathname}?tab=${entry.key}`,
          active: selected === entry.key,
        })),
      },
    ];
  } else if (parts[1] === "host" || parts[1] === "components") {
    groups = [
      {
        label: "Host",
        items: [
          item("Overview", "/platform/host", Server),
          item("Controller", "/platform/host/controller", Cpu),
          item("etcd", "/platform/host/etcd", Database),
          ...store.platform.agents.map((agent) =>
            item(
              `Agent · ${agent.host}`,
              `/platform/host/agents/${agent.id}`,
              Server,
            ),
          ),
          item("Images", "/platform/host/images", Package),
          item("Components", "/platform/components", Boxes),
        ],
      },
    ];
  } else if (parts[1] === "overview") {
    groups = [
      {
        label: "Tenants",
        items: store.tenants.map((entry) =>
          item(entry.name, `/t/${entry.slug}`, Building2),
        ),
      },
    ];
  } else return null;
  return (
    <div className="space-y-5 px-3 py-4">
      {parent && (
        <Link
          to={parent.href}
          className="flex items-start gap-2 rounded-md px-3 py-2 text-xs text-muted-foreground outline-none hover:bg-sidebar-accent focus-visible:ring-2 focus-visible:ring-ring"
        >
          <ArrowLeft className="size-3.5 shrink-0" />
          <span className="[overflow-wrap:anywhere]">{parent.label}</span>
        </Link>
      )}
      <WorkspaceNavigation label="Selected workspace" groups={groups} />
    </div>
  );
}
