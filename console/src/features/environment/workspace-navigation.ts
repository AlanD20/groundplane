import {
  Activity,
  FileCode2,
  Network,
  Layers,
  Settings,
  type LucideIcon,
} from "lucide-react";

export const environmentSections = [
  {
    key: "overview",
    label: "Overview",
    icon: Layers,
    panels: [{ key: "overview", label: "Services & overview" }],
  },
  {
    key: "network",
    label: "Networking",
    icon: Network,
    panels: [
      { key: "zones", label: "Zones & topology" },
      { key: "routes", label: "Routes" },
      { key: "attaches", label: "Backing connections" },
      { key: "facts", label: "Connection values" },
      { key: "router", label: "Router" },
    ],
  },
  {
    key: "configuration",
    label: "Workloads",
    icon: FileCode2,
    panels: [
      { key: "blueprint", label: "Blueprint" },
      { key: "entries", label: "Variables & files" },
      { key: "volumes", label: "Volumes" },
      { key: "scripts", label: "Scripts" },
    ],
  },
  {
    key: "operations",
    label: "Delivery & recovery",
    icon: Activity,
    panels: [
      { key: "tasks", label: "Tasks" },
      { key: "logs", label: "Logs" },
      { key: "releases", label: "Deployments" },
      { key: "release-groups", label: "Deployment groups" },
      { key: "backups", label: "Backups & restore" },
      { key: "connectors", label: "Backup destinations" },
    ],
  },
  {
    key: "settings",
    label: "Settings",
    icon: Settings,
    panels: [{ key: "settings", label: "Identity & encryption key" }],
  },
] satisfies {
  key: string;
  label: string;
  icon: LucideIcon;
  panels: { key: string; label: string }[];
}[];

export function environmentNavigation(search: URLSearchParams) {
  const section =
    environmentSections.find((section) => section.key === search.get("view")) ??
    environmentSections[0];
  const panel =
    section.panels.find((panel) => panel.key === search.get("panel")) ??
    section.panels[0];
  return { section, panel };
}
