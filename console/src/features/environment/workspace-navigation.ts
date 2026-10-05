import {
  Activity,
  Boxes,
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
    panels: [{ key: "overview", label: "Environment overview" }],
  },
  {
    key: "services",
    label: "Services",
    icon: Boxes,
    panels: [
      { key: "services", label: "Services" },
      { key: "logs", label: "Environment logs" },
      { key: "service-list", label: "Service list" },
    ],
  },
  {
    key: "network",
    label: "Network",
    icon: Network,
    panels: [
      { key: "zones", label: "Zones & topology" },
      { key: "routes", label: "Routes" },
      { key: "attaches", label: "Backing connections" },
      { key: "router", label: "Router" },
    ],
  },
  {
    key: "configuration",
    label: "Configuration",
    icon: FileCode2,
    panels: [
      { key: "blueprint", label: "Blueprint" },
      { key: "entries", label: "Entries" },
      { key: "facts", label: "Attach facts" },
      { key: "volumes", label: "Volumes" },
      { key: "scripts", label: "Scripts" },
    ],
  },
  {
    key: "operations",
    label: "Operations",
    icon: Activity,
    panels: [
      { key: "tasks", label: "Tasks" },
      { key: "releases", label: "Releases" },
      { key: "release-groups", label: "Release groups" },
      { key: "backups", label: "Backups & connectors" },
    ],
  },
  {
    key: "settings",
    label: "Settings",
    icon: Settings,
    panels: [{ key: "settings", label: "Settings" }],
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
