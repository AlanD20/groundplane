import {
  GitBranch,
  Route,
  Plug,
  KeyRound,
  Router,
  Braces,
  HardDrive,
  SquareTerminal,
  Terminal,
  History,
  Workflow,
  ArchiveRestore,
  Database,
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
    panels: [{ key: "overview", label: "Services", icon: Layers }],
  },
  {
    key: "network",
    label: "Networking",
    icon: Network,
    panels: [
      { key: "zones", label: "Zones & topology", icon: Network },
      { key: "routes", label: "Routes", icon: Route },
      { key: "attaches", label: "Backing connections", icon: Plug },
      { key: "facts", label: "Connection values", icon: KeyRound },
      { key: "router", label: "Router", icon: Router },
    ],
  },
  {
    key: "configuration",
    label: "Workloads",
    icon: FileCode2,
    panels: [
      { key: "blueprint", label: "Blueprint", icon: FileCode2 },
      { key: "entries", label: "Variables & files", icon: Braces },
      { key: "volumes", label: "Volumes", icon: HardDrive },
      { key: "scripts", label: "Scripts", icon: SquareTerminal },
    ],
  },
  {
    key: "operations",
    label: "Delivery & recovery",
    icon: Activity,
    panels: [
      { key: "runners", label: "Runners", icon: GitBranch },
      { key: "tasks", label: "Tasks", icon: Activity },
      { key: "logs", label: "Logs", icon: Terminal },
      { key: "releases", label: "Deployments", icon: History },
      { key: "release-groups", label: "Deployment groups", icon: Workflow },
      { key: "backups", label: "Backups & restore", icon: ArchiveRestore },
      { key: "connectors", label: "Backup destinations", icon: Database },
    ],
  },
  {
    key: "settings",
    label: "Settings",
    icon: Settings,
    panels: [
      { key: "settings", label: "Identity & encryption key", icon: Settings },
    ],
  },
] satisfies {
  key: string;
  label: string;
  icon: LucideIcon;
  panels: { key: string; label: string; icon: LucideIcon }[];
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
