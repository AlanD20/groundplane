import {
  Layers,
  Terminal,
  History,
  Braces,
  HardDrive,
  Plug,
  Container,
  Network,
  FileCode2,
  ArchiveRestore,
} from "lucide-react";

export const serviceDestinations = [
  { key: "overview", label: "Overview", icon: Layers },
  { key: "logs", label: "Logs", icon: Terminal },
  { key: "releases", label: "Deployments", icon: History },
  { key: "entries", label: "Variables & files", icon: Braces },
  { key: "storage", label: "Storage", icon: HardDrive },
  { key: "connections", label: "Backing connections", icon: Plug },
  { key: "configuration", label: "Runtime & image", icon: Container },
  { key: "network", label: "Networking", icon: Network },
  { key: "report", label: "Configuration report", icon: FileCode2 },
] as const;

export type ServiceDestination = (typeof serviceDestinations)[number]["key"];
export function serviceDestination(
  search: URLSearchParams,
): ServiceDestination {
  return (
    serviceDestinations.find((item) => item.key === search.get("serviceTab"))
      ?.key ?? "overview"
  );
}

export const backingDestinations = [
  { key: "overview", label: "Overview", icon: Layers },
  { key: "logs", label: "Logs", icon: Terminal },
  { key: "connections", label: "Backing connections", icon: Plug },
  { key: "backups", label: "Backups & restore", icon: ArchiveRestore },
  { key: "service", label: "Runtime & image", icon: Container },
  { key: "network", label: "Networking", icon: Network },
  { key: "report", label: "Configuration report", icon: FileCode2 },
] as const;
