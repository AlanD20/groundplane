export const serviceDestinations = [
  { key: "overview", label: "Overview" },
  { key: "logs", label: "Logs" },
  { key: "releases", label: "Deployments" },
  { key: "entries", label: "Variables & files" },
  { key: "storage", label: "Storage" },
  { key: "connections", label: "Backing connections" },
  { key: "configuration", label: "Runtime & image" },
  { key: "network", label: "Networking" },
  { key: "report", label: "Configuration report" },
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
  { key: "overview", label: "Overview" },
  { key: "logs", label: "Logs" },
  { key: "connections", label: "Backing connections" },
  { key: "backups", label: "Backups & restore" },
  { key: "service", label: "Runtime & image" },
  { key: "network", label: "Networking" },
  { key: "report", label: "Configuration report" },
] as const;
