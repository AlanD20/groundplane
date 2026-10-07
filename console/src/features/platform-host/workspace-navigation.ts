import {
  Activity,
  FileCode2,
  Info,
  List,
  Network,
  Package,
  Settings,
  Tag,
  Upload,
} from "lucide-react";

export const controllerDestinations = [
  { key: "overview", label: "Overview", icon: Info },
  { key: "updates", label: "Updates", icon: Upload },
  { key: "configuration", label: "Startup configuration", icon: FileCode2 },
];
export const coreDNSDestinations = [
  { key: "overview", label: "Overview", icon: Network },
  { key: "records", label: "DNS records", icon: List },
  { key: "configuration", label: "Resolver settings", icon: Settings },
];
export const imageDestinations = [
  { key: "overview", label: "Overview", icon: Package },
  { key: "usage", label: "Containers", icon: List },
  { key: "tags", label: "Tags & digests", icon: Tag },
  { key: "history", label: "Fetch history", icon: Activity },
];
