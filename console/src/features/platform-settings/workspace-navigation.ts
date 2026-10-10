import { Settings, Download } from "lucide-react";

export const settingsDestinations = [
  { key: "preferences", label: "Console preferences", icon: Settings },
  { key: "updates", label: "Software updates", icon: Download },
];
export function settingsDestination(search: URLSearchParams) {
  return search.get("tab") === "updates" ? "updates" : "preferences";
}
