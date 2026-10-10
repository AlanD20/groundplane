import { useSearchParams } from "react-router-dom";
import { SettingsContent } from "@/components/common/settings-content";
import { SoftwareUpdates } from "@/features/platform-settings/software-updates";
import { settingsDestination } from "@/features/platform-settings/workspace-navigation";

export default function PlatformSettingsPage() {
  const [search] = useSearchParams();
  return settingsDestination(search) === "updates" ? (
    <SoftwareUpdates />
  ) : (
    <SettingsContent workspace="platform" />
  );
}
