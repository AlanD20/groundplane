import { PageHeader } from "@/components/common/page-header";
import { ResourcePanel, SettingsRow } from "@/components/common/resource-panel";
import { Switch } from "@/components/ui/switch";
import { useStore } from "@/lib/store";
import { Settings } from "lucide-react";
import { ThemeToggle } from "./theme-toggle";

export function SettingsContent({
  workspace,
}: {
  workspace: "platform" | string;
}) {
  const { requireRevealConfirm, setRequireRevealConfirm } = useStore();
  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow={
          workspace === "platform" ? "Platform" : `Tenant · ${workspace}`
        }
        title="Settings"
        icon={<Settings />}
      />
      <ResourcePanel title="Console preferences">
        <div>
          <SettingsRow title="Appearance">
            <ThemeToggle />
          </SettingsRow>
          <SettingsRow
            title="Confirm before revealing secrets"
            help="Require typed confirmation before showing a sensitive value. Revealed values are not cached or logged."
          >
            <Switch
              aria-label="Confirm before revealing secrets"
              checked={requireRevealConfirm}
              onCheckedChange={setRequireRevealConfirm}
            />
          </SettingsRow>
        </div>
      </ResourcePanel>
    </div>
  );
}
