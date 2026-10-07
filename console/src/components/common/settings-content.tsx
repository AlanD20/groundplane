import { PageHeader } from "@/components/common/page-header";
import { ResourcePanel, SettingsRow } from "@/components/common/resource-panel";
import { Switch } from "@/components/ui/switch";
import { Select } from "@/components/ui/select";
import { tablePageSizes } from "@/lib/console-preferences";
import { useStore } from "@/lib/store";
import { Settings } from "lucide-react";
import { ThemeToggle } from "./theme-toggle";

export function SettingsContent({
  workspace,
}: {
  workspace: "platform" | string;
}) {
  const {
    requireRevealConfirm,
    setRequireRevealConfirm,
    showEmptySecretBadges,
    setShowEmptySecretBadges,
    defaultTablePageSize,
    setDefaultTablePageSize,
  } = useStore();
  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow={
          workspace === "platform" ? "Platform" : `Tenant · ${workspace}`
        }
        title="Console preferences"
        description="Display and secret-reveal preferences apply immediately in this browser."
        icon={<Settings />}
      />
      <ResourcePanel title="Visualization">
        <div>
          <SettingsRow title="Appearance">
            <ThemeToggle />
          </SettingsRow>
          <SettingsRow
            title="Default rows per page"
            help="Used by paginated tables, including Tasks. Each table can use a different size."
          >
            <Select
              aria-label="Default rows per page"
              className="w-24"
              value={String(defaultTablePageSize)}
              options={tablePageSizes.map((size) => ({
                value: String(size),
                label: String(size),
              }))}
              onValueChange={(value) => {
                const size = tablePageSizes.find(
                  (size) => String(size) === value,
                );
                if (size !== undefined) setDefaultTablePageSize(size);
              }}
            />
          </SettingsRow>
          <SettingsRow
            title="Show empty-value secret badges"
            help="Mark secrets with no value in Entries and Blueprint reviews."
          >
            <Switch
              aria-label="Show empty-value secret badges"
              checked={showEmptySecretBadges}
              onCheckedChange={setShowEmptySecretBadges}
            />
          </SettingsRow>
        </div>
      </ResourcePanel>
      <ResourcePanel title="Secret reveal">
        <div>
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
