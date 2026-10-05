import { PageHeader } from "@/components/common/page-header";
import { ResourcePanel, SettingsRow } from "@/components/common/resource-panel";
import { Switch } from "@/components/ui/switch";
import { Button } from "@/components/ui/button";
import { useStore } from "@/lib/store";
import { Settings, ChevronRight } from "lucide-react";
import { Link } from "react-router-dom";
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
      {workspace === "platform" && (
        <ResourcePanel title="Platform configuration">
          <div className="grid gap-3 sm:grid-cols-2">
            {[
              ["Controller", "/platform/host/controller"],
              ["etcd", "/platform/host/etcd"],
              ["DNS and Components", "/platform/components"],
              ["Platform Secrets", "/platform/secrets"],
            ].map(([label, href]) => (
              <Button
                key={href}
                nativeButton={false}
                variant="outline"
                render={<Link to={href} />}
                className="justify-between"
              >
                {label}
                <ChevronRight />
              </Button>
            ))}
          </div>
        </ResourcePanel>
      )}
    </div>
  );
}
