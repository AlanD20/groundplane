import { PageHeader } from "@/components/common/page-header";
import { DetailRow } from "@/components/common/detail-row";
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
        description={
          workspace === "platform"
            ? "Platform configuration stays with the component that owns it."
            : "Console preferences for this workspace."
        }
        icon={<Settings />}
      />
      {workspace === "platform" && (
        <>
          <div className="grid items-start gap-6 lg:grid-cols-2">
            <ResourcePanel title="Control plane">
              <div className="flex flex-col gap-2">
                {[
                  ["Controller configuration", "/platform/host/controller"],
                  ["etcd configuration", "/platform/host/etcd"],
                  ["Agent and host", "/platform/host"],
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
            <ResourcePanel title="Platform resources">
              <div className="flex flex-col gap-2">
                {[
                  ["Secret references", "/platform/secrets"],
                  ["DNS and Components", "/platform/components"],
                  ["Backing services", "/platform/backing-services"],
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
          </div>
          <ResourcePanel title="Configuration ownership">
            <div className="grid gap-x-8 sm:grid-cols-2">
              <DetailRow label="Blueprint and age key" value="Environment" />
              <DetailRow
                label="Backup Connectors and policies"
                value="Environment"
              />
              <DetailRow
                label="Shared service credentials"
                value="Backing adapter and consumer Attach"
              />
              <DetailRow label="Controller and etcd YAML" value="Host" />
              <DetailRow
                label="Platform Secret references"
                value="Settings / Secrets"
              />
            </div>
          </ResourcePanel>
        </>
      )}
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
