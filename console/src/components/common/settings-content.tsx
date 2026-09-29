"use client";

import { PageHeader } from "@/components/common/page-header";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { useStore } from "@/lib/store";
import { Eye, Globe, Settings as SettingsIcon, Sun } from "lucide-react";
import { ThemeToggle } from "./theme-toggle";

// Settings stays small, on purpose: everything else lives in desired state.
// Platform component settings (DNS resolver, Agent config) live on the
// Components page, per component.
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
        description="Console appearance and sensitive-value display preferences."
        icon={<SettingsIcon />}
      />
      <Card>
        <CardHeader>
          <CardTitle>Preferences</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col">
          <div className="flex items-center justify-between border-b border-border py-3">
            <div className="flex items-center gap-2.5">
              <Sun className="size-4 text-muted-foreground" />
              <div className="flex flex-col">
                <span className="text-sm font-medium">Theme</span>
                <span className="text-xs text-muted-foreground">
                  Choose the light or dark workspace.
                </span>
              </div>
            </div>
            <ThemeToggle />
          </div>
          <div className="flex items-center justify-between py-3">
            <div className="flex items-center gap-2.5">
              <Eye className="size-4 text-muted-foreground" />
              <div className="flex flex-col">
                <span className="text-sm font-medium">
                  Require typed confirmation to reveal secrets
                </span>
                <span className="text-xs text-muted-foreground">
                  Ask for confirmation before showing a sensitive value.
                  Revealed values are never cached or logged.
                </span>
              </div>
            </div>
            <Switch
              checked={requireRevealConfirm}
              onCheckedChange={setRequireRevealConfirm}
            />
          </div>
          <div className="flex items-center justify-between border-t border-border py-3">
            <div className="flex items-center gap-2.5">
              <Globe className="size-4 text-muted-foreground" />
              <div className="flex flex-col">
                <span className="text-sm font-medium">Routing</span>
                <span className="text-xs text-muted-foreground">
                  Enable Caddy or Cloudflare Tunnel in an Environment&apos;s
                  Network → Router view. Creating a Route does not enable a
                  Router automatically.
                </span>
              </div>
            </div>
            <span className="font-mono text-xs text-muted-foreground">
              opt-in
            </span>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
