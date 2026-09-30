"use client";

import {
  AdvancedDetails,
  HelpHint,
  ResourcePanel,
  SettingsRow,
} from "@/components/common/resource-panel";
import { Button } from "@/components/ui/button";
import { CodeEditor } from "@/components/ui/code-editor";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { useStore } from "@/lib/store";
import { useDraftField } from "@/lib/use-draft-field";
import { TaskLink } from "@/components/common/task-link";
import { Copy, Plus, Save } from "lucide-react";
import { useState } from "react";

export function CoreDnsSettings({
  activeTaskId,
  upstream,
  upstreamAuto,
  tailnetDelegation,
  corefileTemplate,
  forwarders,
  enabled,
  configured,
  managedFiles,
  managedConfigLoading,
  managedConfigError,
  onRefreshManagedConfig,
  onEnabled,
  onTailnet,
  onAddForwarder,
  onRemoveForwarder,
  onSave,
}: {
  activeTaskId: string | null;
  upstream: string;
  upstreamAuto: boolean;
  tailnetDelegation: boolean;
  corefileTemplate: string;
  forwarders: { domain: string; upstream: string }[];
  enabled: boolean;
  configured: boolean;
  managedFiles: ReturnType<typeof useStore>["managedConfigFiles"];
  managedConfigLoading: boolean;
  managedConfigError: string | null;
  onRefreshManagedConfig: () => Promise<unknown>;
  onEnabled: (v: boolean) => Promise<void>;
  onTailnet: (v: boolean) => Promise<void>;
  onAddForwarder: (domain: string, upstream: string) => Promise<void>;
  onRemoveForwarder: (index: number) => Promise<void>;
  onSave: (
    upstream: string,
    upstreamAuto: boolean,
    corefileTemplate: string,
  ) => Promise<void>;
}) {
  const [u, setU] = useDraftField(upstream);
  const [auto, setAuto] = useDraftField(upstreamAuto);
  const [template, setTemplate] = useDraftField(corefileTemplate);
  const [fwdDomain, setFwdDomain] = useState("");
  const [fwdUpstream, setFwdUpstream] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const managedCorefile = managedFiles.find(
    (file) => file.path === "/etc/groundplane/coredns/Corefile",
  );

  async function mutate(action: () => Promise<void>) {
    setSaving(true);
    setError(null);
    try {
      await action();
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "Unable to update CoreDNS",
      );
    } finally {
      setSaving(false);
    }
  }
  async function copyRenderedCorefile() {
    if (!managedCorefile) return;
    setError(null);
    try {
      await navigator.clipboard.writeText(managedCorefile.rendered);
      setCopied(true);
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : "Unable to copy rendered Corefile",
      );
    }
  }
  return (
    <ResourcePanel title="Resolver configuration">
      {activeTaskId && (
        <div
          role="status"
          className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-primary/20 bg-primary/5 p-3 text-xs"
        >
          <span>
            A Task owns this resolver configuration. Updates resume when it
            releases ownership.
          </span>
          <TaskLink taskId={activeTaskId}>Open Task</TaskLink>
        </div>
      )}
      <fieldset disabled={saving || !!activeTaskId} className="contents">
        {!configured ? (
          <p
            className="rounded-lg border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning"
            role="status"
          >
            Save a complete resolver configuration before enabling CoreDNS.
          </p>
        ) : null}
        <SettingsRow
          title="Local resolver"
          help="Enable CoreDNS on this host using the saved configuration."
        >
          <Switch
            aria-label="Local resolver"
            checked={enabled}
            disabled={saving || !configured}
            onCheckedChange={(value) => void mutate(() => onEnabled(value))}
          />
        </SettingsRow>
        <div className="grid gap-3 sm:grid-cols-2">
          <div className="flex flex-col gap-1">
            <Label htmlFor="dns-upstream">Catch-all upstream</Label>
            <Input
              id="dns-upstream"
              value={u}
              onChange={(e) => setU(e.target.value)}
              className="font-mono"
              disabled={auto}
              placeholder="1.1.1.1 8.8.8.8"
            />
          </div>
        </div>
        <SettingsRow
          title="Use host upstream resolvers"
          help="Read resolvers from the host at render time. Turn off to use the catch-all addresses you enter."
        >
          <Switch
            aria-label="Use host upstream resolvers"
            checked={auto}
            disabled={saving}
            onCheckedChange={setAuto}
          />
        </SettingsRow>
        <SettingsRow
          title="Tailnet delegation"
          help="Forward ts.net queries to Tailscale MagicDNS at 100.100.100.100 when Tailscale runs on this host."
        >
          <Switch
            aria-label="Tailnet delegation"
            checked={tailnetDelegation}
            disabled={saving}
            onCheckedChange={(value) => void mutate(() => onTailnet(value))}
          />
        </SettingsRow>
        <AdvancedDetails title="Corefile template">
          <CodeEditor
            id="corefile-template"
            label="Corefile template"
            value={template}
            onValueChange={setTemplate}
          />
          <span className="text-xs text-muted-foreground">
            Include exactly one{" "}
            <span className="font-mono">{"{groundplane}"}</span> marker. The
            Controller replaces it with the listener addresses, hostname
            records, domain forwarders, catch-all resolvers and reload directive
            shown below.
          </span>
        </AdvancedDetails>
        <AdvancedDetails title="Generated DNS directives">
          {managedCorefile?.generatedDirectives ? (
            <CodeEditor
              id="generated-dns-directives"
              label="Exact replacement for {groundplane}"
              value={managedCorefile.generatedDirectives}
              readOnly
            />
          ) : (
            <p className="text-xs text-muted-foreground">
              Load the desired Corefile preview to see the exact replacement.
            </p>
          )}
        </AdvancedDetails>
        <AdvancedDetails title="Desired Corefile preview">
          <div className="flex items-center justify-between gap-3">
            <div>
              <Label htmlFor="rendered-corefile">
                Desired Corefile, not runtime observation
              </Label>
            </div>
            <Button
              variant="outline"
              size="sm"
              disabled={!managedCorefile || managedConfigLoading}
              onClick={() => void copyRenderedCorefile()}
            >
              <Copy className="size-4" /> {copied ? "Copied" : "Copy"}
            </Button>
          </div>
          {managedConfigLoading ? (
            <p className="text-xs text-muted-foreground" role="status">
              Loading rendered Corefile…
            </p>
          ) : null}
          {managedConfigError ? (
            <div className="flex items-center justify-between gap-3 rounded-lg border border-destructive/30 px-3 py-2">
              <p className="text-xs text-destructive" role="alert">
                {managedConfigError}
              </p>
              <Button
                variant="outline"
                size="sm"
                onClick={() => void onRefreshManagedConfig()}
              >
                Retry
              </Button>
            </div>
          ) : null}
          {!managedConfigLoading && !managedConfigError && managedCorefile ? (
            <CodeEditor
              id="rendered-corefile"
              label="Controller-rendered Corefile"
              value={managedCorefile.rendered}
              readOnly
            />
          ) : null}
          {!managedConfigLoading && !managedConfigError && !managedCorefile ? (
            <p className="text-xs text-muted-foreground">
              No managed file preview is available.
            </p>
          ) : null}
        </AdvancedDetails>
        <div className="flex flex-col gap-2">
          <h3 className="flex items-center gap-2 text-sm font-medium">
            Domain forwarders{" "}
            <HelpHint label="About Domain forwarders">
              Send queries for each domain to its selected resolvers before the
              catch-all.
            </HelpHint>
          </h3>
          <div className="flex flex-col gap-1.5">
            {forwarders.map((f, index) => (
              <div
                key={f.domain}
                className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2"
              >
                <div className="flex items-center gap-2 font-mono text-xs">
                  <span className="text-primary">{f.domain}</span>
                  <span className="text-muted-foreground">→</span>
                  <span className="text-muted-foreground">{f.upstream}</span>
                </div>
                <Button
                  variant="ghost"
                  size="sm"
                  disabled={saving}
                  onClick={() => void mutate(() => onRemoveForwarder(index))}
                >
                  Remove
                </Button>
              </div>
            ))}
            {forwarders.length === 0 && (
              <div className="text-xs text-muted-foreground">
                no domain forwarders — everything goes to the catch-all
              </div>
            )}
          </div>
          <div className="grid items-end gap-2 sm:grid-cols-[1fr_1fr_auto]">
            <div className="flex flex-col gap-1">
              <Label htmlFor="fwd-domain">Domain</Label>
              <Input
                id="fwd-domain"
                value={fwdDomain}
                onChange={(e) => setFwdDomain(e.target.value)}
                className="font-mono"
                placeholder="home.arpa"
              />
            </div>
            <div className="flex flex-col gap-1">
              <Label htmlFor="fwd-upstream">Resolvers</Label>
              <Input
                id="fwd-upstream"
                value={fwdUpstream}
                onChange={(e) => setFwdUpstream(e.target.value)}
                className="font-mono"
                placeholder="192.168.1.1 10.0.0.53"
              />
            </div>
            <Button
              size="sm"
              disabled={saving || !fwdDomain.trim() || !fwdUpstream.trim()}
              onClick={() =>
                void mutate(async () => {
                  await onAddForwarder(fwdDomain.trim(), fwdUpstream.trim());
                  setFwdDomain("");
                  setFwdUpstream("");
                })
              }
            >
              <Plus className="size-4" /> Add
            </Button>
          </div>
        </div>
        {error ? (
          <p className="text-xs text-destructive" role="alert">
            {error}
          </p>
        ) : null}
        <Button
          size="sm"
          disabled={saving}
          onClick={() => void mutate(() => onSave(u, auto, template))}
        >
          <Save className="size-4" /> Save
        </Button>
      </fieldset>
    </ResourcePanel>
  );
}
