"use client";

import { Copy, Plus, Save } from "lucide-react";
import { useState } from "react";
import { useStore } from "@/lib/store";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { CodeEditor } from "@/components/ui/code-editor";

function resolverList(value: string): string[] {
  return value
    .trim()
    .split(/[\s,]+/)
    .filter(Boolean);
}

export function CoreDnsSettings({
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
  const [u, setU] = useState(upstream);
  const [auto, setAuto] = useState(upstreamAuto);
  const [template, setTemplate] = useState(corefileTemplate);
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
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between text-sm">
          <span>Settings</span>
          <StatusBadge status={enabled ? "healthy" : "stopped"} />
        </CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        {!configured ? (
          <p
            className="rounded-lg border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning"
            role="status"
          >
            Save a complete resolver configuration before enabling CoreDNS.
          </p>
        ) : null}
        <div className="flex items-center justify-between border-b border-border pb-3">
          <div className="flex flex-col">
            <span className="text-sm font-medium">Local resolver</span>
            <span className="text-xs text-muted-foreground">
              deployed by the Agent in groundplane-infra; the Controller renders
              the Corefile — reloads are graceful, zero-downtime, and a bad edit
              is rejected while the old instance keeps serving.
            </span>
          </div>
          <Switch
            checked={enabled}
            disabled={saving || !configured}
            onCheckedChange={(value) => void mutate(() => onEnabled(value))}
          />
        </div>
        <div className="grid grid-cols-2 gap-3">
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
        <div className="flex items-center justify-between border-b border-border pb-3">
          <div className="flex flex-col">
            <span className="text-sm font-medium">Auto upstream</span>
            <span className="text-xs text-muted-foreground">
              read the resolvers from the host&apos;s /etc/resolv.conf at render
              time — the input above is just a fallback preview. Off = pinned to
              the values you type.
            </span>
          </div>
          <Switch checked={auto} disabled={saving} onCheckedChange={setAuto} />
        </div>
        <div className="flex items-center justify-between border-b border-border py-3">
          <div className="flex flex-col">
            <span className="text-sm font-medium">Tailnet delegation</span>
            <span className="text-xs text-muted-foreground">
              forwards the tailnet domain (ts.net) to 100.100.100.100 so
              MagicDNS names resolve through the local resolver when Tailscale
              runs on the host
            </span>
          </div>
          <Switch
            checked={tailnetDelegation}
            disabled={saving}
            onCheckedChange={(value) => void mutate(() => onTailnet(value))}
          />
        </div>
        <div className="flex flex-col gap-2">
          <Label htmlFor="corefile-template">Template</Label>
          <CodeEditor
            id="corefile-template"
            label="Corefile template"
            value={template}
            onValueChange={setTemplate}
          />
          <span className="text-xs text-muted-foreground">
            Include exactly one{" "}
            <span className="font-mono">{"{groundplane}"}</span> marker. The
            Controller replaces it with bind, hosts, forwarders, catch-all, and
            reload directives.
          </span>
        </div>
        <div className="flex flex-col gap-2">
          <div className="flex items-center justify-between gap-3">
            <div>
              <Label htmlFor="rendered-corefile">
                Controller-rendered Corefile
              </Label>
              <p className="text-xs text-muted-foreground">
                Derived live from durable config, the host resolver baseline,
                and current host resolution.
              </p>
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
        </div>
        <div className="flex flex-col gap-2">
          <span className="text-sm font-medium">Domain forwarders</span>
          <span className="text-xs text-muted-foreground">
            per-zone routing: each domain is answered by its own resolvers
            (rendered as{" "}
            <span className="font-mono">
              forward &lt;domain&gt; &lt;resolvers&gt;
            </span>{" "}
            in the Corefile) before the catch-all. The tailnet delegation above
            is one of these, managed automatically.
          </span>
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
          <div className="grid grid-cols-[1fr_1fr_auto] items-end gap-2">
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
      </CardContent>
    </Card>
  );
}
