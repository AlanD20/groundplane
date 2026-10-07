import { CompactReference } from "@/components/common/compact-reference";
("use client");

import { useSearchParams } from "react-router-dom";

import { DetailRow } from "@/components/common/detail-row";
import { PageHeader } from "@/components/common/page-header";
import {
  SummaryItem,
  SummaryStrip,
  ResourcePanel,
} from "@/components/common/resource-panel";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import { ArrowLeft, Network, RefreshCw } from "lucide-react";
import { useEffect } from "react";
import { Link } from "react-router-dom";
import { CoreDnsSettings } from "./coredns-settings";
import { DNSRecordSettings } from "./dns-record-settings";
import { coreDNSConfigInput, type CoreDNSDraft } from "./core-dns-config";
import type { DNSRecord } from "@/lib/types";
const kindIcon: Record<string, React.ReactNode> = {
  coredns: <Network className="size-4 text-muted-foreground" />,
};

export default function PlatformComponentPage() {
  const params = useRequiredParams("component");
  const [search] = useSearchParams();
  const tab = ["overview", "records", "configuration"].includes(
    search.get("tab") ?? "",
  )
    ? search.get("tab")!
    : "overview";
  const destination = (value: string) => {
    const next = new URLSearchParams(search);
    next.set("tab", value);
    return `?${next}`;
  };
  const {
    platform,
    platformComponentsLoading,
    platformComponentError,
    managedConfigFiles,
    managedConfigLoading,
    managedConfigError,
    managedConfigTaskId,
    managedConfigComponentId,
    setComponentEnabled,
    updateComponentConfig,
    refreshPlatformComponents,
    refreshComponentConfig,
  } = useStore();
  const component = platform.components.find(
    (c) => c.kind === params.component,
  );
  const componentId = component?.kind === "coredns" ? component.id : undefined;

  useEffect(() => {
    if (!componentId) return;
    const controller = new AbortController();
    void refreshComponentConfig(componentId, controller.signal).catch(
      () => undefined,
    );
    return () => controller.abort();
  }, [componentId, refreshComponentConfig]);

  useEffect(() => {
    if (
      !componentId ||
      managedConfigComponentId !== componentId ||
      !managedConfigTaskId
    )
      return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const poll = async () => {
      try {
        await refreshComponentConfig(componentId, controller.signal);
        await refreshPlatformComponents(controller.signal);
      } catch {
        /* Refresh actions expose their errors without discarding drafts. */
      }
      if (!controller.signal.aborted)
        timer = setTimeout(() => void poll(), 2000);
    };
    timer = setTimeout(() => void poll(), 2000);
    return () => {
      controller.abort();
      if (timer) clearTimeout(timer);
    };
  }, [
    componentId,
    managedConfigComponentId,
    managedConfigTaskId,
    refreshComponentConfig,
    refreshPlatformComponents,
  ]);

  const dnsConfig =
    platform.dns.upstream !== undefined &&
    platform.dns.upstreamAuto !== undefined &&
    platform.dns.tailnetDelegation !== undefined &&
    platform.dns.forwarders !== undefined &&
    platform.dns.corefileTemplate !== undefined
      ? {
          upstream: platform.dns.upstream,
          upstreamAuto: platform.dns.upstreamAuto,
          tailnetDelegation: platform.dns.tailnetDelegation,
          corefileTemplate: platform.dns.corefileTemplate,
          forwarders: platform.dns.forwarders,
          records: platform.dns.records ?? [],
        }
      : undefined;
  const editableDNSConfig = dnsConfig ?? {
    upstream: "",
    upstreamAuto: false,
    tailnetDelegation: false,
    corefileTemplate: "",
    forwarders: [],
    records: [],
  };

  if (platformComponentsLoading && !component) {
    return (
      <div className="py-10 text-sm text-muted-foreground">
        Loading platform components…
      </div>
    );
  }
  if (platformComponentError && !component) {
    return (
      <div className="flex flex-col items-start gap-4 py-10">
        <p className="text-sm text-destructive">{platformComponentError}</p>
        <Button
          variant="outline"
          onClick={() => void refreshPlatformComponents()}
        >
          <RefreshCw className="size-4" /> Retry
        </Button>
      </div>
    );
  }
  if (!component || component.kind !== "coredns") {
    return (
      <div className="flex flex-col items-start gap-4 py-10">
        <p className="text-sm text-muted-foreground">
          Unknown platform component.
        </p>
        <Link
          to="/platform/components"
          className="inline-flex h-8 items-center gap-1.5 rounded-lg border border-border bg-background px-2.5 text-sm font-medium hover:bg-muted hover:text-foreground dark:border-input dark:bg-input/30 dark:hover:bg-input/50"
        >
          <ArrowLeft className="size-4" /> Back to Components
        </Link>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-6">
      <div className="flex items-center justify-between">
        <Link
          to="/platform/components"
          className="inline-flex items-center gap-1.5 text-xs font-medium text-muted-foreground transition-colors hover:text-foreground"
        >
          <ArrowLeft className="size-3.5" /> Components
        </Link>
      </div>
      <PageHeader
        title={component.name}
        description="Shared host DNS, custom records and upstream forwarding."
        icon={kindIcon[component.kind]}
        meta={<StatusBadge status={component.status} />}
      />

      <div className="rounded-lg border border-border bg-card p-4">
        <CompactReference value={component.id} label="Component ID" />
      </div>
      {platformComponentError && (
        <p role="alert" className="text-sm text-warning">
          {platformComponentError}
        </p>
      )}
      <div hidden={tab !== "configuration"}>
        <CoreDnsSettings
          activeTaskId={
            managedConfigComponentId === component.id
              ? managedConfigTaskId
              : null
          }
          upstream={editableDNSConfig.upstream}
          upstreamAuto={editableDNSConfig.upstreamAuto}
          tailnetDelegation={editableDNSConfig.tailnetDelegation}
          corefileTemplate={editableDNSConfig.corefileTemplate}
          forwarders={editableDNSConfig.forwarders}
          enabled={platform.dns.enabled}
          configured={dnsConfig !== undefined}
          managedFiles={managedConfigFiles}
          managedConfigLoading={managedConfigLoading}
          managedConfigError={managedConfigError}
          onRefreshManagedConfig={() => refreshComponentConfig(component.id)}
          onEnabled={async (enabled) => {
            await setComponentEnabled(component.id, enabled);
            await refreshComponentConfig(component.id);
            await refreshPlatformComponents();
          }}
          onTailnet={(tailnetDelegation) =>
            replaceCoreDNSConfig(
              updateComponentConfig,
              refreshPlatformComponents,
              refreshComponentConfig,
              component.id,
              editableDNSConfig,
              { tailnetDelegation },
            )
          }
          onAddForwarder={(domain, upstream) =>
            replaceCoreDNSConfig(
              updateComponentConfig,
              refreshPlatformComponents,
              refreshComponentConfig,
              component.id,
              editableDNSConfig,
              {
                forwarders: [
                  ...editableDNSConfig.forwarders,
                  { domain, upstream },
                ],
              },
            )
          }
          onRemoveForwarder={(index) =>
            replaceCoreDNSConfig(
              updateComponentConfig,
              refreshPlatformComponents,
              refreshComponentConfig,
              component.id,
              editableDNSConfig,
              {
                forwarders: editableDNSConfig.forwarders.filter(
                  (_, candidateIndex) => candidateIndex !== index,
                ),
              },
            )
          }
          onSave={(upstream, upstreamAuto, corefileTemplate) =>
            replaceCoreDNSConfig(
              updateComponentConfig,
              refreshPlatformComponents,
              refreshComponentConfig,
              component.id,
              editableDNSConfig,
              { upstream, upstreamAuto, corefileTemplate },
            )
          }
        />
      </div>
      <div hidden={tab !== "records"}>
        <DNSRecordSettings
          records={editableDNSConfig.records}
          disabled={
            managedConfigComponentId === component.id && !!managedConfigTaskId
          }
          onChange={(records) =>
            replaceCoreDNSConfig(
              updateComponentConfig,
              refreshPlatformComponents,
              refreshComponentConfig,
              component.id,
              editableDNSConfig,
              { records },
            )
          }
        />
      </div>
      <div hidden={tab !== "overview"} className="space-y-5">
        <SummaryStrip>
          <SummaryItem label="Resolver">
            {platform.dns.enabled ? "Enabled" : "Disabled"}
          </SummaryItem>
          <SummaryItem label="DNS records">
            {dnsConfig ? dnsConfig.records.length : "Unavailable"}
          </SummaryItem>
          <SummaryItem label="Forwarding rules">
            {dnsConfig ? dnsConfig.forwarders.length : "Unavailable"}
          </SummaryItem>
          <SummaryItem label="Network">
            {component.hostNetwork ? "Host" : "Container"}
          </SummaryItem>
        </SummaryStrip>
        <ResourcePanel
          title="DNS resolution"
          actions={
            <Link
              to={destination("configuration")}
              className="text-sm text-primary hover:underline"
            >
              Resolver settings →
            </Link>
          }
        >
          <DetailRow
            label="Default upstream"
            value={
              !dnsConfig
                ? "Unavailable"
                : dnsConfig.upstreamAuto
                  ? "Automatic"
                  : dnsConfig.upstream || "Not set"
            }
          />
          <DetailRow
            label="Tailnet delegation"
            value={
              !dnsConfig
                ? "Unavailable"
                : dnsConfig.tailnetDelegation
                  ? "Enabled"
                  : "Disabled"
            }
          />
          <DetailRow label="Runtime" value={component.runtime} />
        </ResourcePanel>
        <ResourcePanel
          title="Custom DNS records"
          actions={
            <Link
              to={destination("records")}
              className="text-sm text-primary hover:underline"
            >
              Manage records →
            </Link>
          }
        >
          <p className="text-sm text-muted-foreground">
            {!dnsConfig
              ? "DNS configuration is unavailable."
              : dnsConfig.records.length
                ? `${dnsConfig.records.length} custom records configured for this host.`
                : "No custom records. Add a record to resolve a name to a Service or address."}
          </p>
        </ResourcePanel>
        <ResourcePanel title="Storage & runtime notes">
          <DetailRow
            label="Mounts"
            value={component.mounts.join(", ") || "None"}
            mono
          />
          {component.notes.map((note) => (
            <p key={note} className="text-muted-foreground">
              {note}
            </p>
          ))}
        </ResourcePanel>
      </div>
    </div>
  );
}

type CoreDNSConfigUpdate = {
  records?: DNSRecord[];
  upstream?: string;
  upstreamAuto?: boolean;
  tailnetDelegation?: boolean;
  corefileTemplate?: string;
  forwarders?: { domain: string; upstream: string }[];
};

async function replaceCoreDNSConfig(
  updateComponentConfig: ReturnType<typeof useStore>["updateComponentConfig"],
  refreshPlatformComponents: ReturnType<
    typeof useStore
  >["refreshPlatformComponents"],
  refreshComponentConfig: ReturnType<typeof useStore>["refreshComponentConfig"],
  componentId: string,
  current: CoreDNSDraft,
  update: CoreDNSConfigUpdate,
) {
  const next = { ...current, ...update };
  await updateComponentConfig(componentId, coreDNSConfigInput(next));
  await refreshPlatformComponents();
  await refreshComponentConfig(componentId);
}
