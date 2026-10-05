"use client";

import { DetailRow } from "@/components/common/detail-row";
import { PageHeader } from "@/components/common/page-header";
import {
  AdvancedDetails,
  SummaryItem,
  SummaryStrip,
  ResourcePanel,
} from "@/components/common/resource-panel";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsList, TabsTab, TabsPanel } from "@/components/ui/tabs";
import { environmentPlatformIngress } from "@/features/environment/platform-ingress";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import { ArrowLeft, Network, RefreshCw } from "lucide-react";
import { useEffect, useState } from "react";
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
  const [tab, setTab] = useState("overview");
  const {
    platform,
    tenantProjects,
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

  const { unavailable: environmentsWithoutComponentProjection } =
    environmentPlatformIngress(tenantProjects);
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

  if (platformComponentsLoading) {
    return (
      <div className="py-10 text-sm text-muted-foreground">
        Loading platform components…
      </div>
    );
  }
  if (platformComponentError) {
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
        icon={kindIcon[component.kind]}
        meta={<StatusBadge status={component.status} />}
      />

      <SummaryStrip>
        <SummaryItem label="Runtime">{component.runtime}</SummaryItem>
        <SummaryItem label="Network">
          {component.hostNetwork ? "Host network" : "Container network"}
        </SummaryItem>
        <SummaryItem label="Resolver">
          {platform.dns.enabled ? "Enabled" : "Disabled"}
        </SummaryItem>
        <SummaryItem label="Forwarders">
          {editableDNSConfig.forwarders.length}
        </SummaryItem>
      </SummaryStrip>
      <Tabs value={tab} onValueChange={(value) => setTab(String(value))}>
        <TabsList aria-label="CoreDNS sections">
          <TabsTab value="overview">Overview</TabsTab>
          <TabsTab value="records">DNS records</TabsTab>
          <TabsTab value="configuration">Configuration</TabsTab>
        </TabsList>
        <TabsPanel value="configuration" keepMounted className="pt-5">
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
        </TabsPanel>
        <TabsPanel value="records" keepMounted className="pt-5">
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
        </TabsPanel>
        <TabsPanel value="overview" className="space-y-5 pt-5">
          <ResourcePanel title="Resolver runtime">
            <DetailRow label="Runtime" value={component.runtime} />
            <DetailRow
              label="Network"
              value={
                component.hostNetwork ? "Host network" : "Container network"
              }
            />
            <DetailRow
              label="Local resolver"
              value={platform.dns.enabled ? "Enabled" : "Disabled"}
            />
          </ResourcePanel>
          <AdvancedDetails>
            <DetailRow label="Component ID" value={component.id} mono />
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
          </AdvancedDetails>
        </TabsPanel>
      </Tabs>

      {environmentsWithoutComponentProjection > 0 && (
        <p
          className="rounded-lg border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning"
          role="status"
        >
          Ingress for {environmentsWithoutComponentProjection} Environment
          {environmentsWithoutComponentProjection === 1 ? "" : "s"} is
          unavailable because the Controller did not publish an authoritative
          Component projection.
        </p>
      )}
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
