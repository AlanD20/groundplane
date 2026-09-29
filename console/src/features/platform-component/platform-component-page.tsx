"use client";

import { DetailRow } from "@/components/common/detail-row";
import { PageHeader } from "@/components/common/page-header";
import {
  AdvancedDetails,
  SummaryItem,
  SummaryStrip,
} from "@/components/common/resource-panel";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { environmentPlatformIngress } from "@/features/environment/platform-ingress";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import { ArrowLeft, Network, RefreshCw } from "lucide-react";
import { useEffect } from "react";
import { Link } from "react-router-dom";
import { CoreDnsSettings } from "./coredns-settings";
const kindIcon: Record<string, React.ReactNode> = {
  coredns: <Network className="size-4 text-muted-foreground" />,
};

export default function PlatformComponentPage() {
  const params = useRequiredParams("component");
  const {
    platform,
    tenantProjects,
    platformComponentsLoading,
    platformComponentError,
    managedConfigFiles,
    managedConfigLoading,
    managedConfigError,
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
        }
      : undefined;
  const editableDNSConfig = dnsConfig ?? {
    upstream: "",
    upstreamAuto: false,
    tailnetDelegation: false,
    corefileTemplate: "",
    forwarders: [],
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
      <div>
        <CoreDnsSettings
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
  upstream?: string;
  upstreamAuto?: boolean;
  tailnetDelegation?: boolean;
  corefileTemplate?: string;
  forwarders?: { domain: string; upstream: string }[];
};

function resolverList(value: string): string[] {
  return value
    .trim()
    .split(/[\s,]+/)
    .filter(Boolean);
}

async function replaceCoreDNSConfig(
  updateComponentConfig: ReturnType<typeof useStore>["updateComponentConfig"],
  refreshPlatformComponents: ReturnType<
    typeof useStore
  >["refreshPlatformComponents"],
  refreshComponentConfig: ReturnType<typeof useStore>["refreshComponentConfig"],
  componentId: string,
  current: {
    upstream: string;
    upstreamAuto: boolean;
    tailnetDelegation: boolean;
    corefileTemplate: string;
    forwarders: { domain: string; upstream: string }[];
  },
  update: CoreDNSConfigUpdate,
) {
  const next = { ...current, ...update };
  await updateComponentConfig(componentId, {
    upstream_auto: next.upstreamAuto,
    upstream_resolvers: next.upstreamAuto ? [] : resolverList(next.upstream),
    forwarders: next.forwarders.map((forwarder) => ({
      domain: forwarder.domain,
      resolvers: resolverList(forwarder.upstream),
    })),
    tailnet_delegation: next.tailnetDelegation,
    corefile_template: next.corefileTemplate,
  });
  await refreshPlatformComponents();
  await refreshComponentConfig(componentId);
}
