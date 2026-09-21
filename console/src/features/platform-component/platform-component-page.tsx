"use client";

import { useRequiredParams } from "@/lib/router";
import { Link } from "react-router-dom";
import { ArrowLeft, Network, RefreshCw } from "lucide-react";
import { useEffect } from "react";
import { useStore } from "@/lib/store";
import { environmentPlatformIngress } from "@/features/environment/platform-ingress";
import { PageHeader } from "@/components/common/page-header";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { StatusBadge } from "@/components/common/status-badge";
import { MetaPill } from "@/components/common/meta-pill";
import { Button } from "@/components/ui/button";
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
        description={component.runtime}
        icon={kindIcon[component.kind]}
        meta={
          <>
            <MetaPill icon={kindIcon[component.kind]}>
              {component.name}
            </MetaPill>
            <MetaPill icon={<RefreshCw />}>
              {component.image}:{component.version}
            </MetaPill>
            <StatusBadge status={component.status} />
          </>
        }
      />

      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center justify-between text-sm">
              <span className="flex items-center gap-2">
                {kindIcon[component.kind]}
                {component.name}
              </span>
              <StatusBadge status={component.status} />
            </CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-1.5 text-sm">
            <Row
              label="Image"
              value={`${component.image}:${component.version}`}
              mono
            />
            <Row label="Runtime" value={component.runtime} />
            {component.hostNetwork ? (
              <Row label="Network" value="host network" mono />
            ) : null}
            <div className="mt-1">
              <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground/70">
                Mounts
              </span>
              {component.mounts.map((m) => (
                <div
                  key={m}
                  className="font-mono text-xs text-muted-foreground"
                >
                  {m}
                </div>
              ))}
            </div>
            <div className="mt-1 flex flex-col gap-0.5">
              {component.notes.map((n) => (
                <span key={n} className="text-xs text-muted-foreground">
                  · {n}
                </span>
              ))}
            </div>
          </CardContent>
        </Card>

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

function Row({
  label,
  value,
  mono,
}: {
  label: string;
  value: React.ReactNode;
  mono?: boolean;
}) {
  return (
    <div className="flex items-center justify-between gap-2">
      <span className="text-muted-foreground">{label}</span>
      <span className={mono ? "font-mono text-xs" : "text-xs"}>{value}</span>
    </div>
  );
}
