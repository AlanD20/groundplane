import { InlineEditorRegion } from "@/components/common/inline-editor-region";
import { CompactReference } from "@/components/common/compact-reference";
import { workspaceSectionClassName } from "@/components/common/workspace-section";
("use client");

import { useState } from "react";
import { useRequiredParams } from "@/lib/router";
import { Router as RouterIcon } from "lucide-react";
import { useStore } from "@/lib/store";
import { EnvironmentRouterUnavailable } from "@/features/environment/router-unavailable";
import { ContainerSoftwareIdentity } from "@/features/image-delivery/software-identity";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { Select } from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { FormSection } from "@/components/ui/form-section";
import { AdvancedDetails } from "@/components/common/resource-panel";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { ComponentZonePicker } from "@/features/environment/component-zone-picker";
import { CaddyTemplateEditor } from "@/features/environment/caddy-template-editor";
import { TunnelAccessCheck } from "@/features/environment/tunnel-check-panel";
import type { Environment, Zone } from "@/lib/types";

// ---- Router ----

// C07 may summarize Route desired state, but live Router component state and
// controls belong to the C12/C14 capability surfaces.
export function RouterCard({ env }: { env: Environment }) {
  const store = useStore();
  const params = useRequiredParams("tenant");
  const routerReady = env.components.some(
    (component) =>
      component.kind === "caddy" ||
      component.kind === "envoy" ||
      component.kind === "cloudflare-tunnel",
  );
  const publicRoutes = env.routes.filter(
    (route) => route.exposure === "public",
  );
  const proxy = env.components.find(
    (component) => component.kind === "caddy" || component.kind === "envoy",
  );
  const tunnel = env.components.find(
    (component) => component.kind === "cloudflare-tunnel",
  );
  const tunnelEntries = env.entries.filter((entry) => entry.type === "env" && entry.secret && entry.source.kind === "literal" && entry.exposure.length === 1 && entry.exposure[0] === "component:edge-tunnel");
  const tunnelSecrets = store.reusableSecrets.filter(
    (secret) =>
      secret.kind === "env" &&
      (secret.scope === "platform" || secret.projectId === env.projectId),
  );
  const [operation, setOperation] = useState<{
    component: Environment["components"][number];
    action: "enable" | "disable" | "update" | "config";
  } | null>(null);
  const [selectedZoneIds, setSelectedZoneIds] = useState<string[]>([]);
  const [createdComponentZones, setCreatedComponentZones] = useState<Zone[]>(
    [],
  );
  const [creatingComponentZone, setCreatingComponentZone] = useState(false);
  const [routerAlias, setRouterAlias] = useState(proxy?.config?.alias ?? "");
  const [routerImplementation, setRouterImplementation] = useState<
    "caddy" | "envoy"
  >(proxy?.kind === "envoy" ? "envoy" : "caddy");
  const [caddyTemplate, setCaddyTemplate] = useState(
    proxy?.kind === "caddy" ? (proxy.config?.caddyfile_template ?? "") : "",
  );
  const [caddyTemplateBlocked, setCaddyTemplateBlocked] = useState(false);
  const [tunnelCredentialMode, setTunnelCredentialMode] = useState<
    "environment" | "existing" | "new"
  >(tunnel?.config?.token_entry || !tunnel?.config?.secret_id ? "environment" : "existing");
  const [tunnelEntry, setTunnelEntry] = useState(tunnel?.config?.token_entry ?? "");
  const [tunnelSecret, setTunnelSecret] = useState(
    tunnel?.config?.secret_id ?? "",
  );
  const [tunnelSecretName, setTunnelSecretName] = useState(
    "CLOUDFLARE_TUNNEL_TOKEN",
  );
  const [tunnelToken, setTunnelToken] = useState("");
  if (!routerReady) return <EnvironmentRouterUnavailable />;

  const openConfig = (
    component: Environment["components"][number],
    action: "config" | "enable" = "config",
  ) => {
    setSelectedZoneIds(component.config?.zone_ids ?? []);
    setCreatedComponentZones([]);
    if (component.kind !== "cloudflare-tunnel") {
      setRouterImplementation(component.kind === "envoy" ? "envoy" : "caddy");
      setRouterAlias(component.config?.alias ?? "");
      setCaddyTemplate(
        component.kind === "caddy"
          ? (component.config?.caddyfile_template ?? "")
          : "",
      );
      setCaddyTemplateBlocked(false);
    } else {
      setTunnelCredentialMode(component.config?.token_entry || !component.config?.secret_id ? "environment" : "existing");
      setTunnelEntry(component.config?.token_entry ?? "");
      setTunnelSecret(component.config?.secret_id ?? "");
      setTunnelToken("");
    }
    setOperation({ component, action });
  };

  const configuring =
    operation?.action === "config" || operation?.action === "enable";
  const availableComponentZones = [
    ...env.zones,
    ...createdComponentZones.filter(
      (zone) => !env.zones.some((current) => current.id === zone.id),
    ),
  ];
  const tunnelHasEgress = selectedZoneIds.some((id) =>
    availableComponentZones.some((zone) => zone.id === id && !zone.internal),
  );
  const operationDisabled =
    configuring &&
    (creatingComponentZone ||
      selectedZoneIds.length === 0 ||
      (operation.component.kind !== "cloudflare-tunnel"
        ? (routerAlias !== "" &&
            !/^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/.test(routerAlias)) ||
          (routerImplementation === "caddy" && caddyTemplateBlocked)
        : !tunnelHasEgress ||
          (tunnelCredentialMode === "environment"
            ? !tunnelEntry
            : tunnelCredentialMode === "existing"
            ? !tunnelSecret
            : !tunnelSecretName || !tunnelToken)));
  const componentName =
    operation?.component.kind === "cloudflare-tunnel"
      ? "Cloudflare Tunnel"
      : "Environment proxy";

  const operationView = operation && (
    <TaskRunnerDialog
      runAfter
      key={operation.component.id + operation.action}
      open
      onOpenChange={(open) => {
        if (!open) setOperation(null);
      }}
      variant={configuring ? "embedded" : "dialog"}
      title={`${operation.action === "config" ? "Configure" : operation.action === "enable" ? "Enable" : operation.action === "disable" ? "Disable" : "Reapply configuration for"} ${componentName} · ${env.name}`}
      type="run"
      target={operation.component.id}
      workspace={params.tenant}
      startLabel={
        operation.action === "config"
          ? "Save and apply"
          : `${operation.action === "enable" ? "Enable" : operation.action === "disable" ? "Disable" : "Reapply configuration for"} ${componentName}`
      }
      startDisabled={operationDisabled}
      review={
        configuring ? (
          <div className="flex flex-col gap-4">
            {operation.component.kind !== "cloudflare-tunnel" && (
              <FormSection title="Environment proxy">
                <div className="flex flex-col gap-2 sm:col-span-2">
                  <Label htmlFor="router-implementation">Implementation</Label>
                  <Select
                    id="router-implementation"
                    value={routerImplementation}
                    options={[
                      { value: "caddy", label: "Caddy · HTTP" },
                      { value: "envoy", label: "Envoy · HTTP, TCP, UDP" },
                    ]}
                    onValueChange={(value) => {
                      if (value === "caddy" || value === "envoy") {
                        setRouterImplementation(value);
                        setCaddyTemplateBlocked(false);
                      }
                    }}
                  />
                  {routerImplementation !== operation.component.kind && (
                    <p className="text-xs text-muted-foreground">
                      Engine changes require Recreate for affected running
                      Services. Routes stay in this Environment.
                    </p>
                  )}
                </div>
              </FormSection>
            )}
            <FormSection
              title="Network access"
              description={
                operation.component.kind !== "cloudflare-tunnel"
                  ? ""
                  : "Requires a Zone with outbound internet access."
              }
            >
              <div className="sm:col-span-2">
                <ComponentZonePicker
                  zones={availableComponentZones}
                  selectedZoneIds={selectedZoneIds}
                  onChange={setSelectedZoneIds}
                  networkPool={env.networkPool}
                  onCreate={async (input) => {
                    setCreatingComponentZone(true);
                    try {
                      const zone = await store.addZone(env.id, input);
                      setCreatedComponentZones((current) => [...current, zone]);
                      return zone;
                    } finally {
                      setCreatingComponentZone(false);
                    }
                  }}
                />
              </div>
              {operation.component.kind !== "cloudflare-tunnel" ? (
                <div className="flex flex-col gap-1 sm:col-span-2">
                  <Label htmlFor="component-primary-zone">
                    Primary network
                  </Label>
                  <Select
                    searchable
                    id="component-primary-zone"
                    value={selectedZoneIds[0] ?? ""}
                    onValueChange={(id) =>
                      setSelectedZoneIds((current) => [
                        id,
                        ...current.filter((value) => value !== id),
                      ])
                    }
                    options={selectedZoneIds.map((id) => ({
                      value: id,
                      label:
                        availableComponentZones.find((zone) => zone.id === id)
                          ?.name ?? id,
                    }))}
                  />
                  <p className="text-xs text-muted-foreground">
                    The router keeps a stable IP address on this Zone.
                  </p>
                </div>
              ) : (
                <p className="text-xs text-muted-foreground sm:col-span-2">
                  Select at least one non-internal Zone. The first selected
                  non-internal Zone supplies outbound connectivity.
                </p>
              )}
            </FormSection>
            {operation.component.kind !== "cloudflare-tunnel" ? (
              <div className="flex flex-col gap-3">
                {routerImplementation === "caddy" && (
                  <CaddyTemplateEditor
                    key={operation.component.id}
                    componentId={operation.component.id}
                    enabled={operation.component.enabled}
                    value={caddyTemplate}
                    onChange={setCaddyTemplate}
                    onBlockedChange={setCaddyTemplateBlocked}
                  />
                )}
                <AdvancedDetails title="Network alias (optional)">
                  <div className="flex flex-col gap-1">
                    <Label htmlFor="router-alias">
                      Router alias (optional)
                    </Label>
                    <Input
                      id="router-alias"
                      value={routerAlias}
                      maxLength={63}
                      onChange={(event) => setRouterAlias(event.target.value)}
                    />

                    {routerAlias !== "" &&
                      !/^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/.test(
                        routerAlias,
                      ) && (
                        <p role="alert" className="text-xs text-destructive">
                          Use a lowercase name with letters, numbers and
                          hyphens, up to 63 characters.
                        </p>
                      )}
                  </div>
                </AdvancedDetails>
              </div>
            ) : (
              <FormSection title="Tunnel credentials">
                <div className="flex flex-col gap-3 sm:col-span-2">
                  <div className="flex flex-col gap-1">
                    <Label htmlFor="component-credential-source">
                      Credential source
                    </Label>
                    <Select
                      id="component-credential-source"
                      value={tunnelCredentialMode}
                      onValueChange={(value) =>
                        setTunnelCredentialMode(value as "environment" | "existing" | "new")
                      }
                      options={[
                        {value: "environment", label: "Environment variable"},
                        {
                          value: "existing",
                          label: "Existing configuration value",
                        },
                        { value: "new", label: "New Project token" },
                      ]}
                    />
                  </div>
                  {tunnelCredentialMode === "environment" ? (
                    <div className="flex flex-col gap-1">
                      <Label htmlFor="tunnel-environment-token">Environment token</Label>
                      <Select searchable id="tunnel-environment-token" value={tunnelEntry} onValueChange={setTunnelEntry} options={tunnelEntries.flatMap((entry) => entry.key ? [{value: entry.key, label: entry.key + (entry.emptySecretValue ? " · Empty" : "")}] : [])} />
                      <p className="text-xs text-muted-foreground">Use a protected variable from this Environment, available only to Cloudflare Tunnel. Set its value in Environment Configuration before enabling.</p>
                    </div>
                  ) : tunnelCredentialMode === "existing" ? (
                    <div className="flex flex-col gap-1">
                      <Label htmlFor="component-reusable-secret">
                        Token value
                      </Label>
                      <Select
                        searchable
                        id="component-reusable-secret"
                        value={tunnelSecret}
                        onValueChange={setTunnelSecret}
                        options={tunnelSecrets.map((secret) => ({
                          value: secret.id,
                          label: `${secret.key} · ${secret.scope === "platform" ? "Platform" : "Project"}`,
                        }))}
                      />
                      {tunnelSecrets.length === 0 && (
                        <p className="text-xs text-warning">
                          Add a Project or Platform variable or Secret first, or
                          choose New Project token.
                        </p>
                      )}
                    </div>
                  ) : (
                    <>
                      <div className="flex flex-col gap-1">
                        <Label htmlFor="tunnel-secret-name">Secret name</Label>
                        <Input
                          id="tunnel-secret-name"
                          value={tunnelSecretName}
                          onChange={(event) =>
                            setTunnelSecretName(event.target.value)
                          }
                        />
                      </div>
                      <div className="flex flex-col gap-1">
                        <Label htmlFor="tunnel-token">Tunnel token</Label>
                        <Input
                          id="tunnel-token"
                          type="password"
                          value={tunnelToken}
                          onChange={(event) =>
                            setTunnelToken(event.target.value)
                          }
                        />
                        <p className="text-xs text-muted-foreground">
                          The token is write-only and becomes a Project Secret.
                        </p>
                      </div>
                    </>
                  )}
                </div>
              </FormSection>
            )}
          </div>
        ) : undefined
      }
      onDispatch={(after) => {
        if (operation.action === "disable")
          return store.setComponentEnabled(
            operation.component.id,
            false,
            undefined,
            after,
          );
        if (operation.action === "update")
          return store.reconcileEnvironmentComponent(
            operation.component.id,
            after,
          );
        const config =
          operation.component.kind !== "cloudflare-tunnel" &&
          routerImplementation === "caddy"
            ? {
                implementation: "caddy" as const,
                zone_ids: selectedZoneIds,
                alias: routerAlias,
                ...(caddyTemplate ? { caddyfile_template: caddyTemplate } : {}),
              }
            : operation.component.kind !== "cloudflare-tunnel" &&
                routerImplementation === "envoy"
              ? {
                  implementation: "envoy" as const,
                  zone_ids: selectedZoneIds,
                  alias: routerAlias,
                }
              : {
                  implementation: "cloudflare-tunnel" as const,
                  zone_ids: selectedZoneIds,
                  credential:
                    tunnelCredentialMode === "environment"
                      ? {mode: "environment" as const, token_entry: tunnelEntry}
                      : tunnelCredentialMode === "existing"
                      ? { mode: "existing" as const, secret_id: tunnelSecret }
                      : {
                          mode: "new" as const,
                          secret_name: tunnelSecretName,
                          token: tunnelToken,
                        },
                };
        return operation.action === "enable"
          ? store.setComponentEnabled(
              operation.component.id,
              true,
              config,
              after,
            )
          : store.updateComponentConfig(operation.component.id, config, after);
      }}
      onCommit={() => {
        void store.refreshEnvironmentComponents(env.id).catch(() => undefined);
      }}
    />
  );
  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <RouterIcon className="size-4 text-muted-foreground" /> Proxy &
            tunnel
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          {env.components.map((component) => {
            return (
              <InlineEditorRegion
                key={component.id}
                editing={
                  configuring && operation?.component.id === component.id
                }
              >
                <div
                  className={workspaceSectionClassName(
                    configuring && operation?.component.id === component.id,
                    "flex flex-col gap-3",
                  )}
                >
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <div>
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="font-medium">
                          {component.kind === "cloudflare-tunnel"
                            ? "Cloudflare Tunnel"
                            : "Environment proxy"}
                        </span>
                        {component.kind !== "cloudflare-tunnel" && (
                          <Badge variant="outline">
                            Saved:{" "}
                            {component.kind === "envoy" ? "Envoy" : "Caddy"}
                          </Badge>
                        )}
                        <StatusBadge status={component.status} />
                      </div>
                      <CompactReference
                        value={component.id}
                        label="Component ID"
                      />
                      <ContainerSoftwareIdentity
                        software={component.software}
                        className="mt-1 text-xs"
                        showReferences={false}
                        unavailable="Installed identity not reported by the Controller"
                      />
                    </div>
                    {!(
                      configuring && operation?.component.id === component.id
                    ) && (
                      <div className="flex flex-wrap items-center gap-2">
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => openConfig(component)}
                        >
                          Configure
                        </Button>
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() =>
                            setOperation({ component, action: "update" })
                          }
                        >
                          Reapply configuration
                        </Button>
                        <Button
                          size="sm"
                          variant={
                            component.enabled ? "destructive" : "default"
                          }
                          onClick={() =>
                            component.enabled
                              ? setOperation({ component, action: "disable" })
                              : openConfig(component, "enable")
                          }
                        >
                          {component.enabled ? "Disable" : "Enable"}
                        </Button>
                      </div>
                    )}
                  </div>
                  {component.kind !== "cloudflare-tunnel" ? (
                    <div className="grid gap-3 border-t border-border pt-4 text-sm text-muted-foreground [overflow-wrap:anywhere] sm:grid-cols-2">
                      <span>
                        Networks:{" "}
                        {component.config?.zone_ids
                          .map(
                            (id) =>
                              env.zones.find((zone) => zone.id === id)?.name ??
                              id,
                          )
                          .join(", ") || "not configured"}
                      </span>
                      <span>
                        IP address:{" "}
                        {component.state.pinnedIPv4 || "not allocated"}
                      </span>
                    </div>
                  ) : (
                    <div className="grid gap-3 border-t border-border pt-4 text-sm text-muted-foreground [overflow-wrap:anywhere] sm:grid-cols-2">
                      <span>
                        Token reference:{" "}
                        {component.config?.token_entry ? component.config.token_entry + " · Environment" : component.config?.secret_id || "not configured"}
                      </span>
                      <span>
                        Networks:{" "}
                        {component.config?.zone_ids
                          .map(
                            (id) =>
                              env.zones.find((zone) => zone.id === id)?.name ??
                              id,
                          )
                          .join(", ") || "not configured"}
                      </span>
                      <span>Routing: managed in Cloudflare</span>
                    </div>
                  )}
                  {configuring && operation?.component.id === component.id && (
                    <div className="border-t border-primary/25 pt-4">
                      {operationView}
                    </div>
                  )}
                  {component.kind === "cloudflare-tunnel" &&
                    component.enabled && (
                      <TunnelAccessCheck
                        key={component.id}
                        env={env}
                        connectorId={component.id}
                      />
                    )}
                </div>
              </InlineEditorRegion>
            );
          })}
          <div className="rounded-lg border border-border bg-background px-3 py-2 text-xs text-muted-foreground">
            <span className="font-medium text-foreground">Public Routes:</span>{" "}
            {publicRoutes.length > 0
              ? publicRoutes
                  .map((route) => route.host)
                  .filter(Boolean)
                  .join(", ")
              : "none"}
          </div>
          <p className="text-xs text-muted-foreground">
            Manage hostnames and tunnel routing in Cloudflare.
          </p>
        </CardContent>
      </Card>

      {!configuring && operationView}
    </>
  );
}
