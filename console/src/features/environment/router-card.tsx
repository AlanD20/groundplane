import { CompactReference } from "@/components/common/compact-reference";
import { workspaceSectionClassName } from "@/components/common/workspace-section";
("use client");

import { useState } from "react";
import { useRequiredParams } from "@/lib/router";
import { Router as RouterIcon } from "lucide-react";
import { useStore } from "@/lib/store";
import { EnvironmentRouterUnavailable } from "@/features/environment/router-unavailable";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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
import type { Environment, Zone } from "@/lib/types";

// ---- Router ----

// C07 may summarize Route desired state, but live Router component state and
// controls belong to the C12/C14 capability surfaces.
export function RouterCard({ env }: { env: Environment }) {
  const store = useStore();
  const params = useRequiredParams("tenant");
  const routerReady =
    env.components.some((component) => component.kind === "caddy") &&
    env.components.some((component) => component.kind === "cloudflare-tunnel");
  const publicRoutes = env.routes.filter(
    (route) => route.exposure === "public",
  );
  const caddy = env.components.find((component) => component.kind === "caddy");
  const tunnel = env.components.find(
    (component) => component.kind === "cloudflare-tunnel",
  );
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
  const [routerAlias, setRouterAlias] = useState(caddy?.config?.alias ?? "");
  const [caddyTemplate, setCaddyTemplate] = useState(
    caddy?.config?.caddyfile_template ?? "",
  );
  const [caddyTemplateBlocked, setCaddyTemplateBlocked] = useState(false);
  const [tunnelCredentialMode, setTunnelCredentialMode] = useState<
    "existing" | "new"
  >("existing");
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
    if (component.kind === "caddy") {
      setRouterAlias(component.config?.alias ?? "");
      setCaddyTemplate(component.config?.caddyfile_template ?? "");
      setCaddyTemplateBlocked(false);
    } else {
      setTunnelCredentialMode("existing");
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
      (operation.component.kind === "caddy"
        ? (routerAlias !== "" &&
            !/^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/.test(routerAlias)) ||
          caddyTemplateBlocked
        : !tunnelHasEgress ||
          (tunnelCredentialMode === "existing"
            ? !tunnelSecret
            : !tunnelSecretName || !tunnelToken)));
  const componentName =
    operation?.component.kind === "caddy" ? "router" : "Cloudflare Tunnel";

  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <RouterIcon className="size-4 text-muted-foreground" /> Router and
            tunnel components
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          {env.components.map((component) => {
            return (
              <div
                key={component.id}
                className={workspaceSectionClassName(
                  false,
                  "flex flex-col gap-3",
                )}
              >
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <div className="flex items-center gap-2">
                      <span className="font-medium">
                        {component.kind === "caddy"
                          ? "Caddy"
                          : "Cloudflare Tunnel"}
                      </span>
                      <StatusBadge status={component.status} />
                    </div>
                    <CompactReference
                      value={component.id}
                      label="Component ID"
                    />
                  </div>
                  <div className="flex items-center gap-2">
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
                      Update
                    </Button>
                    <Button
                      size="sm"
                      variant={component.enabled ? "destructive" : "default"}
                      onClick={() =>
                        component.enabled
                          ? setOperation({ component, action: "disable" })
                          : openConfig(component, "enable")
                      }
                    >
                      {component.enabled ? "Disable" : "Enable"}
                    </Button>
                  </div>
                </div>
                {component.kind === "caddy" ? (
                  <div className="grid gap-3 border-t border-border pt-4 text-sm text-muted-foreground [overflow-wrap:anywhere] sm:grid-cols-2">
                    <span>
                      Networks:
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
                      {component.config?.secret_id || "not configured"}
                    </span>
                    <span>
                      Networks:
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
              </div>
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
            Groundplane starts the outbound connector from its Secret and
            reports health. DNS, public hostnames, ingress rules, origin
            targets, and protocol remain provider-managed.
          </p>
        </CardContent>
      </Card>

      {operation && (
        <TaskRunnerDialog
          open
          onOpenChange={(open) => {
            if (!open) setOperation(null);
          }}
          variant={configuring ? "inline" : "dialog"}
          title={`${operation.action === "config" ? "Configure" : operation.action === "enable" ? "Enable" : operation.action === "disable" ? "Disable" : "Update"} ${componentName} · ${env.name}`}
          description={
            operation.component.kind === "caddy"
              ? "Route requests to this Environment’s Services. Changes are applied through a Task."
              : "Connect this Environment to Cloudflare using your tunnel token. Changes are applied through a Task."
          }
          type="run"
          target={operation.component.id}
          workspace={params.tenant}
          startLabel={
            operation.action === "config"
              ? "Save and apply"
              : `${operation.action === "enable" ? "Enable" : operation.action === "disable" ? "Disable" : "Update"} ${componentName}`
          }
          startDisabled={operationDisabled}
          review={
            configuring ? (
              <div className="flex flex-col gap-4">
                <FormSection
                  title="Network access"
                  description={
                    operation.component.kind === "caddy"
                      ? "Select the Zones that connect the router to your Services."
                      : "Select the Zones the tunnel can reach, including one with outbound internet access."
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
                          setCreatedComponentZones((current) => [
                            ...current,
                            zone,
                          ]);
                          return zone;
                        } finally {
                          setCreatingComponentZone(false);
                        }
                      }}
                    />
                  </div>
                  {operation.component.kind === "caddy" ? (
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
                            availableComponentZones.find(
                              (zone) => zone.id === id,
                            )?.name ?? id,
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
                {operation.component.kind === "caddy" ? (
                  <div className="flex flex-col gap-3">
                    <CaddyTemplateEditor
                      key={operation.component.id}
                      componentId={operation.component.id}
                      enabled={operation.component.enabled}
                      value={caddyTemplate}
                      onChange={setCaddyTemplate}
                      onBlockedChange={setCaddyTemplateBlocked}
                    />
                    <AdvancedDetails title="Network alias (optional)">
                      <div className="flex flex-col gap-1">
                        <Label htmlFor="router-alias">
                          Router alias (optional)
                        </Label>
                        <Input
                          id="router-alias"
                          value={routerAlias}
                          maxLength={63}
                          onChange={(event) =>
                            setRouterAlias(event.target.value)
                          }
                          aria-describedby="router-alias-help"
                        />
                        <p
                          id="router-alias-help"
                          className="text-xs text-muted-foreground"
                        >
                          An additional name for reaching the router on its
                          primary network, for example from a tunnel. Leave
                          empty if not needed.
                        </p>
                        {routerAlias !== "" &&
                          !/^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/.test(
                            routerAlias,
                          ) && (
                            <p
                              role="alert"
                              className="text-xs text-destructive"
                            >
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
                            setTunnelCredentialMode(value as "existing" | "new")
                          }
                          options={[
                            { value: "existing", label: "Existing Secret" },
                            { value: "new", label: "New token" },
                          ]}
                        />
                      </div>
                      {tunnelCredentialMode === "existing" ? (
                        <div className="flex flex-col gap-1">
                          <Label htmlFor="component-reusable-secret">
                            Reusable Secret
                          </Label>
                          <Select
                            searchable
                            id="component-reusable-secret"
                            value={tunnelSecret}
                            onValueChange={setTunnelSecret}
                            options={tunnelSecrets.map((secret) => ({
                              value: secret.id,
                              label: secret.key + " · " + secret.id,
                            }))}
                          />
                          {tunnelSecrets.length === 0 && (
                            <p className="text-xs text-warning">
                              Create a Project or platform env-var Secret first,
                              or choose New token.
                            </p>
                          )}
                        </div>
                      ) : (
                        <>
                          <div className="flex flex-col gap-1">
                            <Label htmlFor="tunnel-secret-name">
                              Secret name
                            </Label>
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
                              The token is write-only and becomes a Project
                              Secret.
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
          steps={[
            {
              label: "Check configuration and network access",
              state: "pending",
            },
            { label: `Apply ${componentName} configuration`, state: "pending" },
            { label: `Report ${componentName} status`, state: "pending" },
          ]}
          executionCopy="After you confirm:"
          onDispatch={() => {
            if (operation.action === "disable")
              return store.setComponentEnabled(operation.component.id, false);
            if (operation.action === "update")
              return store.reconcileEnvironmentComponent(
                operation.component.id,
              );
            const config =
              operation.component.kind === "caddy"
                ? {
                    zone_ids: selectedZoneIds,
                    alias: routerAlias,
                    ...(caddyTemplate
                      ? { caddyfile_template: caddyTemplate }
                      : {}),
                  }
                : {
                    zone_ids: selectedZoneIds,
                    credential:
                      tunnelCredentialMode === "existing"
                        ? { mode: "existing" as const, secret_id: tunnelSecret }
                        : {
                            mode: "new" as const,
                            secret_name: tunnelSecretName,
                            token: tunnelToken,
                          },
                  };
            return operation.action === "enable"
              ? store.setComponentEnabled(operation.component.id, true, config)
              : store.updateComponentConfig(operation.component.id, config);
          }}
          onCommit={() => {
            void store
              .refreshEnvironmentComponents(env.id)
              .catch(() => undefined);
          }}
        />
      )}
    </>
  );
}
