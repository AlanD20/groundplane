import { useState } from "react";
import { Check, Circle, LoaderCircle, PlugZap, X } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { Disclosure } from "@/components/common/disclosure";
import { CompactReference } from "@/components/common/compact-reference";
import { TaskLink } from "@/components/common/task-link";
import type { EnvironmentComponent, Route } from "@/lib/types";
import { useEdgeCheck } from "./use-edge-check";
import type { TunnelCheckSelection } from "./edge-check-api";

const names: Record<string, string> = {
  tunnel_connection: "Tunnel connected",
  origin_http: "Origin HTTP response",
  public_dns: "Public DNS",
  public_tls: "Hostname TLS certificate",
  public_http: "Public HTTP response",
};
const failures: Record<string, string> = {
  connection:
    "The selected endpoint could not be reached. Check the serving runtime and its network.",
  dns: "The public hostname could not be resolved.",
  non_public_address:
    "DNS returned a non-public address; GP did not connect to it.",
  tls: "The selected address did not present a valid TLS certificate for this hostname.",
  http_response: "The endpoint returned an unsuccessful HTTP response.",
  connector_not_ready:
    "The connector does not report an established Tunnel connection.",
  timeout: "The check reached its time limit.",
  canceled: "The check was canceled.",
  protocol: "The endpoint did not return the expected response.",
  interrupted:
    "Execution was interrupted. Cleanup does not rerun the probe; submit a new check afterwards.",
  configuration_changed:
    "The captured connector was replaced or no longer matches its acknowledged runtime.",
  agent_unavailable: "The captured Agent could not complete the check.",
  execution_failed: "The Agent could not execute the fixed helper.",
  cleanup: "The temporary helper could not be removed.",
  journal: "GP could not persist the check result.",
};

export function RouteAccessCheck({
  environmentId,
  route,
  components,
  open,
}: {
  environmentId: string;
  route: Route;
  components: EnvironmentComponent[];
  open: boolean;
}) {
  const connectors = components.filter(
    (component) => component.kind === "cloudflare-tunnel" && component.enabled,
  );
  const [expanded, setExpanded] = useState(false);
  const [connectorId, setConnectorId] = useState(connectors[0]?.id ?? "");
  const selectedConnectorId = connectors.some(
    (connector) => connector.id === connectorId,
  )
    ? connectorId
    : (connectors[0]?.id ?? "");
  const [path, setPath] = useState(route.path.endsWith("*") ? "" : route.path);
  return (
    <Disclosure
      summary={
        <span className="flex items-center gap-2 text-xs font-medium">
          <PlugZap className="size-3.5" />
          Test public access
        </span>
      }
      open={expanded}
      onOpenChange={setExpanded}
    >
      <div className="space-y-3 pt-3">
        {connectors.length > 1 && (
          <Select
            aria-label="Tunnel connector"
            value={selectedConnectorId}
            onValueChange={setConnectorId}
            options={connectors.map((connector, index) => ({
              value: connector.id,
              label: `Tunnel ${index + 1}`,
            }))}
          />
        )}
        {route.path.endsWith("*") && (
          <div className="space-y-1.5">
            <Label htmlFor={`edge-path-${route.id}`}>Request path</Label>
            <Input
              id={`edge-path-${route.id}`}
              value={path}
              onChange={(event) => setPath(event.target.value)}
              placeholder="/api/health"
            />
            <p className="text-xs text-muted-foreground">
              Choose a concrete path matching {route.path}.
            </p>
          </div>
        )}
        <EdgeCheckPanel
          environmentId={environmentId}
          routeId={route.id}
          connectorId={selectedConnectorId}
          path={path}
          open={open && expanded}
          disabled={route.status !== "served" || !selectedConnectorId || !path}
        />
      </div>
    </Disclosure>
  );
}

export function EdgeCheckPanel({
  environmentId,
  routeId,
  connectorId,
  path,
  taskId,
  open,
  disabled = false,
  tunnel,
}: {
  environmentId: string;
  routeId?: string;
  connectorId?: string;
  path?: string;
  taskId?: string;
  open: boolean;
  disabled?: boolean;
  tunnel?: TunnelCheckSelection;
}) {
  const check = useEdgeCheck(
    environmentId,
    open,
    routeId,
    connectorId,
    path,
    taskId,
    tunnel,
  );
  const result = check.result;
  const stages = result?.stages ?? [];
  const addresses = result?.addresses ?? [];
  const differentSelection =
    !!result &&
    (!!routeId || !!tunnel) &&
    (result.configuration.connector_id !== connectorId ||
      (!!path && result.configuration.target.path !== path) ||
      (!!tunnel &&
        (result.configuration.origin_service_id !== tunnel.originServiceId ||
          result.configuration.target.hostname !== tunnel.hostname ||
          result.configuration.target.path !== tunnel.path ||
          !result.configuration.origin_address.endsWith(`:${tunnel.port}`))));
  return (
    <section
      aria-label="Public access check"
      className="flex flex-col gap-3 rounded-lg border border-border bg-surface p-4"
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-medium">Public access check</h3>
        {(routeId || tunnel) && (
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={
              disabled || check.sending || check.loading || check.active
            }
            onClick={() => void check.run()}
          >
            {check.sending ? (
              <LoaderCircle className="size-3.5 animate-spin" />
            ) : (
              <PlugZap className="size-3.5" />
            )}
            Test access
          </Button>
        )}
      </div>
      {disabled && (
        <p className="text-xs text-muted-foreground">
          Apply the public Route and enable its Tunnel first. Wildcard Routes
          need a concrete request path.
        </p>
      )}
      <p className="text-xs text-muted-foreground">
        Read-only check. Tunnel and origin use the connector network; public
        DNS/TLS/HTTP use the Controller’s outbound network.
      </p>
      {check.loading && (
        <p role="status" className="text-xs text-muted-foreground">
          Loading retained check…
        </p>
      )}
      {check.error && (
        <p role="alert" className="text-xs text-destructive">
          {check.error}
        </p>
      )}
      {check.taskId && (
        <TaskLink taskId={check.taskId}>
          {check.active ? "Inspect running check" : "Inspect check Task"}
        </TaskLink>
      )}
      {result && (
        <>
          <div className="flex flex-wrap items-center gap-2">
            <Badge>{result.outcome || result.phase.replaceAll("_", " ")}</Badge>
            {result.stale && (
              <Badge variant="outline">Configuration changed</Badge>
            )}
            {differentSelection && (
              <Badge variant="outline">Different selection</Badge>
            )}
            <span className="text-xs text-muted-foreground">
              {new Date(
                result.finished_at || result.created_at,
              ).toLocaleString()}
            </span>
          </div>
          <p className="break-all text-xs">
            https://{result.configuration.target.hostname}
            {result.configuration.target.path}
          </p>
          <p className="break-all text-xs text-muted-foreground">
            Origin: {result.configuration.origin_address}
          </p>
          {stages.map((stage) => (
            <div
              key={stage.name}
              className="space-y-1 rounded-md border border-border p-2.5"
            >
              <div className="flex items-center gap-2 text-xs">
                {stage.outcome === "failed" ? (
                  <X className="size-3.5 text-destructive" />
                ) : stage.outcome === "skipped" ? (
                  <Circle className="size-3.5 text-muted-foreground" />
                ) : (
                  <Check className="size-3.5 text-success" />
                )}
                <span className="font-medium">{names[stage.name]}</span>
                <span className="ml-auto">
                  {stage.outcome}
                  {stage.http_status ? ` · HTTP ${stage.http_status}` : ""}
                </span>
              </div>
              <p className="text-xs text-muted-foreground">
                {stage.execution_location} · {stage.elapsed_ms} ms
              </p>
              {stage.failure && (
                <p className="text-xs text-destructive">
                  {failures[stage.failure] || "This stage could not complete."}
                </p>
              )}
            </div>
          ))}
          {!stages.length && check.active && (
            <p role="status" className="flex items-center gap-2 text-xs">
              <LoaderCircle className="size-3.5 animate-spin" />
              Waiting for recorded stage results…
            </p>
          )}
          {[result.failure, result.private_failure, result.public_failure]
            .filter(
              (value, index, values) =>
                value && values.indexOf(value) === index,
            )
            .map((failure) => (
              <p key={failure} className="text-xs text-destructive">
                {failures[failure!] || "The check could not complete."}
              </p>
            ))}
          {result.outcome === "protected" && (
            <p className="text-xs text-muted-foreground">
              HTTP 401/403 proves the endpoint responded; it does not verify
              application authentication.
            </p>
          )}
          {result.cleanup_required && (
            <div className="space-y-2">
              <p className="text-xs text-destructive">
                Helper cleanup remains. A cleanup retry never repeats the probe.
              </p>
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={check.sending}
                onClick={() => void check.cleanup()}
              >
                Resume cleanup
              </Button>
            </div>
          )}
          <Disclosure summary="Captured configuration">
            <div className="space-y-2 pt-3 text-xs">
              <CompactReference
                label="Connector"
                value={result.configuration.connector_id}
              />
              <CompactReference
                label="Runtime digest"
                value={result.configuration.runtime_sha256}
              />
              <p>
                Connector desired/applied revisions:{" "}
                {result.configuration.connector_revision}/
                {result.configuration.connector_applied_revision}
              </p>
              {result.configuration.router_id && (
                <p>
                  Router desired/applied revisions:{" "}
                  {result.configuration.router_revision}/
                  {result.configuration.router_applied_revision}
                </p>
              )}
              {result.configuration.origin_service_id && (
                <>
                  <CompactReference
                    label="Origin Service"
                    value={result.configuration.origin_service_id}
                  />
                  <p>
                    Applied origin revision:{" "}
                    {result.configuration.origin_runtime_revision}
                  </p>
                  {result.configuration.origin_runtime_sha256 && (
                    <CompactReference
                      label="Origin runtime digest"
                      value={result.configuration.origin_runtime_sha256}
                    />
                  )}
                </>
              )}
              {addresses.length > 0 && (
                <p className="break-all">DNS answers: {addresses.join(", ")}</p>
              )}
              {result.selected_address && (
                <p className="break-all">
                  Checked address: {result.selected_address}
                </p>
              )}
              {!!result.tls_version && (
                <p>TLS {result.tls_version === 772 ? "1.3" : "1.2"}</p>
              )}
              {result.certificate_expires_at && (
                <p>
                  Certificate expires:{" "}
                  {new Date(result.certificate_expires_at).toLocaleString()}
                </p>
              )}
              {result.helper_name && (
                <CompactReference
                  label="Cleanup helper"
                  value={result.helper_name}
                />
              )}
            </div>
          </Disclosure>
        </>
      )}
      {!result && !check.loading && !check.active && (
        <p className="text-xs text-muted-foreground">
          No retained check. Testing does not save, deploy or change provider
          ingress.
        </p>
      )}
    </section>
  );
}
