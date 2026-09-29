import {
  AdvancedDetails,
  ResourcePanel,
} from "@/components/common/resource-panel";
import { RevealValue } from "@/components/common/reveal-value";
import { useStore } from "@/lib/store";
import type { ConsumerLink, Project } from "@/lib/types";
import { valkeyAuthenticationDetails } from "@/lib/valkey-authentication";
import { Row } from "./service-tab";

// ---- Connections ----

export function ConnectionsTab({
  g,
  env,
  svc,
}: {
  g: Project;
  env: NonNullable<Project["environments"]>[number];
  svc: NonNullable<Project["environments"]>[number]["services"][number];
}) {
  const store = useStore();
  const adapter = store.adapters.find((a) => a.key === svc.adapter);
  const authenticationDetails = valkeyAuthenticationDetails(svc.authentication);
  const authenticationUnavailable =
    svc.adapter === "valkey:9" && !authenticationDetails;
  const exposedFacts = authenticationDetails
    ? authenticationDetails.factSuffixes.map(
        (suffix) => `${svc.prefix ?? adapter?.prefix}_${suffix}`,
      )
    : authenticationUnavailable
      ? []
      : (adapter?.envVars ?? []);
  const provision =
    authenticationDetails?.provision ??
    (authenticationUnavailable ? [] : (adapter?.provision ?? []));
  const exposesRole =
    adapter?.requires.role &&
    (svc.adapter !== "valkey:9" || svc.authentication === "username_password");

  return (
    <>
      {adapter && (
        <AdvancedDetails title={`Adapter details · ${svc.adapter}`}>
          <div className="flex flex-col gap-3">
            {adapter?.custom ? (
              <p className="text-xs text-muted-foreground">
                <span className="font-medium text-foreground">
                  Custom container managed by Groundplane.
                </span>{" "}
                {svc.hooks?.attach
                  ? "Each new Attach runs your provisioning command and publishes its declared facts after success."
                  : "Attaching connects a consumer to the backing network without provisioning."}{" "}
                Hooks are configured under Edit service. Custom services have no
                managed grants or backups.
              </p>
            ) : authenticationUnavailable ? (
              <p role="alert" className="text-xs text-destructive">
                The Controller did not return this Valkey backing
                instance&apos;s authentication mode. Refresh before using
                connection guidance.
              </p>
            ) : authenticationDetails ? (
              <p className="text-xs text-muted-foreground">
                Every Attach{" "}
                <span className="font-medium text-foreground">
                  inherits the backing instance&apos;s immutable{" "}
                  {authenticationDetails.label.toLowerCase()} authentication
                  mode
                </span>
                ; there is no per-Attach authentication selector.{" "}
                {authenticationDetails.summary} Attaching always joins this
                backing network and publishes the mode&apos;s facts; nothing is
                injected automatically.
              </p>
            ) : (
              <p className="text-xs text-muted-foreground">
                The Controller dispatches to this adapter to auto-provision
                consumers. These operations run on attach, rotate, and repair —
                each parameter is filled from the attach facts and the generated
                password. Attaching exposes facts (prefix{" "}
                <span className="font-mono">
                  {svc.prefix ?? adapter?.prefix}
                </span>
                ), e.g.{" "}
                <span className="font-mono">
                  {svc.prefix ?? adapter?.prefix}_URL
                </span>{" "}
                — you create env vars from them; nothing is injected
                automatically.
              </p>
            )}
            <div className="flex flex-col gap-1.5 text-sm">
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Network</span>
                <span className="font-mono text-xs">
                  owns/joins:{" "}
                  {env.zones
                    .map(
                      (z) =>
                        `${z.name} · ${z.subnet}${z.internal ? " · internal" : ""}`,
                    )
                    .join(", ") || "—"}
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Healthcheck</span>
                <span className="font-mono text-xs">
                  {svc.healthcheck
                    ? `${svc.healthcheck.kind} ${svc.healthcheck.target} · every ${svc.healthcheck.interval}`
                    : "none"}
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Volume</span>
                <span className="font-mono text-xs">
                  {svc.mounts.find((m) => m.type === "volume")?.volume ??
                    "data"}
                </span>
              </div>
            </div>
            {!adapter?.custom && (
              <>
                <div className="flex flex-wrap gap-1.5">
                  {exposedFacts.map((v) => (
                    <span
                      key={v}
                      className="rounded-full bg-primary/10 px-2.5 py-1 font-mono text-xs text-primary"
                    >
                      {v}
                    </span>
                  ))}
                </div>
                <div className="flex flex-col gap-1 border-t border-border pt-3">
                  {authenticationUnavailable ? (
                    <p className="text-xs text-muted-foreground">
                      Mode-specific provisioning is unavailable until the
                      Controller returns the authentication mode.
                    </p>
                  ) : (
                    provision.length === 0 && (
                      <p className="text-xs text-muted-foreground">
                        No credential provisioning steps. Attach manages network
                        membership and fact ownership only.
                      </p>
                    )
                  )}
                  {provision.map((op) => (
                    <div
                      key={op.op}
                      className="flex items-baseline gap-2.5 text-xs"
                    >
                      <span className="size-1.5 shrink-0 translate-y-[-2px] rounded-full bg-success" />
                      <span className="w-36 shrink-0 font-mono text-primary">
                        {op.op}
                      </span>
                      <span className="break-all font-mono text-muted-foreground">
                        {op.detail}
                      </span>
                    </div>
                  ))}
                </div>
              </>
            )}
          </div>
        </AdvancedDetails>
      )}

      <ResourcePanel title="Consumers">
        <div className="space-y-3">
          {(g.consumers ?? []).length === 0 && (
            <p className="text-xs text-muted-foreground">
              No environments attached yet — attachment opens once this backing
              service is running.
            </p>
          )}
          {(g.consumers ?? []).map((c) => (
            <div
              key={`${c.environment}-${c.service}-${c.attachId}`}
              className="rounded-xl border border-border bg-card p-4"
            >
              <div className="flex items-center justify-between">
                <span className="font-mono text-sm">
                  {c.project} / {c.environment}{" "}
                  <span className="text-muted-foreground">· {c.service}</span>
                </span>
                {!adapter?.custom && (
                  <div className="flex items-center gap-1">
                    <ConsumerConnectionActions
                      consumer={c}
                      authentication={svc.authentication}
                    />
                  </div>
                )}
              </div>
              {adapter?.custom ? (
                <div className="mt-2 flex flex-col gap-1.5 text-sm">
                  <Row
                    label="Access"
                    value={
                      svc.hooks?.attach
                        ? "network access and custom provisioning"
                        : "network-only, no provisioning"
                    }
                    mono
                  />
                  <Row
                    label="Declared facts"
                    value={
                      svc.hooks?.facts?.map((fact) => fact.key).join(", ") ||
                      "none"
                    }
                    mono
                  />
                  <Row
                    label="Reach at"
                    value={`${svc.serviceName ?? svc.name} on the network`}
                    mono
                  />
                </div>
              ) : (
                <div className="mt-2 flex flex-col gap-1.5 text-sm">
                  <Row label="Service" value={c.service} mono />
                  {adapter?.requires.database && (
                    <Row label="Database" value={c.database} mono />
                  )}
                  {exposesRole && <Row label="Role" value={c.role} mono />}
                  <Row
                    label="Host"
                    value={`${svc.serviceName}:${adapter?.urlScheme === "redis" ? 6379 : 5432}`}
                    mono
                  />
                  <Row
                    label="Connection"
                    value={
                      c.connectionFactKey
                        ? "available through explicit reveal"
                        : "unavailable"
                    }
                    mono
                    masked={svc.authentication !== "none"}
                  />
                </div>
              )}
              {!adapter?.custom && (
                <details className="mt-2">
                  <summary className="cursor-pointer text-xs text-primary">
                    procedure the Agent runs · {provision.length} steps
                  </summary>
                  <div className="mt-2 flex flex-col gap-1 border-t border-border pt-2">
                    {authenticationUnavailable ? (
                      <p className="text-xs text-muted-foreground">
                        Mode-specific provisioning is unavailable until the
                        Controller returns the authentication mode.
                      </p>
                    ) : (
                      provision.length === 0 && (
                        <p className="text-xs text-muted-foreground">
                          No credential procedure; the Attach joins the network
                          and publishes credential-free facts.
                        </p>
                      )
                    )}
                    {provision.map((op) => (
                      <div
                        key={op.op}
                        className="flex items-baseline gap-2.5 text-xs"
                      >
                        <span className="size-1.5 shrink-0 translate-y-[-2px] rounded-full bg-success" />
                        <span className="w-32 shrink-0 font-mono text-primary">
                          {op.op}
                        </span>
                        <span className="break-all font-mono text-muted-foreground">
                          {op.detail
                            .replaceAll("<db>", c.database)
                            .replaceAll("<role>", c.role)
                            .replaceAll("<generated>", "••••••")}
                        </span>
                      </div>
                    ))}
                  </div>
                </details>
              )}
            </div>
          ))}
        </div>
      </ResourcePanel>
    </>
  );
}

function ConsumerConnectionActions({
  consumer,
  authentication,
}: {
  consumer: ConsumerLink;
  authentication?: string;
}) {
  const store = useStore();
  if (!consumer.connectionFactKey) {
    return (
      <span className="text-xs text-muted-foreground">
        Connection fact unavailable
      </span>
    );
  }
  const factKey = consumer.connectionFactKey;
  return (
    <RevealValue
      loadValue={() => store.revealAttachFact(consumer.attachId, factKey)}
      label="connection"
      confirmWord={consumer.role || consumer.service}
      sensitive={authentication !== "none"}
    />
  );
}
