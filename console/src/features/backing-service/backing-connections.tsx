import { InspectionWorkspace } from "@/components/common/inspection-workspace";
import { Disclosure } from "@/components/common/disclosure";
import { describeHealthcheck } from "@/features/service/healthcheck";
import { Button } from "@/components/ui/button";
import { SearchableSelect } from "@/components/ui/searchable-select";
import {
  ListToolbar,
  TablePagination,
  useTableView,
} from "@/components/common/table-controls";
import { useState } from "react";
import { Link } from "react-router-dom";
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
    svc.adapter === "valkey" && !authenticationDetails;
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
    (svc.adapter !== "valkey" || svc.authentication === "username_password");

  const [query, setQuery] = useState("");
  const [selectedId, setSelectedId] = useState<string>();
  const [tenant, setTenant] = useState("");
  const [project, setProject] = useState("");
  const [environment, setEnvironment] = useState("");
  const consumers = g.consumers ?? [];
  const projectKey = (c: ConsumerLink) => JSON.stringify([c.tenant, c.project]);
  const environmentKey = (c: ConsumerLink) =>
    JSON.stringify([c.tenant, c.project, c.environment]);
  const tenantRows = consumers.filter((c) => !tenant || c.tenant === tenant);
  const projectRows = tenantRows.filter(
    (c) => !project || projectKey(c) === project,
  );
  const options = (
    items: ConsumerLink[],
    value: (c: ConsumerLink) => string,
    label: (c: ConsumerLink) => string,
  ) =>
    Array.from(
      new Map(
        items.map((c) => [value(c), { value: value(c), label: label(c) }]),
      ).values(),
    ).sort((a, b) => a.label.localeCompare(b.label));
  const rows = projectRows.filter(
    (c) =>
      (!environment || environmentKey(c) === environment) &&
      `${c.tenant} ${c.project} ${c.environment} ${c.service} ${c.database} ${c.role}`
        .toLowerCase()
        .includes(query.toLowerCase()),
  );
  const table = useTableView(
    rows,
    {
      service: (c) => c.service,
      environment: (c) => `${c.project}/${c.environment}`,
      database: (c) => c.database,
    },
    "service",
    "asc",
    JSON.stringify([query, tenant, project, environment]),
  );
  const selected =
    table.rows.find((consumer) => consumer.attachId === selectedId) ??
    table.rows[0];
  return (
    <>
      <ResourcePanel title="Consumer connections">
        <div className="flex flex-wrap items-center gap-2">
          <SearchableSelect
            aria-label="Filter consumers by tenant"
            className="w-full sm:w-44"
            value={tenant}
            onValueChange={(value) => {
              setTenant(value);
              setProject("");
              setEnvironment("");
            }}
            options={[
              { value: "", label: "All tenants" },
              ...options(
                consumers,
                (c) => c.tenant,
                (c) => c.tenant,
              ),
            ]}
          />
          <SearchableSelect
            aria-label="Filter consumers by project"
            className="w-full sm:w-52"
            value={project}
            onValueChange={(value) => {
              setProject(value);
              setEnvironment("");
            }}
            options={[
              { value: "", label: "All projects" },
              ...options(tenantRows, projectKey, (c) =>
                tenant ? c.project : `${c.tenant} / ${c.project}`,
              ),
            ]}
          />
          <SearchableSelect
            aria-label="Filter consumers by environment"
            className="w-full sm:w-60"
            value={environment}
            onValueChange={setEnvironment}
            options={[
              { value: "", label: "All environments" },
              ...options(projectRows, environmentKey, (c) =>
                project
                  ? c.environment
                  : `${tenant ? "" : `${c.tenant} / `}${c.project} / ${c.environment}`,
              ),
            ]}
          />
          {(query || tenant || project || environment) && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => {
                setQuery("");
                setTenant("");
                setProject("");
                setEnvironment("");
              }}
            >
              Clear filters
            </Button>
          )}
        </div>
        {consumers.length ? (
          <InspectionWorkspace
            title="Consumer list"
            selectionLabel="Select consumer"
            detailsLabel="Selected consumer connection"
            emptyMessage="No consumers match your filters."
            selectedId={selected?.attachId}
            onSelect={setSelectedId}
            items={table.rows.map((c) => ({
              id: c.attachId,
              label: c.service,
              description: `${c.tenant} / ${c.project} / ${c.environment}`,
            }))}
            toolbar={
              <ListToolbar
                label="consumers"
                query={query}
                onQueryChange={setQuery}
                sort={table}
                fields={[
                  { value: "service", label: "Service" },
                  { value: "environment", label: "Environment" },
                  { value: "database", label: "Database" },
                ]}
              />
            }
            footer={
              table.pages > 1 && (
                <TablePagination table={table} label="Consumers" />
              )
            }
          >
            {selected && (
              <div key={selected.attachId} className="min-w-0 space-y-4">
                <header className="flex flex-wrap items-start justify-between gap-3 border-b border-border pb-3">
                  <div className="min-w-0 space-y-1">
                    <h3 className="break-words text-sm font-semibold">
                      {selected.service}
                    </h3>
                    <p className="break-words text-xs text-muted-foreground">
                      {selected.tenant} / {selected.project} / {selected.environment}
                    </p>
                  </div>
                  <Link
                    className="text-xs text-primary hover:underline"
                    to={`/t/${selected.tenant}/${selected.project}/${encodeURIComponent(selected.environment)}?view=network&panel=attaches`}
                  >
                    Open Environment connections
                  </Link>
                </header>
                {!adapter?.custom && (
                  <div className="min-w-0 space-y-1.5">
                    <p className="text-xs text-muted-foreground">
                      Connection string
                    </p>
                    <ConsumerConnectionActions
                      consumer={selected}
                      authentication={svc.authentication}
                    />
                  </div>
                )}
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
                    <Row label="Service" value={selected.service} mono />
                    {adapter?.requires.database && (
                      <Row label="Database" value={selected.database} mono />
                    )}
                    {exposesRole && <Row label="Role" value={selected.role} mono />}
                    <Row
                      label="Host"
                      value={`${svc.serviceName}:${adapter?.urlScheme === "redis" ? 6379 : adapter?.urlScheme === "mysql" ? 3306 : 5432}`}
                      mono
                    />
                  </div>
                )}
                {!adapter?.custom && (
                  <Disclosure
                    className="mt-2"
                    summaryClassName="cursor-pointer text-xs text-primary"
                    summary={
                      <>procedure the Agent runs · {provision.length} steps</>
                    }
                  >
                    <div className="mt-2 flex flex-col gap-1 border-t border-border pt-2">
                      {authenticationUnavailable ? (
                        <p className="text-xs text-muted-foreground">
                          Mode-specific provisioning is unavailable until the
                          Controller returns the authentication mode.
                        </p>
                      ) : (
                        provision.length === 0 && (
                          <p className="text-xs text-muted-foreground">
                            No credential procedure; the Attach joins the
                            network and publishes credential-free facts.
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
                              .replaceAll("<db>", selected.database)
                              .replaceAll("<role>", selected.role)
                              .replaceAll("<generated>", "••••••")}
                          </span>
                        </div>
                      ))}
                    </div>
                  </Disclosure>
                )}
              </div>
            )}
          </InspectionWorkspace>
        ) : (
          <p className="text-sm text-muted-foreground">
            No consumer connections yet.
          </p>
        )}
      </ResourcePanel>
      {adapter && (
        <AdvancedDetails
          title={`Provisioning & connection fields · ${svc.adapter}`}
        >
          <div className="flex flex-col gap-3">
            {adapter?.custom ? (
              <p className="text-xs text-muted-foreground">
                Custom services have no managed grants or backups.
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
            ) : null}
            <div className="flex flex-col gap-1.5 text-sm">
              <div className="flex min-w-0 flex-wrap items-start justify-between gap-2 [overflow-wrap:anywhere]">
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
              <div className="flex min-w-0 flex-wrap items-start justify-between gap-2 [overflow-wrap:anywhere]">
                <span className="text-muted-foreground">Healthcheck</span>
                <span className="font-mono text-xs">
                  {svc.healthcheck
                    ? `${describeHealthcheck(svc.healthcheck)} · every ${svc.healthcheck.interval || "Docker default"}`
                    : "none"}
                </span>
              </div>
              <div className="flex min-w-0 flex-wrap items-start justify-between gap-2 [overflow-wrap:anywhere]">
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
