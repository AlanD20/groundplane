import { ResourceRow, ResourceTable } from "@/components/common/resource-table";
import {
  CollectionToolbar,
  TablePagination,
  TableSortHead,
  useTableView,
} from "@/components/common/table-controls";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
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
  const [selected, setSelected] = useState<ConsumerLink | null>(null);
  const rows = (g.consumers || []).filter((c) =>
    `${c.project} ${c.environment} ${c.service} ${c.database} ${c.role}`
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
    query,
  );
  return (
    <>
      <ResourcePanel title="Consumer connections">
        <CollectionToolbar
          query={query}
          onQueryChange={setQuery}
          label="Consumers"
        />
        <ResourceTable>
          <Table
            className="min-w-[640px] table-fixed [overflow-wrap:anywhere]"
            aria-label="Backing Service consumers"
          >
            <TableHeader>
              <TableRow>
                <TableSortHead sort={table} field="service">
                  Service
                </TableSortHead>
                <TableSortHead sort={table} field="environment">
                  Environment
                </TableSortHead>
                <TableSortHead sort={table} field="database">
                  Database
                </TableSortHead>
                <TableHead className="w-[40%]">Connection</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {table.rows.map((c) => (
                <ResourceRow key={c.attachId} onOpen={() => setSelected(c)}>
                  <TableCell>
                    <Button
                      variant="ghost"
                      size="content"
                      className="justify-start text-xs"
                      onClick={() => setSelected(c)}
                    >
                      {c.service}
                    </Button>
                  </TableCell>
                  <TableCell>
                    <Link
                      to={`/t/${c.tenant}/${c.project}/${encodeURIComponent(c.environment)}?view=network&panel=attaches`}
                      className="text-xs text-primary hover:underline"
                    >
                      {c.project}/{c.environment}
                    </Link>
                  </TableCell>
                  <TableCell>{c.database || "—"}</TableCell>
                  <TableCell className="w-[40%] min-w-0 whitespace-normal">
                    {!adapter?.custom ? (
                      <ConsumerConnectionActions
                        consumer={c}
                        authentication={svc.authentication}
                      />
                    ) : svc.hooks?.attach ? (
                      "Custom provisioning"
                    ) : (
                      "Network access"
                    )}
                  </TableCell>
                </ResourceRow>
              ))}
            </TableBody>
          </Table>
        </ResourceTable>
        {!rows.length && (
          <p className="py-5 text-xs text-muted-foreground">
            {g.consumers?.length
              ? "No matching consumers."
              : "No consumer Attaches yet."}
          </p>
        )}
        <TablePagination table={table} label="Consumers" />
      </ResourcePanel>
      {adapter && (
        <AdvancedDetails
          title={`Provisioning & connection fields · ${svc.adapter}`}
        >
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
                    ? `${svc.healthcheck.kind} ${svc.healthcheck.target} · every ${svc.healthcheck.interval}`
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

      <Dialog
        open={Boolean(selected)}
        onOpenChange={(open) => {
          if (!open) setSelected(null);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{selected?.service} connection</DialogTitle>
          </DialogHeader>
          {selected && (
            <>
              <div
                key={`${selected.environment}-${selected.service}-${selected.attachId}`}
                className="min-w-0 rounded-xl border border-border bg-card p-4"
              >
                <div className="flex min-w-0 flex-col gap-3">
                  <span className="min-w-0 font-mono text-sm [overflow-wrap:anywhere]">
                    {selected.project} / {selected.environment}{" "}
                    <span className="text-muted-foreground">
                      · {selected.service}
                    </span>
                  </span>
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
                    <Row label="Service" value={selected.service} mono />
                    {adapter?.requires.database && (
                      <Row label="Database" value={selected.database} mono />
                    )}
                    {exposesRole && (
                      <Row label="Role" value={selected.role} mono />
                    )}
                    <Row
                      label="Host"
                      value={`${svc.serviceName}:${adapter?.urlScheme === "redis" ? 6379 : adapter?.urlScheme === "mysql" ? 3306 : 5432}`}
                      mono
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
                  </details>
                )}
              </div>
            </>
          )}
        </DialogContent>
      </Dialog>
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
