import { EmptyState } from "@/components/common/empty-state";
import { ImageReference } from "@/components/common/image-reference";
import { HelpHint } from "@/components/common/resource-panel";
import { ResourceRow, ResourceTable } from "@/components/common/resource-table";
import {
  CollectionToolbar,
  TablePagination,
  TableSortHead,
  useTableView,
} from "@/components/common/table-controls";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Select } from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { LogViewer } from "@/features/logs/log-viewer";
import { serviceObservationState } from "@/features/service/service-observation";
import { ServiceStateBadges } from "@/features/service/service-runtime-actions";
import { useStore } from "@/lib/store";
import type { Environment, Service } from "@/lib/types";
import { cn } from "@/lib/utils";
import { Boxes, Plug, RefreshCw } from "lucide-react";
import { useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { ServiceDetailsDrawer } from "./service-details-drawer";
import type { ZoneSelection } from "./zone-map";

export function ServicesList({
  env,
  now = Date.now(),
}: {
  env: Environment;
  now?: number;
}) {
  const store = useStore();
  const [search, setSearch] = useSearchParams();
  const selected = env.services.find(
    (service) => service.id === search.get("service"),
  );
  const inspect = (id: string | null) =>
    setSearch((current) => {
      const next = new URLSearchParams(current);
      if (id) next.set("service", id);
      else next.delete("service");
      return next;
    });
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");
  const visible = env.services.filter(
    (service) =>
      `${service.name} ${service.image} ${service.zones.join(" ")}`
        .toLowerCase()
        .includes(query.trim().toLowerCase()) &&
      (status === "all" ||
        serviceObservationState(service.observation, now) === status),
  );
  const table = useTableView(
    visible,
    {
      name: (service) => service.name,
      image: (service) => service.image,
      state: (service) => serviceObservationState(service.observation, now),
      replicas: (service) => service.replicas,
    },
    "name",
    "asc",
    `${env.id}/${query}/${status}`,
  );
  return (
    <>
      <CollectionToolbar
        query={query}
        onQueryChange={setQuery}
        label="Services"
      >
        <Select
          aria-label="Runtime status"
          className="w-auto min-w-44"
          value={status}
          onValueChange={setStatus}
          options={[
            "all",
            "healthy",
            "running",
            "degraded",
            "starting",
            "stopped",
            "failed",
            "absent",
            "unavailable",
          ].map((value) => ({
            value,
            label: value === "all" ? "All runtime states" : value,
          }))}
        />
      </CollectionToolbar>
      <ResourceTable>
        <Table aria-label="Environment Services">
          <TableHeader>
            <TableRow>
              <TableSortHead sort={table} field="name">
                Service
              </TableSortHead>
              <TableSortHead sort={table} field="state">
                Runtime
              </TableSortHead>
              <TableSortHead sort={table} field="image">
                Configured image
              </TableSortHead>
              <TableSortHead sort={table} field="replicas">
                Configuration
              </TableSortHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {table.rows.map((service) => {
              const attached = env.attaches.filter(
                (attach) => attach.service === service.name,
              );
              return (
                <ResourceRow
                  key={service.id}
                  onOpen={() => inspect(service.id)}
                >
                  <TableCell className="min-w-40">
                    <Button
                      variant="ghost"
                      size="content"
                      aria-label={`Inspect ${service.name}`}
                      aria-haspopup="dialog"
                      className="flex-col items-start gap-1 text-left"
                      onClick={() => inspect(service.id)}
                    >
                      <span className="text-sm font-medium">
                        {service.name}
                      </span>
                      {service.role && (
                        <span className="max-w-64 truncate text-xs font-normal text-muted-foreground">
                          {service.role}
                        </span>
                      )}
                    </Button>
                    {attached.length > 0 && (
                      <div className="mt-2 flex flex-wrap gap-1.5">
                        {attached.map((attach) => (
                          <Link
                            key={attach.id}
                            to={`/platform/backing-services/${attach.projectId}`}
                            className="inline-flex items-center gap-1 text-[11px] text-primary hover:underline"
                          >
                            <Plug className="size-3" />
                            {store.getBackingProject(attach.projectId)?.name ??
                              attach.name}
                          </Link>
                        ))}
                      </div>
                    )}
                  </TableCell>
                  <TableCell className="min-w-44">
                    <ServiceStateBadges service={service} compact now={now} />
                  </TableCell>
                  <TableCell className="min-w-48 max-w-72">
                    <ImageReference value={service.image} />
                    <p className="mt-1 text-[11px] text-muted-foreground">
                      {service.zones.join(", ") || "No Zones"}
                    </p>
                  </TableCell>
                  <TableCell className="min-w-32">
                    <p>
                      {service.replicas}{" "}
                      {service.replicas === 1 ? "replica" : "replicas"} ·{" "}
                      {service.strategy}
                    </p>
                    <p className="mt-1 text-[11px] text-muted-foreground">
                      {service.resources.mem}, {service.resources.cpus} CPU
                    </p>
                  </TableCell>
                </ResourceRow>
              );
            })}
          </TableBody>
        </Table>
        {visible.length === 0 && (
          <p className="py-10 text-center text-xs text-muted-foreground">
            No Services match these filters.
          </p>
        )}
      </ResourceTable>
      <TablePagination table={table} label="Services" />
      {selected && (
        <ServiceDetailsDrawer
          key={selected.id}
          env={env}
          service={selected}
          now={now}
          open
          onOpenChange={(open) => {
            if (!open) inspect(null);
          }}
        />
      )}
    </>
  );
}

export function ServiceCard({
  service,
  env,
  selection,
}: {
  service: Service;
  env: Environment;
  selection: ZoneSelection;
}) {
  const store = useStore();
  const attached = env.attaches.filter(
    (attach) => attach.service === service.name,
  );
  const [open, setOpen] = useState(false);
  const selected = selection.selectedId === service.id;
  return (
    <>
      <article
        className={cn(
          "overflow-hidden rounded-xl border bg-background transition-[opacity,border-color] duration-150",
          selected ? "border-primary" : "border-border",
          selection.selectedId && !selected && "opacity-50",
        )}
      >
        <Button
          variant="ghost"
          aria-pressed={selected}
          onClick={() => selection.onSelect(service.id)}
          className="h-auto w-full flex-col items-stretch gap-3 whitespace-normal rounded-none p-4 text-left"
        >
          <span className="flex items-center justify-between gap-2">
            <strong className="text-sm">{service.name}</strong>
            {service.zones.length > 1 && (
              <Badge variant="outline">{service.zones.length} zones</Badge>
            )}
          </span>
          <ServiceStateBadges service={service} compact />
          <ImageReference value={service.image} />
          {service.role && (
            <span className="text-xs text-muted-foreground">
              {service.role}
            </span>
          )}
          <span className="text-[11px] text-muted-foreground">
            {service.resources.mem}, {service.resources.cpus} CPU
          </span>
        </Button>
        {attached.length > 0 && (
          <div className="flex flex-wrap gap-1 border-t border-dashed border-border px-3 py-2">
            {attached.map((attach) => {
              const backing = store.getBackingProject(attach.projectId);
              return (
                <Link
                  key={attach.id}
                  to={`/platform/backing-services/${attach.projectId}`}
                  className="inline-flex items-center gap-1 rounded-md bg-accent px-2 py-1 text-[10px] text-primary"
                >
                  <Plug className="size-3" />
                  {attach.name}, {backing?.name ?? attach.projectId}
                </Link>
              );
            })}
          </div>
        )}
        <div className="flex justify-end gap-1 border-t border-border p-2">
          <LogViewer target={{ kind: "service", id: service.id }} />
          <Button size="xs" variant="ghost" onClick={() => setOpen(true)}>
            Details
          </Button>
        </div>
      </article>
      <ServiceDetailsDrawer
        env={env}
        service={service}
        open={open}
        onOpenChange={setOpen}
      />
    </>
  );
}

export function ServicesPanel({
  env,
  now,
  refreshing,
  onRefresh,
  createAction,
}: {
  env: Environment;
  now: number;
  refreshing: boolean;
  onRefresh: () => void;
  createAction: React.ReactNode;
}) {
  const count = env.services.length;
  return (
    <section
      aria-labelledby="environment-services-heading"
      className="flex flex-col gap-4"
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2
            id="environment-services-heading"
            className="flex items-center gap-2 text-sm font-semibold"
          >
            <Boxes className="size-4 text-muted-foreground" /> Services
          </h2>
          <HelpHint label="About Service configuration">
            Saving configuration does not deploy it. The runtime report shows
            what the Agent currently observes.
          </HelpHint>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            disabled={refreshing}
            onClick={onRefresh}
          >
            <RefreshCw
              className={cn("size-3.5", refreshing && "animate-spin")}
            />
            {refreshing ? "Refreshing runtime" : "Refresh runtime"}
          </Button>
          <Badge variant="outline" className="font-mono">
            {count} {count === 1 ? "service" : "services"}
          </Badge>
          {createAction}
        </div>
      </div>
      {count === 0 ? (
        <EmptyState
          icon={<Boxes />}
          title="No services yet"
          description="Add a service to start building this environment's workload."
          action={createAction}
        />
      ) : (
        <ServicesList env={env} now={now} />
      )}
    </section>
  );
}
