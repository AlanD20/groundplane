import { useState, type ReactNode } from "react";
import type { Environment, Zone } from "@/lib/types";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ResourcePanel } from "@/components/common/resource-panel";
import { workspaceSectionClassName } from "@/components/common/workspace-section";
import {
  TablePagination,
  ListToolbar,
  useTableView,
} from "@/components/common/table-controls";
import { Boxes, Network } from "lucide-react";
import { useSearchParams } from "react-router-dom";

export type ZoneSelection = {
  selectedId: string | null;
  onSelect: (id: string) => void;
};

export function ZoneMap({
  env,
  actions,
  creation,
  renderZone,
  renderUnzoned,
}: {
  env: Environment;
  actions: ReactNode;
  creation?: ReactNode;
  renderZone: (zone: Zone, selection: ZoneSelection) => ReactNode;
  renderUnzoned: (selection: ZoneSelection) => ReactNode;
}) {
  const [selectedZone, setSelectedZone] = useState<string | null>(null);
  const [selectedService, setSelectedService] = useState<string | null>(null);
  const [, setSearch] = useSearchParams();
  const selection: ZoneSelection = {
    selectedId: selectedService,
    onSelect: (id) =>
      setSelectedService((current) => (current === id ? null : id)),
  };
  const members = (zone: Zone) =>
    env.services.filter(
      (s) => s.zones.includes(zone.name) || s.zones.includes(zone.id),
    );
  const select = (id: string) => {
    setSelectedZone((current) => (current === id ? null : id));
    setSelectedService(null);
  };
  const [query, setQuery] = useState("");
  const table = useTableView(
    env.zones.filter((zone) =>
      `${zone.name} ${zone.subnet}`.toLowerCase().includes(query.toLowerCase()),
    ),
    {
      name: (z) => z.name,
      subnet: (z) => z.subnet,
      access: (z) => (z.internal ? "Internal" : "Outbound"),
      services: (z) => members(z).length,
    },
    "name",
    "asc",
    env.id,
  );
  const zone = env.zones.find((z) => z.id === selectedZone);
  const unzoned = env.services.filter((s) => !s.zones.length);
  return (
    <section className="space-y-5" aria-label="Network topology">
      <div className="flex flex-wrap items-center gap-3 rounded-lg border border-border bg-card px-5 py-4">
        <Network className="size-5 text-primary" />
        <div className="min-w-0">
          <strong className="block text-sm">{env.networkPool}</strong>
          <span className="text-xs text-muted-foreground">
            Environment network pool
          </span>
        </div>
        <span className="ml-auto text-xs text-muted-foreground">
          {env.networkCapacity.allocatedAddresses} reserved ·{" "}
          {env.networkCapacity.availableAddresses} available
        </span>
        <Button
          variant="outline"
          size="sm"
          onClick={() => setSearch({ view: "settings" })}
        >
          Edit pool
        </Button>
      </div>
      <ResourcePanel
        title={
          <span className="flex items-center gap-2">
            <Network className="size-4 text-primary" />
            Zone topology
          </span>
        }
        actions={actions}
      >
        {creation}
        <ListToolbar
          label="Zones"
          query={query}
          onQueryChange={setQuery}
          sort={table}
          fields={[
            { value: "name", label: "Name" },
            { value: "subnet", label: "Subnet" },
            { value: "access", label: "Access" },
            { value: "services", label: "Services" },
          ]}
        />
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
          {table.rows.map((z) => (
            <Button
              key={z.id}
              variant="outline"
              size="content"
              aria-pressed={selectedZone === z.id}
              onClick={() => select(z.id)}
              className={workspaceSectionClassName(
                false,
                `flex-col items-start gap-2 text-left ${selectedZone === z.id ? "border-primary bg-accent" : ""}`,
              )}
            >
              <span className="grid size-10 place-items-center rounded-lg bg-accent text-primary">
                <Network className="size-5" />
              </span>
              <strong className="text-xs">{z.name}</strong>
              <span className="font-mono text-xs font-normal text-muted-foreground">
                {z.subnet}
              </span>
              <Badge variant="outline" className="text-xs">
                {z.internal ? "Internal" : "Outbound allowed"}
              </Badge>
              <span className="mt-1 flex flex-wrap gap-1.5">
                {members(z).length ? (
                  members(z).map((s) => (
                    <span
                      key={s.id}
                      className="flex items-center gap-1 text-xs font-normal text-muted-foreground"
                    >
                      <Boxes className="size-2.5" />
                      {s.name}
                    </span>
                  ))
                ) : (
                  <span className="text-xs font-normal text-muted-foreground">
                    No Services assigned
                  </span>
                )}
              </span>
            </Button>
          ))}
          {!!unzoned.length && (
            <Button
              variant="outline"
              size="content"
              onClick={() => select("unzoned")}
              aria-pressed={selectedZone === "unzoned"}
              className="flex-col items-start gap-2 border-dashed p-4"
            >
              <Boxes className="size-5 text-muted-foreground" />
              <strong className="text-xs">No Zone</strong>
              <span className="text-xs font-normal text-muted-foreground">
                {unzoned.length} unconnected Services
              </span>
            </Button>
          )}
        </div>
        {selectedZone && (
          <div className="space-y-3 border-y border-border py-4">
            <div className="flex items-center justify-between">
              <span className="text-xs">{zone?.name ?? "No Zone"}</span>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => select(selectedZone)}
              >
                Clear selection
              </Button>
            </div>
            {zone ? renderZone(zone, selection) : renderUnzoned(selection)}
          </div>
        )}
        {!table.total && (
          <p className="text-sm text-muted-foreground">
            {env.zones.length
              ? "No Zones match your search."
              : "No Zones yet. Add a Zone to connect your Services."}
          </p>
        )}
        <TablePagination table={table} label="Zones" />
      </ResourcePanel>
    </section>
  );
}
