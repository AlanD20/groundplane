import { AnimatedSection } from "@/components/common/animated-section";
import { useState, type ReactNode } from "react";
import type { Environment, Zone } from "@/lib/types";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ResourcePanel } from "@/components/common/resource-panel";
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
        <div className="space-y-4">
          {table.rows.map((z) => (
            <article
              key={z.id}
              className="grid min-w-0 gap-4 rounded-xl border border-border bg-card p-4 lg:grid-cols-[minmax(0,15rem)_2rem_minmax(0,1fr)]"
            >
              <Button
                variant="ghost"
                size="content"
                aria-expanded={selectedZone === z.id}
                onClick={() => select(z.id)}
                className="min-w-0 flex-col items-start justify-start gap-3 whitespace-normal text-left"
              >
                <span className="flex items-center gap-2">
                  <Network className="size-4 text-primary" />
                  <strong className="break-all">{z.name}</strong>
                </span>
                <span className="font-mono text-xs text-muted-foreground">
                  {z.subnet}
                </span>
                <Badge variant="outline">
                  {z.internal ? "Internal" : "Outbound allowed"}
                </Badge>
                <span className="text-xs text-muted-foreground">
                  {members(z).length} Services
                </span>
              </Button>
              <div
                aria-hidden
                className="hidden items-start justify-center pt-2 text-primary lg:flex"
              >
                →
              </div>
              <div className="min-w-0">
                <div className="grid gap-2 sm:grid-cols-2">
                  {members(z).map((s) => (
                    <Button
                      key={s.id}
                      variant={
                        selectedZone === z.id && selectedService === s.id
                          ? "secondary"
                          : "outline"
                      }
                      aria-expanded={
                        selectedZone === z.id && selectedService === s.id
                      }
                      className="h-auto min-h-12 justify-start whitespace-normal break-all text-left"
                      onClick={() => {
                        const closing =
                          selectedZone === z.id && selectedService === s.id;
                        setSelectedZone(closing ? null : z.id);
                        setSelectedService(closing ? null : s.id);
                      }}
                    >
                      <Boxes className="size-4 shrink-0" />
                      {s.name}
                    </Button>
                  ))}
                  {!members(z).length && (
                    <span className="text-xs text-muted-foreground">
                      No Services assigned
                    </span>
                  )}
                </div>
                <AnimatedSection open={selectedZone === z.id}>
                  <div className="mt-4">
                    {selectedZone === z.id && renderZone(z, selection)}
                  </div>
                </AnimatedSection>
              </div>
            </article>
          ))}
          {!!unzoned.length && (
            <article className="rounded-xl border border-dashed border-border p-4">
              <Button
                variant="ghost"
                aria-expanded={selectedZone === "unzoned"}
                onClick={() => select("unzoned")}
              >
                <Boxes />
                No Zone · {unzoned.length} Services
              </Button>
              <AnimatedSection open={selectedZone === "unzoned"}>
                {selectedZone === "unzoned" && renderUnzoned(selection)}
              </AnimatedSection>
            </article>
          )}
        </div>
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
