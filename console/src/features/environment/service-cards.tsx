import { EmptyState } from "@/components/common/empty-state";
import { ImageReference } from "@/components/common/image-reference";
import { Button } from "@/components/ui/button";
import { Select } from "@/components/ui/select";
import { ServiceStateBadges } from "@/features/service/service-runtime-actions";
import { Input } from "@/components/ui/input";
import { DeployDialog } from "@/features/release/environment-deploy-controls";
import type { Environment } from "@/lib/types";
import {
  ArrowUpRight,
  Boxes,
  Plug,
  Network,
  Play,
  Search,
  Terminal,
} from "lucide-react";
import { useState } from "react";
import { useSearchParams } from "react-router-dom";

export function ServiceCards({ env, now }: { env: Environment; now: number }) {
  const [, setSearch] = useSearchParams();
  const [query, setQuery] = useState("");
  const [zone, setZone] = useState<string | null>(null);
  const [deployService, setDeployService] = useState<string | null>(null);
  const inZone = (refs: string[], id: string) =>
    refs.some(
      (ref) => ref === id || ref === env.zones.find((z) => z.id === id)?.name,
    );
  const services = env.services.filter(
    (s) =>
      `${s.name} ${s.image}`
        .toLowerCase()
        .includes(query.trim().toLowerCase()) &&
      (!zone || (zone === "none" ? !s.zones.length : inZone(s.zones, zone))),
  );
  const inspect = (id: string, tab = "overview") =>
    setSearch((current) => {
      const next = new URLSearchParams(current);
      next.set("service", id);
      next.set("serviceTab", tab);
      return next;
    });
  return (
    <div className="space-y-4">
      {env.services.length > 0 && (
        <div className="flex flex-wrap gap-3">
          <label className="relative min-w-0 flex-1 basis-56">
            <Search
              aria-hidden="true"
              className="pointer-events-none absolute left-3 top-3 size-4 text-muted-foreground"
            />
            <Input
              aria-label="Search Services"
              placeholder="Find a Service…"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              className="pl-9"
            />
          </label>
          <Select
            aria-label="Filter Services by Zone"
            className="w-auto min-w-40"
            value={zone ?? "all"}
            onValueChange={(value) => setZone(value === "all" ? null : value)}
            options={[
              { value: "all", label: "All Zones" },
              ...env.zones.map((entry) => ({
                value: entry.id,
                label: entry.name,
              })),
              ...(env.services.some((service) => !service.zones.length)
                ? [{ value: "none", label: "No Zone" }]
                : []),
            ]}
          />
        </div>
      )}
      {services.length ? (
        <div className="grid gap-4 md:grid-cols-2 2xl:grid-cols-3">
          {services.map((s) => {
            const bindings = env.attaches.filter((a) => a.service === s.name);
            return (
              <article
                key={s.id}
                className="flex min-w-0 flex-col overflow-hidden rounded-lg border border-border bg-card"
              >
                <Button
                  variant="ghost"
                  size="content"
                  onClick={() => inspect(s.id)}
                  className="w-full flex-col items-stretch gap-0 rounded-none px-4 pb-3 pt-4 text-left hover:bg-transparent"
                >
                  <span className="flex items-center gap-2.5">
                    <span className="min-w-0 flex-1">
                      <strong className="block whitespace-normal text-sm [overflow-wrap:anywhere]">
                        {s.name}
                      </strong>
                      {s.role && (
                        <span className="mt-0.5 block whitespace-normal text-xs font-normal text-muted-foreground">
                          {s.role}
                        </span>
                      )}
                    </span>
                    <ArrowUpRight className="size-3.5 text-muted-foreground" />
                  </span>
                  <span className="mb-3 mt-4 flex flex-wrap items-center justify-between gap-2">
                    <ServiceStateBadges service={s} now={now} />
                  </span>
                  <span className="flex min-w-0 items-center gap-1.5 text-xs font-normal text-muted-foreground">
                    <Boxes className="size-3" />
                    <ImageReference value={s.image} />
                  </span>
                </Button>
                <div className="flex flex-wrap items-center gap-x-4 gap-y-2 px-4 pb-4 text-xs text-muted-foreground">
                  <span className="flex min-w-0 items-start gap-1.5">
                    <Network aria-hidden="true" className="size-3.5 shrink-0" />
                    <span className="[overflow-wrap:anywhere]">
                      {s.zones
                        .map(
                          (ref) =>
                            env.zones.find(
                              (entry) => entry.id === ref || entry.name === ref,
                            )?.name ?? ref,
                        )
                        .join(", ") || "No Zone"}
                    </span>
                  </span>
                  <Button
                    variant="ghost"
                    size="content"
                    className="text-xs text-primary"
                    onClick={() => inspect(s.id, "connections")}
                  >
                    <Plug aria-hidden="true" className="size-3.5" />
                    {bindings.length
                      ? `${bindings.length} backing ${bindings.length === 1 ? "connection" : "connections"}`
                      : "Connect backing service"}
                  </Button>
                </div>
                <div className="mt-auto flex flex-wrap items-center justify-between gap-2 border-t border-border px-4 py-2">
                  <Button
                    variant="ghost"
                    size="sm"
                    className="gap-1 px-0 text-xs text-muted-foreground"
                    onClick={() => inspect(s.id, "logs")}
                  >
                    <Terminal className="size-3" />
                    Logs
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="gap-1 px-0 text-xs text-muted-foreground"
                    onClick={() => inspect(s.id, "configuration")}
                  >
                    Runtime & image
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="gap-1 px-0 text-xs text-primary"
                    onClick={() => setDeployService(s.id)}
                  >
                    <Play className="size-3" />
                    Deploy
                  </Button>
                </div>
              </article>
            );
          })}
        </div>
      ) : (
        <EmptyState
          icon={<Boxes />}
          title={
            env.services.length ? "No Services in this view" : "No Services yet"
          }
          description={
            env.services.length
              ? "Try another Zone or search."
              : "Create a Service or apply a Blueprint."
          }
          action={
            env.services.length ? (
              <Button
                variant="outline"
                onClick={() => {
                  setQuery("");
                  setZone(null);
                }}
              >
                Show all Services
              </Button>
            ) : undefined
          }
        />
      )}
      {deployService && (
        <DeployDialog
          key={deployService}
          env={env}
          serviceId={deployService}
          open
          onOpenChange={(open) => !open && setDeployService(null)}
        />
      )}
    </div>
  );
}
