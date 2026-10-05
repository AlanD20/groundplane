import { EmptyState } from "@/components/common/empty-state";
import { ImageReference } from "@/components/common/image-reference";
import { ResourcePanel } from "@/components/common/resource-panel";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ServiceStateBadges } from "@/features/service/service-runtime-actions";
import { useStore } from "@/lib/store";
import type { Environment } from "@/lib/types";
import { Boxes, Database, Network, Search } from "lucide-react";
import { useState } from "react";
import { Link, useSearchParams } from "react-router-dom";

export function ConnectedEnvironment({
  env,
  now,
  createAction,
}: {
  env: Environment;
  now: number;
  createAction: React.ReactNode;
}) {
  const store = useStore();
  const [, setSearch] = useSearchParams();
  const [query, setQuery] = useState("");
  const [zone, setZone] = useState<string | null>(null);
  const matchesZone = (refs: string[], id: string) =>
    refs.some(
      (ref) => ref === id || ref === env.zones.find((z) => z.id === id)?.name,
    );
  const services = env.services.filter(
    (s) =>
      s.name.toLowerCase().includes(query.trim().toLowerCase()) &&
      (!zone ||
        (zone === "none" ? s.zones.length === 0 : matchesZone(s.zones, zone))),
  );
  const attachedProjects = [...new Set(env.attaches.map((a) => a.projectId))];
  const inspect = (id: string) =>
    setSearch((current) => {
      const next = new URLSearchParams(current);
      next.set("service", id);
      return next;
    });
  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div className="flex flex-wrap gap-2">
          <Button
            variant={zone === null ? "secondary" : "outline"}
            size="sm"
            aria-pressed={zone === null}
            onClick={() => setZone(null)}
          >
            All Zones
          </Button>
          {env.zones.map((z) => (
            <Button
              key={z.id}
              variant={zone === z.id ? "secondary" : "outline"}
              size="sm"
              aria-pressed={zone === z.id}
              onClick={() => setZone(zone === z.id ? null : z.id)}
            >
              <Network className="size-3.5" />
              {z.name}
            </Button>
          ))}
          <Button
            variant={zone === "none" ? "secondary" : "outline"}
            size="sm"
            aria-pressed={zone === "none"}
            onClick={() => setZone(zone === "none" ? null : "none")}
          >
            No Zone
          </Button>
        </div>
        <label className="relative min-w-44">
          <Search className="pointer-events-none absolute left-3 top-3 size-4 text-muted-foreground" />
          <Input
            aria-label="Search connected Services"
            placeholder="Find a Service…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            className="pl-9"
          />
        </label>
      </div>
      <div className="connected-workspace rounded-xl border border-border bg-surface/20 p-4 sm:p-6">
        <div className="mb-5 flex items-center justify-between gap-3">
          <h2 className="flex items-center gap-2 text-xs font-medium text-muted-foreground">
            <Boxes className="size-4" />
            Services <span>{services.length}</span>
          </h2>
          {createAction}
        </div>
        {services.length ? (
          <div className="grid gap-4 md:grid-cols-2 2xl:grid-cols-3">
            {services.map((s) => (
              <article
                key={s.id}
                className="min-w-0 overflow-hidden rounded-xl border border-border bg-card transition-colors hover:border-primary/40"
              >
                <Button
                  variant="ghost"
                  size="content"
                  onClick={() => inspect(s.id)}
                  className="w-full flex-col items-stretch gap-3 rounded-none p-5 text-left"
                >
                  <span className="flex items-center justify-between gap-3">
                    <span className="flex min-w-0 items-center gap-2">
                      <Boxes className="size-4 shrink-0 text-primary" />
                      <strong className="truncate text-sm">{s.name}</strong>
                    </span>
                  </span>
                  <ServiceStateBadges service={s} compact now={now} />
                  <ImageReference value={s.image} />
                  {s.role && (
                    <span className="truncate text-xs text-muted-foreground">
                      {s.role}
                    </span>
                  )}
                </Button>
                <div className="flex flex-wrap gap-1.5 border-t border-border px-5 py-3">
                  {s.zones.length ? (
                    s.zones.map((ref) => {
                      const z = env.zones.find(
                        (z) => z.id === ref || z.name === ref,
                      );
                      return (
                        <Button
                          key={ref}
                          variant="secondary"
                          size="xs"
                          aria-pressed={zone === z?.id}
                          onClick={() =>
                            z && setZone(zone === z.id ? null : z.id)
                          }
                        >
                          <Network className="size-3" />
                          {z?.name ?? ref}
                        </Button>
                      );
                    })
                  ) : (
                    <span className="text-[11px] text-muted-foreground">
                      No Zone
                    </span>
                  )}
                </div>
                {env.attaches.some((a) => a.service === s.name) && (
                  <div className="flex flex-wrap gap-2 border-t border-border px-5 py-3">
                    {env.attaches
                      .filter((a) => a.service === s.name)
                      .map((a) => (
                        <Link
                          key={a.id}
                          to={`/platform/backing-services/${a.projectId}`}
                          className="flex items-center gap-1 text-[11px] text-primary hover:underline"
                        >
                          <Database className="size-3" />
                          {store.getBackingProject(a.projectId)?.name ?? a.name}
                        </Link>
                      ))}
                  </div>
                )}
              </article>
            ))}
          </div>
        ) : (
          <EmptyState
            icon={<Boxes />}
            title={
              env.services.length ? "No matching Services" : "No Services yet"
            }
            description={
              env.services.length
                ? "Choose another Zone or clear the search."
                : "Create a Service or apply a Blueprint."
            }
            action={
              !env.services.length ? (
                createAction
              ) : (
                <Button
                  variant="outline"
                  onClick={() => {
                    setQuery("");
                    setZone(null);
                  }}
                >
                  Clear filters
                </Button>
              )
            }
          />
        )}
      </div>
      {attachedProjects.length > 0 && (
        <ResourcePanel title="Shared infrastructure">
          <div className="flex flex-wrap gap-3">
            {attachedProjects.map((id) => {
              const p = store.getBackingProject(id);
              return (
                <Link
                  key={id}
                  to={`/platform/backing-services/${id}`}
                  className="flex items-center gap-3 rounded-lg border border-border px-4 py-3 hover:border-primary/40"
                >
                  <Database className="size-4 text-primary" />
                  <div>
                    <span className="text-xs font-medium">
                      {p?.name ?? "Backing Service"}
                    </span>
                    <p className="mt-1 text-[11px] text-muted-foreground">
                      {env.attaches.filter((a) => a.projectId === id).length}{" "}
                      Attaches
                    </p>
                  </div>
                </Link>
              );
            })}
          </div>
        </ResourcePanel>
      )}
    </div>
  );
}
