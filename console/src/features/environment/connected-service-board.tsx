import { EmptyState } from "@/components/common/empty-state";
import { IconTile } from "@/components/common/icon-tile";
import { ImageReference } from "@/components/common/image-reference";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { DeployDialog } from "@/features/release/environment-deploy-controls";
import {
  currentServiceObservation,
  replicaTotal,
} from "@/features/service/service-observation";
import { useStore } from "@/lib/store";
import type { Environment } from "@/lib/types";
import {
  ArrowUpRight,
  Boxes,
  Clock,
  Database,
  GitBranch,
  Network,
  Play,
  Search,
  Settings,
  Terminal,
} from "lucide-react";
import { useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { formatTimestamp } from "@/lib/format-timestamp";

export function ConnectedServiceBoard({
  env,
  now,
}: {
  env: Environment;
  now: number;
}) {
  const store = useStore();
  const [, setSearch] = useSearchParams();
  const [query, setQuery] = useState("");
  const [zone, setZone] = useState<string | null>(null);
  const [connections, setConnections] = useState(true);
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
  const attachedProjects = [...new Set(env.attaches.map((a) => a.projectId))];
  const running = env.services.filter((s) =>
    ["healthy", "running"].includes(
      currentServiceObservation(s.observation, now).state,
    ),
  ).length;
  const inspect = (id: string, tab = "overview") =>
    setSearch((current) => {
      const next = new URLSearchParams(current);
      next.set("service", id);
      next.set("serviceTab", tab);
      return next;
    });
  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-3 text-[10px] text-muted-foreground">
          <span className="flex items-center gap-1.5">
            <span className="size-1.5 rounded-full bg-success" />
            {running} running
          </span>
          <span className="h-3 border-l border-border" />
          {env.zones.length} Zones
        </div>
        <div className="flex flex-wrap items-center gap-4">
          <label className="relative w-52">
            <Search className="pointer-events-none absolute left-3 top-3 size-3.5 text-muted-foreground" />
            <Input
              aria-label="Search connected Services"
              placeholder="Find a Service…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="pl-9 text-[11px]"
            />
          </label>
          <Button
            variant="ghost"
            size="sm"
            aria-pressed={connections}
            onClick={() => setConnections(!connections)}
            className="px-0 text-[10px] text-primary"
          >
            <GitBranch className="size-3.5" />
            {connections ? "Hide" : "Show"} connections
          </Button>
        </div>
      </div>
      <nav
        className="flex flex-wrap gap-1.5"
        aria-label="Filter Services by Zone"
      >
        {[
          { id: "all", name: "All Zones" },
          ...env.zones,
          ...(env.services.some((s) => !s.zones.length)
            ? [{ id: "none", name: "No Zone" }]
            : []),
        ].map((z) => {
          const selected = z.id === "all" ? zone === null : zone === z.id;
          const count =
            z.id === "all"
              ? env.services.length
              : env.services.filter((s) =>
                  z.id === "none" ? !s.zones.length : inZone(s.zones, z.id),
                ).length;
          return (
            <Button
              key={z.id}
              variant="outline"
              size="sm"
              aria-pressed={selected}
              onClick={() =>
                setZone(z.id === "all" || zone === z.id ? null : z.id)
              }
              className={`gap-1.5 rounded-md text-[10px] ${selected ? "border-primary bg-accent text-primary" : "bg-transparent text-muted-foreground"}`}
            >
              {z.id !== "all" && <Network className="size-3" />}
              {z.name}
              <span className="text-[9px] text-foreground">{count}</span>
            </Button>
          );
        })}
      </nav>
      <div className="connected-workspace overflow-hidden rounded-lg border border-border px-5.5 pb-5.5 pt-5">
        <h2 className="mb-4 text-[8px] font-normal uppercase tracking-[0.12em] text-muted-foreground">
          {env.zones.find((z) => z.id === zone)?.name ??
            (zone === "none" ? "No Zone" : "All Zones")}{" "}
          / {services.length} Services
        </h2>
        {services.length ? (
          <div className="grid gap-5 md:grid-cols-2 xl:grid-cols-3">
            {services.map((s) => {
              const observation = currentServiceObservation(s.observation, now);
              const bindings = env.attaches.filter((a) => a.service === s.name);
              return (
                <article
                  key={s.id}
                  className="flex min-w-0 flex-col overflow-hidden rounded-lg border border-border bg-card shadow-sm transition-[border-color,transform] duration-150 hover:-translate-y-0.5 hover:border-primary"
                >
                  <Button
                    variant="ghost"
                    size="content"
                    onClick={() => inspect(s.id)}
                    className="w-full flex-col items-stretch gap-0 rounded-none px-4 pb-3 pt-4 text-left hover:bg-transparent"
                  >
                    <span className="flex items-center gap-2.5">
                      <IconTile>
                        <Boxes />
                      </IconTile>
                      <span className="min-w-0 flex-1">
                        <strong className="block truncate text-xs">
                          {s.name}
                        </strong>
                        {s.role && (
                          <span className="mt-0.5 block truncate text-[10px] font-normal text-muted-foreground">
                            {s.role}
                          </span>
                        )}
                      </span>
                      <ArrowUpRight className="size-3.5 text-muted-foreground" />
                    </span>
                    <span className="mb-3 mt-4 flex flex-wrap items-center justify-between gap-2">
                      <StatusBadge
                        status={
                          observation.state === "running"
                            ? "degraded"
                            : observation.state
                        }
                        label={
                          observation.state === "running"
                            ? "Running · no healthcheck"
                            : undefined
                        }
                      />
                      <span className="text-[9px] font-normal text-muted-foreground">
                        {observation.state === "unavailable"
                          ? "Not reported"
                          : `${replicaTotal(observation.replicas)} containers`}
                      </span>
                    </span>
                    <span className="flex min-w-0 items-center gap-1.5 text-[10px] font-normal text-muted-foreground">
                      <Boxes className="size-3" />
                      <ImageReference value={s.image} />
                    </span>
                  </Button>
                  {connections && (
                    <div className="grid gap-1.5 border-t border-dashed border-border px-3.5 py-2">
                      <div className="flex flex-wrap gap-1">
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
                                className="h-5 gap-1 rounded-sm px-1.5 text-[9px] font-normal"
                                onClick={() =>
                                  z && setZone(zone === z.id ? null : z.id)
                                }
                              >
                                <Network className="size-2.5" />
                                {z?.name ?? ref}
                              </Button>
                            );
                          })
                        ) : (
                          <span className="text-[9px] text-muted-foreground">
                            No Zone
                          </span>
                        )}
                      </div>
                      {!!bindings.length && (
                        <div className="flex flex-wrap gap-1">
                          {bindings.map((a) => (
                            <Button
                              key={a.id}
                              nativeButton={false}
                              render={
                                <Link
                                  to={`/platform/backing-services/${a.projectId}`}
                                />
                              }
                              variant="secondary"
                              size="xs"
                              className="h-5 gap-1 rounded-sm px-1.5 text-[9px] font-normal"
                            >
                              <Database className="size-2.5" />
                              {store.getBackingProject(a.projectId)?.name ??
                                a.name}
                            </Button>
                          ))}
                        </div>
                      )}
                    </div>
                  )}
                  <div className="mt-auto flex items-center justify-between border-t border-border px-3.5 py-1.5">
                    <Button
                      variant="ghost"
                      size="sm"
                      className="gap-1 px-0 text-[9px] text-muted-foreground"
                      onClick={() => inspect(s.id, "logs")}
                    >
                      <Terminal className="size-3" />
                      Logs
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="gap-1 px-0 text-[9px] text-muted-foreground"
                      onClick={() => inspect(s.id, "configuration")}
                    >
                      <Settings className="size-3" />
                      Configure
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="gap-1 px-0 text-[9px] text-primary"
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
              env.services.length
                ? "No Services in this view"
                : "No Services yet"
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
        {connections && !!attachedProjects.length && (
          <div className="mt-6 border-t border-dashed border-border pt-4">
            <div className="mb-3 flex flex-wrap items-center gap-2 text-[8px] uppercase tracking-wider text-muted-foreground">
              Attached shared services
              <span className="normal-case tracking-normal">
                Outside this Environment
              </span>
            </div>
            <div className="flex flex-wrap gap-3">
              {attachedProjects.map((id) => {
                const p = store.getBackingProject(id);
                const backing = p?.environments?.[0]?.services[0];
                const observation = backing
                  ? currentServiceObservation(backing.observation, now)
                  : undefined;
                return (
                  <Link
                    key={id}
                    to={`/platform/backing-services/${id}`}
                    className="flex min-w-48 items-center gap-3 rounded-lg border border-border bg-card px-4 py-3 transition-colors hover:border-primary"
                  >
                    <Database className="size-5 text-primary" />
                    <div className="min-w-0 flex-1">
                      <strong className="block truncate text-[11px]">
                        {p?.name ?? "Backing Service"}
                      </strong>
                      <span className="mt-1 block text-[9px] text-muted-foreground">
                        {env.attaches.filter((a) => a.projectId === id).length}{" "}
                        Attaches
                      </span>
                    </div>
                    <StatusBadge
                      status={observation?.state ?? "unavailable"}
                      className="text-[9px]"
                    />
                  </Link>
                );
              })}
            </div>
          </div>
        )}
      </div>
      <div className="flex flex-wrap items-center gap-2 text-[10px] text-muted-foreground">
        <Clock className="size-3.5" />
        Last deploy{" "}
        <span className="text-foreground">
          {formatTimestamp(env.lastDeployAt, "Never")}
        </span>
        <Button
          variant="ghost"
          size="sm"
          className="ml-auto px-0 text-[10px] text-primary"
          onClick={() => setSearch({ view: "operations", panel: "tasks" })}
        >
          Task history
          <ArrowUpRight className="size-3" />
        </Button>
      </div>
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
