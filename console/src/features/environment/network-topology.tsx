"use client";

import { ZoneFormDialog } from "@/features/environment/zone-form-dialog";
import { useState } from "react";
import { Link } from "react-router-dom";
import { useRequiredParams } from "@/lib/router";
import { ChevronRight, Database, Plus, Trash2 } from "lucide-react";
import { useStore } from "@/lib/store";
import { Button } from "@/components/ui/button";
import {
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Drawer, DrawerContent } from "@/components/ui/drawer";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { DetailRow } from "@/features/environment/service-details-drawer";
import { ServiceCard } from "@/features/environment/services-list";
import { ZoneMap, type ZoneSelection } from "@/features/environment/zone-map";
import type { Environment, Zone } from "@/lib/types";
import { ServiceFormDialog } from "@/features/service/environment-service-dialog";
import {
  RenameAttach,
  DetachAttach,
} from "@/features/attach/environment-attaches";

export function Topology({ env }: { env: Environment }) {
  const [zoneOpen, setZoneOpen] = useState(false);
  return (
    <>
      <ZoneMap
        env={env}
        actions={
          <>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setZoneOpen(true)}
            >
              <Plus className="size-3.5" /> Zone
            </Button>
            <ServiceFormDialog env={env} />
          </>
        }
        renderZone={(zone, selection) => (
          <ZoneColumn zone={zone} env={env} selection={selection} />
        )}
        renderUnzoned={(selection) => (
          <UnzonedColumn env={env} selection={selection} />
        )}
      />
      <ZoneFormDialog env={env} open={zoneOpen} onOpenChange={setZoneOpen} />
    </>
  );
}

// Services without a zone (e.g. after a zone removal) stay visible here so
// they can never disappear from the topology: they join no network until
// edited back into one. The column is ALWAYS rendered — even with zero
// zones — so unzoned services can never vanish from view.
export function UnzonedColumn({
  env,
  selection,
}: {
  env: Environment;
  selection: ZoneSelection;
}) {
  const unzoned = env.services.filter((s) => s.zones.length === 0);
  return (
    <div className="flex min-w-[240px] flex-1 flex-col gap-2 rounded-xl border border-dashed border-muted-foreground/40 bg-card p-3">
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-sm font-semibold text-muted-foreground">
          no zone
        </span>
        <span className="font-mono text-[11px] text-muted-foreground">
          no network
        </span>
      </div>
      <div className="text-xs text-muted-foreground">
        not attached to any network — they can talk to nothing until you edit
        them into a zone
      </div>
      <div className="mt-1 flex flex-col gap-1.5">
        {unzoned.length === 0 && (
          <div className="text-xs text-muted-foreground/60">no services</div>
        )}
        {unzoned.map((s) => (
          <ServiceCard key={s.id} service={s} env={env} selection={selection} />
        ))}
      </div>
    </div>
  );
}

export function ZoneColumn({
  zone,
  env,
  selection,
}: {
  zone: Zone;
  env: Environment;
  selection: ZoneSelection;
}) {
  const store = useStore();
  const params = useRequiredParams("tenant");
  const [detailOpen, setDetailOpen] = useState(false);
  const [detailZone, setDetailZone] = useState<Zone>();
  const [detailError, setDetailError] = useState<string>();
  const [removeOpen, setRemoveOpen] = useState(false);
  const [removalImpact, setRemovalImpact] =
    useState<Awaited<ReturnType<typeof store.getZoneRemovalImpact>>>();
  const [removalImpactError, setRemovalImpactError] = useState<string>();
  const services = env.services.filter((s) => s.zones.includes(zone.name));
  const attaches = env.attaches.filter((a) => {
    const g = store.getBackingProject(a.projectId);
    return (g?.environments?.[0]?.zones ?? []).some(
      (z) => z.name === zone.name,
    );
  });
  const impactServices = removalImpact?.services ?? [];
  const impactAttaches = removalImpact?.attaches ?? [];
  const impactDatabases = removalImpact?.databases ?? [];
  return (
    <div className="flex min-w-[240px] flex-1 flex-col gap-2 rounded-xl border border-border bg-card p-3">
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-sm font-semibold text-primary">{zone.name}</span>
        <span className="flex items-center gap-1">
          <span className="font-mono text-[11px] text-muted-foreground">
            {zone.subnet}
          </span>
          <Button
            variant="ghost"
            size="icon-xs"
            data-action-id="zone.show"
            aria-label={`Show Zone ${zone.name}`}
            title="Show zone details"
            onClick={() => {
              setDetailOpen(true);
              setDetailZone(undefined);
              setDetailError(undefined);
              void store
                .getZone(zone.id)
                .then(setDetailZone)
                .catch((error: unknown) => {
                  setDetailError(
                    error instanceof Error
                      ? error.message
                      : "Unable to load Zone details",
                  );
                });
            }}
          >
            <ChevronRight className="size-3.5" />
          </Button>
          <Button
            variant="ghost"
            size="icon-xs"
            className="text-muted-foreground hover:text-destructive"
            onClick={() => {
              setRemovalImpact(undefined);
              setRemovalImpactError(undefined);
              setRemoveOpen(true);
              void store
                .getZoneRemovalImpact(zone.id)
                .then(setRemovalImpact)
                .catch((error: unknown) => {
                  setRemovalImpactError(
                    error instanceof Error
                      ? error.message
                      : "Unable to load zone removal impact",
                  );
                });
            }}
            title="Remove zone"
          >
            <Trash2 className="size-3.5" />
          </Button>
        </span>
      </div>
      <div className="text-xs text-muted-foreground">
        {zone.ownerKind === "environment"
          ? "Environment-owned"
          : "Backing Project-owned"}{" "}
        · {zone.internal ? "internal" : "egress allowed"}
      </div>
      <div className="mt-1 flex flex-col gap-1.5">
        {services.length === 0 && (
          <div className="text-xs text-muted-foreground/60">no services</div>
        )}
        {services.map((s) => (
          <ServiceCard key={s.id} service={s} env={env} selection={selection} />
        ))}
        {attaches.map((a) => (
          <div
            key={a.id}
            className="flex items-start gap-1 rounded-lg border border-dashed border-border bg-surface/40 px-2 py-1.5"
          >
            <Link
              to={`/platform/backing-services/${a.projectId}`}
              className="flex min-w-0 flex-1 items-start gap-2 transition-colors hover:border-ring/50"
            >
              <Database className="mt-0.5 size-3.5 shrink-0 text-muted-foreground" />
              <div className="flex min-w-0 flex-col">
                <span className="font-mono text-xs font-medium">
                  {a.projectId}
                </span>
                <span className="truncate font-mono text-[11px] text-muted-foreground">
                  {a.database !== "—"
                    ? `db ${a.database} · role ${a.role}`
                    : "attached"}
                  {a.service ? ` · for ${a.service}` : ""} · open backing
                  service →
                </span>
              </div>
            </Link>
            <div className="flex items-center gap-1">
              <RenameAttach env={env} attach={a} />
              <DetachAttach env={env} attach={a} />
            </div>
          </div>
        ))}
      </div>

      <Drawer open={detailOpen} onOpenChange={setDetailOpen}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>Zone details · {zone.name}</DialogTitle>
          </DialogHeader>
          {!detailZone && !detailError ? (
            <p role="status" className="text-sm text-muted-foreground">
              Loading Zone details...
            </p>
          ) : null}
          {detailError ? (
            <p role="alert" className="text-sm text-destructive">
              {detailError}
            </p>
          ) : null}
          {detailZone ? (
            <div className="flex flex-col">
              <DetailRow label="ID" value={detailZone.id} mono />
              <DetailRow
                label="Environment"
                value={detailZone.environmentId}
                mono
              />
              <DetailRow label="Name" value={detailZone.name} mono />
              <DetailRow label="Subnet" value={detailZone.subnet} mono />
              <DetailRow
                label="Internal"
                value={detailZone.internal ? "yes" : "no"}
              />
              <DetailRow label="Owner kind" value={detailZone.ownerKind} />
              <DetailRow label="Owner ID" value={detailZone.ownerId} mono />
            </div>
          ) : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setDetailOpen(false)}>
              Close
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>

      <TaskRunnerDialog
        open={removeOpen}
        onOpenChange={setRemoveOpen}
        title={`Remove zone · ${zone.name}`}
        description={`Removes ${zone.name} (${zone.subnet}) and disconnects ${services.length} joined service${services.length === 1 ? "" : "s"}.`}
        type="destroy"
        target={zone.id}
        workspace={params.tenant}
        destructive
        confirmText={zone.name}
        startLabel="Remove zone"
        review={
          <div className="flex flex-col gap-2 text-xs">
            {removalImpactError ? (
              <p className="text-destructive">{removalImpactError}</p>
            ) : null}
            {!removalImpact && !removalImpactError ? (
              <p className="text-muted-foreground">
                Loading exact removal impact...
              </p>
            ) : null}
            {removalImpact ? (
              <>
                <p>
                  {impactServices.length} affected service
                  {impactServices.length === 1 ? "" : "s"}
                </p>
                {impactServices.map((service) => (
                  <p key={service.id} className="font-mono">
                    {service.name} ({service.id})
                  </p>
                ))}
                <p>
                  {impactAttaches.length} detach
                  {impactAttaches.length === 1 ? "" : "es"}
                </p>
                {impactAttaches.map((attach) => (
                  <p key={attach.id} className="font-mono">
                    {attach.name} ({attach.id})
                  </p>
                ))}
                <p>
                  {impactDatabases.length} provisioned database
                  {impactDatabases.length === 1 ? "" : "s"}
                </p>
                {impactDatabases.map((database) => (
                  <p key={database.attach_id} className="font-mono">
                    {database.name}
                  </p>
                ))}
              </>
            ) : null}
          </div>
        }
        steps={[
          {
            label: "Validate dependent services and attaches",
            state: "pending",
          },
          { label: `Remove network ${zone.name}`, state: "pending" },
          { label: "Update affected service memberships", state: "pending" },
        ]}
        onDispatch={() => {
          if (!removalImpact)
            return Promise.reject(
              new Error("Exact zone removal impact is not loaded"),
            );
          return store.removeZone(env.id, zone.id, removalImpact.impact_token);
        }}
      />
    </div>
  );
}
