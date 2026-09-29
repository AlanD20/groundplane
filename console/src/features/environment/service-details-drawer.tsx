import { ImageReference } from "@/components/common/image-reference";
import { ServiceFormBody } from "@/components/common/service-form-body";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Drawer, DrawerContent } from "@/components/ui/drawer";
import { Tabs, TabsList, TabsPanel, TabsTab } from "@/components/ui/tabs";
import { LogStream } from "@/features/logs/log-viewer";
import {
  DeployDialog,
  RollbackDialog,
} from "@/features/release/environment-deploy-controls";
import { ServiceConfiguration } from "@/features/service/service-configuration";
import {
  RemoveDesiredServiceButton,
  ServiceObservationDetails,
  ServiceOperationDialog,
  ServiceRuntimeActions,
  ServiceStateBadges,
  type ServiceOperation,
} from "@/features/service/service-runtime-actions";
import { useVisibleServiceObservations } from "@/features/service/use-service-observation-refresh";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import type { Environment, Service } from "@/lib/types";
import { ArrowUpCircle, History, Settings2 } from "lucide-react";
import { useEffect, useRef, useState } from "react";

export function ServiceDetailsDrawer({
  env,
  service: summary,
  now: listNow = Date.now(),
  open,
  onOpenChange,
}: {
  env: Environment;
  service: Service;
  now?: number;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const params = useRequiredParams("tenant");
  const heading = useRef<HTMLHeadingElement>(null);
  const store = useStore();
  const [detail, setDetail] = useState<Service>();
  const [reload, setReload] = useState(0);
  const service = {
    ...summary,
    nativeCompose: detail?.nativeCompose,
    releaseLedger: detail?.releaseLedger ?? summary.releaseLedger,
  };
  const servingReleaseId =
    summary.observation.state === "unavailable"
      ? undefined
      : summary.observation.servingReleaseId;
  const [detailError, setDetailError] = useState<string>();
  const [loading, setLoading] = useState(true);
  const [editing, setEditing] = useState(false);
  const [operation, setOperation] = useState<ServiceOperation | null>(null);
  const [releaseAction, setReleaseAction] = useState<
    "deploy" | "rollback" | null
  >(null);
  const [tab, setTab] = useState("runtime");
  const clock = useVisibleServiceObservations({
    environmentIds: [],
    observations: open ? [service.observation] : [],
    refreshEnvironment: store.refreshEnvironmentServices,
  });
  const now = Math.max(listNow, clock.now);
  useEffect(() => {
    if (!open) {
      setEditing(false);
      setOperation(null);
      setReleaseAction(null);
      return;
    }
    let current = true;
    setDetail(undefined);
    setLoading(true);
    setDetailError(undefined);
    void store
      .getService(summary.id)
      .then((detail) => {
        if (current) setDetail(detail);
      })
      .catch((error: unknown) => {
        if (current)
          setDetailError(
            error instanceof Error
              ? error.message
              : "Unable to load Service details",
          );
      })
      .finally(() => {
        if (current) setLoading(false);
      });
    return () => {
      current = false;
    };
  }, [open, summary.id, servingReleaseId, reload, store.getService]);

  return (
    <>
      <Drawer
        open={open && !editing && !operation && !releaseAction}
        onOpenChange={onOpenChange}
      >
        <DrawerContent initialFocus={heading}>
          <DialogHeader>
            <span className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">
              Service / {env.name}
            </span>
            <DialogTitle
              ref={heading}
              tabIndex={-1}
              className="text-2xl font-medium tracking-tight outline-none"
            >
              {service.name}
            </DialogTitle>
            <ServiceStateBadges service={service} now={now} />
            {service.role && (
              <p className="text-xs text-muted-foreground">{service.role}</p>
            )}
          </DialogHeader>
          <div className="flex flex-wrap items-center gap-2">
            <Button variant="outline" onClick={() => setTab("logs")}>
              Logs
            </Button>
            <Button variant="outline" onClick={() => setEditing(true)}>
              <Settings2 className="size-3.5" />
              Configure
            </Button>
            <Button onClick={() => setReleaseAction("deploy")}>
              <ArrowUpCircle className="size-3.5" />
              Deploy
            </Button>
          </div>
          {detailError && (
            <p
              role="alert"
              className="rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-xs text-destructive"
            >
              {detailError}
            </p>
          )}
          <Tabs
            value={tab}
            onValueChange={(value) => setTab(String(value))}
            className="min-w-0 flex-1"
          >
            <TabsList aria-label="Service sections">
              <TabsTab value="runtime">Runtime</TabsTab>
              <TabsTab value="configuration">Configuration</TabsTab>
              <TabsTab value="logs">Logs</TabsTab>
              <TabsTab value="history">History</TabsTab>
            </TabsList>
            <TabsPanel value="runtime" className="flex flex-col gap-5 pt-5">
              <ServiceObservationDetails service={service} now={now} />
              <ServiceRuntimeActions
                service={service}
                onAction={setOperation}
              />
              <details className="rounded-lg border border-border p-3">
                <summary className="text-xs font-medium">
                  Technical details
                </summary>
                <dl className="mt-3 space-y-2 text-xs text-muted-foreground">
                  <dt>Service ID</dt>
                  <dd className="break-all font-mono">{service.id}</dd>
                  <dt>Ownership labels</dt>
                  <dd className="break-all font-mono">
                    com.groundplane.managed=true, com.groundplane.service-id=
                    {service.id}
                  </dd>
                </dl>
              </details>
            </TabsPanel>
            <TabsPanel value="configuration" className="pt-5">
              <ServiceConfiguration
                service={service}
                loading={loading}
                error={detailError}
              />
            </TabsPanel>
            <TabsPanel value="logs" className="pt-5">
              <LogStream target={{ kind: "service", id: service.id }} />
            </TabsPanel>
            <TabsPanel value="history" className="space-y-4 pt-5">
              <div className="flex items-center justify-between">
                <h3 className="text-sm font-medium">Release history</h3>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setReleaseAction("rollback")}
                >
                  <History className="size-3.5" />
                  Rollback
                </Button>
              </div>
              {service.releaseLedger?.length ? (
                <div className="divide-y divide-border rounded-lg border border-border">
                  {service.releaseLedger.map((release) => (
                    <div
                      key={release.id}
                      className="flex min-w-0 items-center justify-between gap-3 p-3"
                    >
                      <div className="min-w-0 flex-1">
                        <ImageReference value={release.tag} />
                        <details className="mt-1 text-[11px] text-muted-foreground">
                          <summary>Exact reference</summary>
                          <p className="break-all font-mono">
                            {release.digest || release.id}
                          </p>
                        </details>
                      </div>
                      <Badge
                        variant={
                          release.status === "active" ? "primary" : "outline"
                        }
                      >
                        {release.status}
                      </Badge>
                    </div>
                  ))}
                </div>
              ) : (
                <p role="status" className="text-xs text-muted-foreground">
                  {loading ? "Loading Release history…" : "No Releases yet."}
                </p>
              )}
            </TabsPanel>
          </Tabs>
          <DialogFooter className="sm:justify-between">
            <RemoveDesiredServiceButton
              onClick={() => setOperation("remove")}
            />
            <Button variant="outline" onClick={() => onOpenChange(false)}>
              Close
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>
      <Drawer open={open && editing} onOpenChange={onOpenChange}>
        {open && editing && (
          <ServiceFormBody
            key={service.id}
            env={env}
            workspace={params.tenant}
            initial={service}
            onClose={() => {
              setEditing(false);
              setReload((value) => value + 1);
            }}
          />
        )}
      </Drawer>
      {open && releaseAction === "deploy" && (
        <DeployDialog
          env={env}
          serviceId={service.id}
          open
          onOpenChange={(value) => {
            if (!value) setReleaseAction(null);
          }}
        />
      )}
      {open && releaseAction === "rollback" && (
        <RollbackDialog
          env={env}
          serviceId={service.id}
          open
          onOpenChange={(value) => {
            if (!value) setReleaseAction(null);
          }}
        />
      )}
      <ServiceOperationDialog
        env={env}
        service={service}
        operation={operation}
        workspace={params.tenant}
        onOpenChange={(value) => {
          if (!value) setOperation(null);
        }}
        onRemoved={() => {
          setOperation(null);
          onOpenChange(false);
        }}
      />
    </>
  );
}
