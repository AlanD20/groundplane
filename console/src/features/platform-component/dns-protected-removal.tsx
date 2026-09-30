import { useEffect, useState, type ReactNode } from "react";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useStore } from "@/lib/store";
import { coreDNSConfigInput } from "./core-dns-config";

// DNS changes have their own Task and must finish before presenting the
// resource's existing removal confirmation. Cancelling keeps both resources.
export function DNSProtectedRemoval({
  open,
  onOpenChange,
  serviceId,
  zoneId,
  onReferencesRemoved,
  children,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  serviceId?: string;
  zoneId?: string;
  onReferencesRemoved?: () => Promise<void>;
  children: ReactNode;
}) {
  const store = useStore();
  const [loaded, setLoaded] = useState(false);
  const [released, setReleased] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    if (!open) {
      setLoaded(false);
      setReleased(false);
      return;
    }
    let cancelled = false;
    setLoaded(false);
    setError(null);
    void store
      .refreshPlatformComponents()
      .then(() => {
        if (!cancelled) setLoaded(true);
      })
      .catch((cause) => {
        if (!cancelled)
          setError(
            cause instanceof Error
              ? cause.message
              : "Unable to load DNS references",
          );
      });
    return () => {
      cancelled = true;
    };
  }, [open, serviceId, zoneId, store.refreshPlatformComponents]);

  const records = store.platform.dns.records ?? [];
  const references = records.filter(
    (record) =>
      (serviceId && record.service_id === serviceId) ||
      (zoneId && record.zone_id === zoneId),
  );
  if (!open || released || (loaded && references.length === 0)) return children;
  if (!loaded)
    return (
      <Dialog open onOpenChange={onOpenChange}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Check DNS references</DialogTitle>
          </DialogHeader>
          <p
            role={error ? "alert" : "status"}
            className="text-sm text-muted-foreground"
          >
            {error ?? "Loading current DNS records…"}
          </p>
          <DialogFooter>
            <Button variant="outline" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    );
  const component = store.platform.components.find(
    (value) => value.kind === "coredns",
  );
  return (
    <TaskRunnerDialog
      open
      onOpenChange={onOpenChange}
      title="Remove DNS references first"
      type="update"
      target={component?.id ?? ""}
      workspace="platform"
      destructive
      startLabel="Remove DNS records and continue"
      steps={[
        {
          label: "Apply DNS configuration without these records",
          state: "pending",
        },
      ]}
      description="Keep these records by cancelling. To remove this resource, first remove its DNS records and wait for that Task to complete."
      review={
        <div className="space-y-2">
          <ul className="space-y-1 font-mono text-sm">
            {references.map((record) => (
              <li key={record.hostname}>{record.hostname}</li>
            ))}
          </ul>
          {error && (
            <p role="alert" className="text-sm text-destructive">
              {error}
            </p>
          )}
        </div>
      }
      onDispatch={async () => {
        const dns = store.platform.dns;
        if (
          !component ||
          dns.upstream === undefined ||
          dns.upstreamAuto === undefined ||
          dns.tailnetDelegation === undefined ||
          dns.corefileTemplate === undefined ||
          dns.forwarders === undefined
        )
          throw new Error("Complete CoreDNS configuration is not available.");
        return store.updateComponentConfig(
          component.id,
          coreDNSConfigInput({
            upstream: dns.upstream,
            upstreamAuto: dns.upstreamAuto,
            tailnetDelegation: dns.tailnetDelegation,
            corefileTemplate: dns.corefileTemplate,
            forwarders: dns.forwarders,
            records: records.filter((record) => !references.includes(record)),
          }),
        );
      }}
      onCommit={() => {
        void (async () => {
          await store.refreshPlatformComponents();
          await onReferencesRemoved?.();
          setReleased(true);
        })().catch((cause) =>
          setError(
            cause instanceof Error
              ? cause.message
              : "Unable to refresh removal details",
          ),
        );
      }}
    />
  );
}
