"use client";

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { CopyButton } from "@/components/common/copy-button";
import type { Connector } from "@/lib/types";
import { toYAML } from "@/lib/yaml";
import { credentialLabel, connectorDocument } from "./connector-display";
export function ConnectorViewDialog({
  connector,
  onOpenChange,
  tenantSlug,
  projectSlug,
  environmentName,
}: {
  connector: Connector | null;
  onOpenChange: (open: boolean) => void;
  tenantSlug: string;
  projectSlug: string;
  environmentName: string;
}) {
  return (
    <Dialog open={connector !== null} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>Connector : {connector?.name}</DialogTitle>
          <DialogDescription>
            Environment-owned destination and redacted desired state.
          </DialogDescription>
        </DialogHeader>
        {connector && (
          <div className="flex flex-col gap-4">
            <dl className="grid gap-3 rounded-lg border border-border bg-surface p-3 text-sm sm:grid-cols-2">
              <Detail label="Endpoint" value={connector.endpoint} />
              <Detail label="Region" value={connector.region} />
              <Detail label="Bucket" value={connector.bucket} />
              <Detail label="Prefix" value={connector.prefix} />
              <Detail
                label="Addressing"
                value={
                  connector.pathStyle ? "path style" : "virtual hosted style"
                }
              />
              <Detail
                label="Access key"
                value={credentialLabel(connector.credentials.accessKey)}
              />
              <Detail
                label="Secret key"
                value={credentialLabel(connector.credentials.secretKey)}
              />
            </dl>
            <div className="flex flex-col gap-1.5">
              <div className="flex items-center justify-between gap-2">
                <span className="font-mono text-xs text-muted-foreground">
                  connector.yaml
                </span>
                <CopyButton
                  value={toYAML(
                    connectorDocument(
                      connector,
                      tenantSlug,
                      projectSlug,
                      environmentName,
                    ),
                  )}
                />
              </div>
              <pre className="overflow-x-auto rounded-lg border border-border bg-background p-4 font-mono text-xs leading-relaxed">
                {toYAML(
                  connectorDocument(
                    connector,
                    tenantSlug,
                    projectSlug,
                    environmentName,
                  ),
                )}
              </pre>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

function Detail({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex min-w-0 flex-col gap-1">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="break-all font-mono text-xs">{value}</dd>
    </div>
  );
}
