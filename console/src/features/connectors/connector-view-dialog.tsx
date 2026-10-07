"use client";

import {
  ResourceForm,
  ResourceFormHeader as DialogHeader,
  ResourceFormTitle as DialogTitle,
  ResourceFormDescription as DialogDescription,
  ResourceFormFooter,
} from "@/components/common/resource-form";
import { Button } from "@/components/ui/button";
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
    <ResourceForm
      editing={false}
      open={connector !== null}
      onOpenChange={onOpenChange}
    >
      <DialogHeader>
        <DialogTitle>Backup destination · {connector?.name}</DialogTitle>
        <DialogDescription>
          Where this Environment stores backups. Credentials remain hidden.
        </DialogDescription>
      </DialogHeader>
      {connector && (
        <div className="flex flex-col gap-4">
          <dl className="grid gap-4 divide-y divide-border text-sm [&>div]:pt-3">
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
              copyable={false}
            />
            <Detail
              label="Secret key"
              value={credentialLabel(connector.credentials.secretKey)}
              copyable={false}
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
      <ResourceFormFooter>
        <Button variant="outline" onClick={() => onOpenChange(false)}>
          Close details
        </Button>
      </ResourceFormFooter>
    </ResourceForm>
  );
}

function Detail({
  label,
  value,
  copyable = true,
}: {
  label: string;
  value: string;
  copyable?: boolean;
}) {
  return (
    <div className="flex min-w-0 flex-col gap-1">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="min-w-0 break-all font-mono text-xs">
        {copyable && value ? (
          <CopyButton
            value={value}
            label={`Copy ${label.toLowerCase()}`}
            className="max-w-full justify-start px-0 py-0.5 font-mono text-xs text-foreground"
          >
            {value}
          </CopyButton>
        ) : (
          value || "—"
        )}
      </dd>
    </div>
  );
}
