import { Disclosure } from "@/components/common/disclosure";
("use client");

import { ResourceForm } from "@/components/common/resource-form";
import { CopyButton } from "@/components/common/copy-button";
import type { Connector } from "@/lib/types";
import { toYAML } from "@/lib/yaml";
import { credentialLabel, connectorDocument } from "./connector-display";
import { getConnectorEditSnapshot, connectorSnapshotRevision } from "./api";
import { ConnectorCheckPanel } from "./connector-check-panel";
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
      {connector && (
        <div className="flex flex-col gap-4">
          <dl className="grid grid-cols-1 gap-x-6 gap-y-4 sm:grid-cols-2 xl:grid-cols-3 text-sm">
            <Detail wide label="Endpoint" value={connector.endpoint} />
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
          <ConnectorCheckPanel
            environmentId={connector.scopeRef}
            identity={connector.id}
            connectorId={connector.id}
            open
            getRequest={async () => {
              const snapshot = await getConnectorEditSnapshot(
                connector.id,
                new AbortController().signal,
              );
              return {
                connector_id: connector.id,
                revision: connectorSnapshotRevision(snapshot),
              };
            }}
          />
          <Disclosure summary="YAML" summaryClassName="text-xs font-medium">
            <div className="mt-3 flex flex-col gap-1.5">
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
          </Disclosure>
        </div>
      )}
    </ResourceForm>
  );
}

function Detail({
  label,
  value,
  copyable = true,
  wide = false,
}: {
  label: string;
  value: string;
  copyable?: boolean;
  wide?: boolean;
}) {
  return (
    <div
      className={
        wide
          ? "col-span-full flex min-w-0 flex-col gap-1"
          : "flex min-w-0 flex-col gap-1"
      }
    >
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
