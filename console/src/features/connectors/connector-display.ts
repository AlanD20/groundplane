"use client";

import type { Connector, ConnectorCredential } from "@/lib/types";

export function normalizedPrefix(prefix: string) {
  const trimmed = prefix.trim().replace(/^\/+/, "");
  return trimmed === "" || trimmed.endsWith("/") ? trimmed : `${trimmed}/`;
}

export function credentialLabel(credential: ConnectorCredential) {
  return credential.kind === "ref"
    ? `secret ref : ${credential.name}`
    : "direct value : encrypted";
}

export function connectorDocument(
  connector: Connector,
  tenantSlug: string,
  projectSlug: string,
  environmentName: string,
) {
  const credential = (value: ConnectorCredential) =>
    value.kind === "ref" ? { secret_ref: value.name } : { value: "<redacted>" };
  return {
    kind: "connector",
    schema: 1,
    metadata: {
      name: connector.name,
      tenant: tenantSlug,
      project: projectSlug,
      environment: environmentName,
    },
    connector: {
      kind: connector.kind,
      endpoint: connector.endpoint,
      bucket: connector.bucket,
      prefix: connector.prefix,
      region: connector.region,
      path_style: connector.pathStyle,
      credentials: {
        access_key: credential(connector.credentials.accessKey),
        secret_key: credential(connector.credentials.secretKey),
      },
    },
  };
}
