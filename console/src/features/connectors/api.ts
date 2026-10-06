import type { operations } from "@/lib/api.generated";
import type {
  Connector,
  ConnectorCredential,
  ConnectorCredentialInput,
  ConnectorEditInput,
} from "@/lib/types";
import { controllerRequest } from "@/lib/controller-json-request";
export type ConnectorPageResponse =
  operations["connector.list"]["responses"][200]["content"]["application/json"];
export type ConnectorResponse =
  operations["connector.create"]["responses"][201]["content"]["application/json"];
export type ConnectorCreateRequest =
  operations["connector.create"]["requestBody"]["content"]["application/json"];
export type ConnectorTaskAccepted =
  operations["connector.remove"]["responses"][202]["content"]["application/json"];
export type ConnectorEditRequest =
  operations["connector.edit"]["requestBody"]["content"]["application/json"];
type ConnectorCredentialWrite = NonNullable<
  NonNullable<ConnectorEditRequest["credentials"]>["access_key"]
>;
export type ConnectorEditSnapshot = {
  connector: Connector;
  etag: string;
};

export function connectorFromAPI(connector: ConnectorResponse): Connector {
  const credential = (
    name: "access_key" | "secret_key",
  ): ConnectorCredential => {
    const source = connector.credentials[name];
    if (!source)
      throw new Error(
        `Controller returned Connector ${connector.id} without ${name}`,
      );
    if (source.kind === "secret_ref" && source.secret_ref)
      return { kind: "ref", name: source.secret_ref };
    if (source.kind === "direct") return { kind: "direct" };
    throw new Error(`Controller returned invalid Connector credential ${name}`);
  };
  if (connector.kind !== "s3-compatible") {
    throw new Error(
      `Controller returned unknown Connector kind ${connector.kind}`,
    );
  }
  return {
    id: connector.id,
    name: connector.name,
    kind: connector.kind,
    scope: "environment",
    scopeRef: connector.environment_id,
    endpoint: connector.endpoint,
    bucket: connector.bucket,
    prefix: connector.prefix ?? "",
    region: connector.region,
    pathStyle: connector.path_style,
    credentials: {
      accessKey: credential("access_key"),
      secretKey: credential("secret_key"),
    },
  };
}

export async function listEnvironmentConnectors(
  environmentId: string,
  signal: AbortSignal,
): Promise<Connector[]> {
  const connectors: Connector[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({
      environment: environmentId,
      limit: "200",
    });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<ConnectorPageResponse>(
      `/connectors?${query}`,
      200,
      { signal },
    );
    connectors.push(...(page.items ?? []).map(connectorFromAPI));
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return connectors;
}

export async function getConnectorEditSnapshot(
  connectorId: string,
  signal: AbortSignal,
): Promise<ConnectorEditSnapshot> {
  let etag: string | null = null;
  const connector = connectorFromAPI(
    await controllerRequest<ConnectorResponse>(
      `/connectors/${encodeURIComponent(connectorId)}`,
      200,
      {
        signal,
        onResponseHeaders: (headers) => {
          etag = headers.get("ETag");
        },
      },
    ),
  );
  if (!etag || !/^"[^"]+"$/.test(etag)) {
    throw new Error("Controller response is missing a quoted Connector ETag");
  }
  return { connector, etag };
}

export function connectorEditRequest(input: ConnectorEditInput) {
  const credential = (
    value: ConnectorCredentialInput | undefined,
  ): ConnectorCredentialWrite | undefined => {
    if (!value) return undefined;
    return value.kind === "ref"
      ? { secret_ref: value.name }
      : { value: value.value };
  };
  const credentials: NonNullable<ConnectorEditRequest["credentials"]> = {};
  const accessKey = credential(input.credentials?.accessKey);
  const secretKey = credential(input.credentials?.secretKey);
  if (accessKey) credentials.access_key = accessKey;
  if (secretKey) credentials.secret_key = secretKey;
  return {
    name: input.name,
    endpoint: input.endpoint,
    bucket: input.bucket,
    prefix: input.prefix,
    region: input.region,
    path_style: input.pathStyle,
    credentials: accessKey || secretKey ? credentials : undefined,
  } satisfies ConnectorEditRequest;
}

export async function listAllConnectors(
  environmentIds: string[],
  signal: AbortSignal,
): Promise<Connector[]> {
  return (
    await Promise.all(
      environmentIds.map((id) => listEnvironmentConnectors(id, signal)),
    )
  ).flat();
}
