import type { operations } from "@/lib/api.generated";
import type { Connector, ConnectorCredential } from "@/lib/types";
import { controllerRequest } from "@/lib/controller-json-request";
export type ConnectorPageResponse =
  operations["connector.list"]["responses"][200]["content"]["application/json"];
export type ConnectorResponse =
  operations["connector.create"]["responses"][201]["content"]["application/json"];
export type ConnectorCreateRequest =
  operations["connector.create"]["requestBody"]["content"]["application/json"];
export type ConnectorTaskAccepted =
  operations["connector.remove"]["responses"][202]["content"]["application/json"];

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
