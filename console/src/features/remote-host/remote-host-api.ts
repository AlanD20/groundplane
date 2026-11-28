import type { components, operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";

export type RemoteHost = components["schemas"]["RemoteHost"];
export type RemoteHostEnrollment =
  components["schemas"]["RemoteHostEnrollment"];
export type RemoteHostPage =
  operations["remote-host.list"]["responses"][200]["content"]["application/json"];

const remoteHosts = "/remote-hosts";

export function listRemoteHosts(cursor: string, signal: AbortSignal) {
  const query = new URLSearchParams({ limit: "10" });
  if (cursor) query.set("cursor", cursor);
  return controllerRequest<RemoteHostPage>(`${remoteHosts}?${query}`, 200, {
    signal,
  });
}

function hostPath(id: string) {
  return `${remoteHosts}/${encodeURIComponent(id)}`;
}

export function readRemoteHost(id: string, signal: AbortSignal) {
  return controllerRequest<RemoteHost>(hostPath(id), 200, { signal });
}

export function fetchRemoteImage(
  host: string,
  image: string,
  idempotencyKey: string,
) {
  const body = {
    image,
  } satisfies components["schemas"]["RemoteImageFetchRequest"];
  type Receipt =
    operations["remote-host.fetch-image"]["responses"][202]["content"]["application/json"];
  return controllerRequest<Receipt>(
    `/hosts/${encodeURIComponent(host)}/images/fetch`,
    202,
    {
      method: "POST",
      body,
      idempotencyKey,
    },
  );
}

export function createRemoteHost(name: string, idempotencyKey: string) {
  const body = {
    name,
  } satisfies components["schemas"]["RemoteHostCreateRequest"];
  return controllerRequest<RemoteHost>(remoteHosts, 201, {
    method: "POST",
    body,
    idempotencyKey,
  });
}

export function stageRemoteEnrollment(
  host: RemoteHost,
  csr: string,
  idempotencyKey: string,
) {
  const body = {
    csr,
    expected_generation: host.generation,
  } satisfies components["schemas"]["RemoteHostEnrollmentRequest"];
  return controllerRequest<RemoteHostEnrollment>(
    `${hostPath(host.id)}/enrollment`,
    200,
    {
      method: "POST",
      body,
      idempotencyKey,
    },
  );
}

export function revokeRemoteEnrollment(
  host: RemoteHost,
  idempotencyKey: string,
) {
  return controllerRequest<void>(
    `${hostPath(host.id)}/enrollment?${new URLSearchParams({ generation: String(host.generation) })}`,
    204,
    {
      method: "DELETE",
      idempotencyKey,
    },
  );
}
