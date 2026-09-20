import type { operations } from "@/lib/api.generated";
import type { Attach, HealthState, Service } from "@/lib/types";
import { controllerRequest } from "@/lib/controller-json-request";
export type AttachPageResponse =
  operations["attach.list"]["responses"][200]["content"]["application/json"];
export type AttachResponse = NonNullable<AttachPageResponse["items"]>[number];
export type AttachCreateRequest =
  operations["attach.create"]["requestBody"]["content"]["application/json"];
export type AttachTaskAccepted =
  operations["attach.create"]["responses"][202]["content"]["application/json"];
export type AttachRenameRequest =
  operations["attach.rename"]["requestBody"]["content"]["application/json"];
export type AttachRenameResponse =
  operations["attach.rename"]["responses"][200]["content"]["application/json"];
export type AttachFactValueResponse =
  operations["attach.fact.reveal"]["responses"][200]["content"]["application/json"];
function attachHealth(status: string): HealthState {
  if (status === "ready") return "healthy";
  if (status === "failed") return "failed";
  if (status === "detached") return "stopped";
  return "pending";
}

export async function revealAttachFactValue(
  attachId: string,
  key: string,
  grantAttachId?: string,
  signal?: AbortSignal,
): Promise<string> {
  const query = new URLSearchParams();
  if (grantAttachId) query.set("grant_attach_id", grantAttachId);
  const suffix = query.size > 0 ? `?${query}` : "";
  const response = await controllerRequest<AttachFactValueResponse>(
    `/attaches/${encodeURIComponent(attachId)}/facts/${encodeURIComponent(key)}${suffix}`,
    200,
    { signal },
  );
  return response.value;
}

async function attachFromAPI(
  attach: AttachResponse,
  services: Service[],
  signal?: AbortSignal,
): Promise<Attach> {
  const factSets = (attach.fact_sets ?? []).map((set) => ({
    grantAttachId: set.grant_attach_id,
    facts: (set.facts ?? []).map((fact) => ({
      key: fact.key,
      secret: fact.secret,
    })),
  }));
  const ready = attach.status === "ready";
  const revealSuffix = async (
    set: (typeof factSets)[number] | undefined,
    suffix: string,
  ) => {
    const fact = set?.facts.find(
      (candidate) => !candidate.secret && candidate.key.endsWith(suffix),
    );
    if (!ready || !fact) return "";
    return revealAttachFactValue(
      attach.id,
      fact.key,
      set?.grantAttachId,
      signal,
    );
  };
  const own = factSets.find((set) => !set.grantAttachId);
  const [database, role, ...grants] = await Promise.all([
    revealSuffix(own, "_DATABASE"),
    revealSuffix(own, "_ROLE"),
    ...factSets
      .filter((set) => set.grantAttachId)
      .map((set) => revealSuffix(set, "_DATABASE")),
  ]);
  const serviceId = attach.service_id;
  return {
    id: attach.id,
    name: attach.name,
    backingProjectId: attach.backing_project_id,
    backingServiceId: attach.backing_service_id,
    backingEnvironmentId: attach.backing_environment_id,
    backingNetworkId: attach.backing_network_id,
    serviceId,
    credential: {
      mode: attach.credential.mode,
      attachId: attach.credential.attach_id,
    },
    grantAttachIds: [...(attach.grant_attach_ids ?? [])],
    factSets,
    projectId: attach.backing_project_id,
    database: database || "—",
    role,
    service:
      services.find((service) => service.id === serviceId)?.name ?? serviceId,
    grants: grants.filter(Boolean),
    status: attachHealth(attach.status),
  };
}

export async function listAllAttaches(
  environmentId: string,
  services: Service[],
  signal?: AbortSignal,
): Promise<Attach[]> {
  const attaches: AttachResponse[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({
      environment: environmentId,
      limit: "200",
    });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<AttachPageResponse>(
      `/attaches?${query}`,
      200,
      { signal },
    );
    attaches.push(...(page.items ?? []));
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return Promise.all(
    attaches.map((attach) => attachFromAPI(attach, services, signal)),
  );
}
