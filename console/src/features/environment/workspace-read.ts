import type { operations } from "@/lib/api.generated";
import type { Environment } from "@/lib/types";
import {
  environmentFromAPI,
  environmentMetadataFromAPI,
} from "@/features/environment/projection";
import { listAllZones, listAllRoutes } from "./network-api";
import { listAllServices } from "@/features/service/api";
import { listAllEntries } from "@/features/entry/api";
import { listAllScripts } from "@/features/script/api";
import { listAllVolumes } from "@/features/volume/api";
import { listAllComponents } from "@/features/component/api";
import { listAllAttaches } from "@/features/attach/api";
import { listAllReleases, projectReleaseSummary } from "@/features/release/api";
import { listAllReleaseGroups } from "@/features/release-group/api";
import { ControllerRequestError } from "@/lib/controller-request-errors";
import { controllerRequest } from "@/lib/controller-json-request";
type EnvironmentPageResponse =
  operations["environment.list"]["responses"][200]["content"]["application/json"];
async function listEnvironmentDocuments(
  projectId: string,
  signal?: AbortSignal,
) {
  const environments: NonNullable<EnvironmentPageResponse["items"]> = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({ project: projectId, limit: "200" });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<EnvironmentPageResponse>(
      `/environments?${query}`,
      200,
      { signal },
    );
    for (const item of page.items ?? []) {
      environments.push(item);
    }
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return environments;
}

export async function listEnvironmentMetadata(
  projectId: string,
  signal?: AbortSignal,
) {
  return (await listEnvironmentDocuments(projectId, signal)).map(
    environmentMetadataFromAPI,
  );
}

export async function listAllEnvironments(
  projectId: string,
  signal?: AbortSignal,
): Promise<Environment[]> {
  const environments = (await listEnvironmentDocuments(projectId, signal)).map(
    environmentFromAPI,
  );
  return Promise.all(
    environments.map(async (environment) => {
      try {
        return await loadEnvironmentResources(environment, signal);
      } catch (error) {
        if (signal?.aborted) throw error;
        return {
          ...environment,
          resourceLoadError:
            error instanceof Error
              ? error.message
              : "Unable to load Environment resources",
        };
      }
    }),
  );
}

export async function readEnvironment(
  environmentId: string,
  signal?: AbortSignal,
): Promise<Environment | null> {
  const response = await controllerRequest<
    operations["environment.show"]["responses"][200]["content"]["application/json"]
  >(`/environments/${encodeURIComponent(environmentId)}`, 200, {
    signal,
  }).catch((cause: unknown) => {
    if (cause instanceof ControllerRequestError && cause.status === 404)
      return null;
    throw cause;
  });
  if (!response) return null;
  return loadEnvironmentResources(environmentFromAPI(response), signal);
}

async function loadEnvironmentResources(
  environment: Environment,
  signal?: AbortSignal,
): Promise<Environment> {
  const [zones, routes, services, entries, scripts, volumes, components] =
    await Promise.all([
      listAllZones(environment.id, signal),
      listAllRoutes(environment.id, signal),
      listAllServices(environment.id, signal),
      listAllEntries(environment.id, signal),
      listAllScripts(environment.id, signal),
      listAllVolumes(controllerRequest, environment.id, signal),
      listAllComponents(environment.id, signal),
    ]);
  const [attaches, deploys, releaseGroups] = await Promise.all([
    listAllAttaches(environment.id, services, signal),
    listAllReleases(environment.id, services, signal),
    listAllReleaseGroups(environment.id, services, signal),
  ]);
  return {
    ...environment,
    ...projectReleaseSummary(deploys),
    zones,
    routes,
    services,
    entries,
    scripts,
    attaches,
    deploys,
    releaseGroups,
    volumes,
    components,
  };
}
