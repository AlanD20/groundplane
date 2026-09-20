import type { operations } from "@/lib/api.generated";
import type { Environment } from "@/lib/types";
import { environmentFromAPI } from "@/lib/environment-projection";
import { listAllZones, listAllRoutes } from "./network-api";
import { listAllServices } from "@/features/service/api";
import { listAllEntries } from "@/features/entry/api";
import { listAllScripts } from "@/features/script/api";
import { listAllVolumes } from "@/features/volume/api";
import { listAllComponents } from "@/features/component/api";
import { listAllAttaches } from "@/features/attach/api";
import { listAllReleases, projectReleaseSummary } from "@/features/release/api";
import { listAllReleaseGroups } from "@/features/release-group/api";
import { controllerRequest } from "@/lib/controller-json-request";
type EnvironmentPageResponse =
  operations["environment.list"]["responses"][200]["content"]["application/json"];
export async function listAllEnvironments(
  projectId: string,
  signal?: AbortSignal,
): Promise<Environment[]> {
  const environments: Environment[] = [];
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
      const environment = environmentFromAPI(item);
      environments.push(environment);
    }
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return Promise.all(
    environments.map(async (environment) => {
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
    }),
  );
}
