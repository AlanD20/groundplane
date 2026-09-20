import type { operations } from "@/lib/api.generated";
import type { Project, Tenant } from "@/lib/types";
import {
  projectFromAPI,
  type ProjectShowResponse,
} from "@/features/project/api";
import { environmentFromAPI } from "@/features/environment/projection";
import {
  serviceFromAPI,
  type ServiceShowResponse,
} from "@/features/service/api";
import { listAllZones } from "@/features/environment/network-api";
import { listAllEntries } from "@/features/entry/api";
import { backingConsumers } from "./consumer-projection";
import { controllerRequest } from "@/lib/controller-json-request";
type EnvironmentResponse =
  operations["environment.show"]["responses"][200]["content"]["application/json"];
type BackingServicePageResponse =
  operations["backing-service.list"]["responses"][200]["content"]["application/json"];
type BackingServiceResponse = NonNullable<
  BackingServicePageResponse["items"]
>[number];
export async function listAllBackingProjects(
  tenantProjects: Project[],
  tenants: Tenant[],
  signal: AbortSignal,
): Promise<Project[]> {
  const facades: BackingServiceResponse[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({ limit: "200" });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<BackingServicePageResponse>(
      `/backing-services?${query}`,
      200,
      { signal },
    );
    facades.push(...(page.items ?? []));
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return Promise.all(
    facades.map(async (facade) => {
      const [
        projectResponse,
        environmentResponse,
        serviceResponse,
        zones,
        entries,
      ] = await Promise.all([
        controllerRequest<ProjectShowResponse>(
          `/projects/${encodeURIComponent(facade.project_id)}`,
          200,
          { signal },
        ),
        controllerRequest<EnvironmentResponse>(
          `/environments/${encodeURIComponent(facade.environment_id)}`,
          200,
          { signal },
        ),
        controllerRequest<ServiceShowResponse>(
          `/services/${encodeURIComponent(facade.service_id)}`,
          200,
          { signal },
        ),
        listAllZones(facade.environment_id, signal),
        listAllEntries(facade.environment_id, signal),
      ]);
      const service = {
        ...serviceFromAPI(serviceResponse),
        authentication: facade.authentication,
      };
      const environment = {
        ...environmentFromAPI(environmentResponse),
        zones,
        services: [service],
        entries,
        routes: [],
        attaches: [],
        components: [],
        backup: undefined,
      };
      const project = projectFromAPI(projectResponse);
      return {
        ...project,
        environments: [environment],
        status: environment.status,
        consumers: backingConsumers(facade.project_id, tenantProjects, tenants),
      };
    }),
  );
}
