import type { operations } from "@/lib/api.generated";
import type { Zone, Route } from "@/lib/types";
import { controllerRequest } from "@/lib/controller-json-request";

export type ZonePageResponse =
  operations["zone.list"]["responses"][200]["content"]["application/json"];
export type ZoneCreateRequest =
  operations["zone.create"]["requestBody"]["content"]["application/json"];
export type ZoneCreateResponse =
  operations["zone.create"]["responses"][201]["content"]["application/json"];
export type ZoneRemoveResponse =
  operations["zone.remove"]["responses"][202]["content"]["application/json"];
export type ZoneRemovalImpactResponse =
  operations["zone.removal-impact"]["responses"][200]["content"]["application/json"];
export type RoutePageResponse =
  operations["route.list"]["responses"][200]["content"]["application/json"];
export type RouteCreateRequest =
  operations["route.create"]["requestBody"]["content"]["application/json"];
export type GeneratedRouteResponse =
  operations["route.show"]["responses"][200]["content"]["application/json"];
export type RouteCreateResponse = GeneratedRouteResponse & {
  status: Route["status"];
};
export type RouteCreateAccepted = {
  route: RouteCreateResponse;
  task_id: string;
};
export type RouteEditRequest =
  operations["route.edit"]["requestBody"]["content"]["application/json"];
export type RouteEditResponse = RouteCreateResponse;
export type RouteEditAccepted = { route: RouteEditResponse; task_id: string };
export type RouteShowResponse = RouteCreateResponse;
export type ZoneShowResponse =
  operations["zone.show"]["responses"][200]["content"]["application/json"];

export function zoneFromAPI(zone: ZoneCreateResponse | ZoneShowResponse): Zone {
  if (
    zone.owner_kind !== "environment" &&
    zone.owner_kind !== "backing_project"
  ) {
    throw new Error(
      `Controller returned unknown Zone owner kind ${zone.owner_kind}`,
    );
  }
  return {
    id: zone.id,
    environmentId: zone.environment_id,
    name: zone.name,
    subnet: zone.subnet,
    internal: zone.internal,
    ownerKind: zone.owner_kind,
    ownerId: zone.owner_id,
  };
}

export async function listAllZones(
  environmentId: string,
  signal?: AbortSignal,
): Promise<Zone[]> {
  const zones: Zone[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({
      environment: environmentId,
      limit: "200",
    });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<ZonePageResponse>(
      `/zones?${query}`,
      200,
      { signal },
    );
    zones.push(...(page.items ?? []).map(zoneFromAPI));
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return zones;
}

export function routeFromAPI(
  route: RouteCreateResponse | RouteEditResponse | RouteShowResponse,
): Route {
  if (route.exposure !== "public" && route.exposure !== "internal") {
    throw new Error(
      `Controller returned unknown Route exposure ${route.exposure}`,
    );
  }
  if (
    route.status !== "unserved" &&
    route.status !== "pending" &&
    route.status !== "served" &&
    route.status !== "degraded"
  ) {
    throw new Error(`Controller returned unknown Route status ${route.status}`);
  }
  return {
    id: route.id,
    environmentId: route.environment_id,
    host: route.host ?? "",
    path: route.path,
    exposure: route.exposure,
    targetServiceId: route.target_service_id,
    targetPort: route.target_port,
    status: route.status,
  };
}

export async function listAllRoutes(
  environmentId: string,
  signal?: AbortSignal,
): Promise<Route[]> {
  const routes: Route[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({
      environment: environmentId,
      limit: "200",
    });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<RoutePageResponse>(
      `/routes?${query}`,
      200,
      { signal },
    );
    routes.push(
      ...(page.items ?? []).map((route) =>
        routeFromAPI(route as RouteShowResponse),
      ),
    );
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return routes;
}
