import { useRef, type MutableRefObject } from "react";
import type { Zone, Route } from "@/lib/types";
import { controllerRequest } from "@/lib/controller-json-request";
import {
  findEnvironment,
  type EnvironmentRemovalDraft,
  type EnvironmentMutationKind,
  type PendingResourceRemoval,
} from "./environment-removal-model";
import type { TaskResponse } from "@/features/task/api";
import {
  zoneFromAPI,
  routeFromAPI,
  type ZoneCreateRequest,
  type ZoneCreateResponse,
  type ZoneRemoveResponse,
  type ZoneRemovalImpactResponse,
  type RouteCreateRequest,
  type RouteCreateAccepted,
  type RouteEditRequest,
  type RouteEditAccepted,
  type RouteShowResponse,
  type ZoneShowResponse,
} from "./network-api";

export type NetworkActions = {
  addZone: (
    envId: string,
    input: { name: string; subnet: string; internal: boolean },
  ) => Promise<Zone>;
  getZone: (zoneId: string) => Promise<Zone>;
  getZoneRemovalImpact: (zoneId: string) => Promise<ZoneRemovalImpactResponse>;
  removeZone: (
    envId: string,
    zoneId: string,
    impactToken: string,
  ) => Promise<string>;
  addRoute: (
    envId: string,
    route: Omit<Route, "id" | "environmentId" | "status">,
  ) => Promise<Route>;
  getRoute: (routeId: string) => Promise<Route>;
  updateRoute: (
    envId: string,
    routeId: string,
    patch: Pick<Route, "exposure">,
  ) => Promise<Route>;
  removeRoute: (envId: string, routeId: string) => Promise<string>;
};
type NetworkWorkspace = {
  update: (change: (draft: EnvironmentRemovalDraft) => void) => void;
  assertEnvironmentMutable: (environmentId: string, operation: string) => void;
  nextEnvironmentGeneration: (
    environmentId: string,
    kind?: EnvironmentMutationKind,
  ) => number;
  environmentGenerations: MutableRefObject<Map<string, number>>;
  dispatchResourceRemoval: (removal: PendingResourceRemoval) => Promise<string>;
};
export function useNetworkActions({
  update,
  assertEnvironmentMutable,
  nextEnvironmentGeneration,
  environmentGenerations,
  dispatchResourceRemoval,
}: NetworkWorkspace) {
  const pendingZoneRemovals = useRef(
    new Map<string, { envId: string; zoneId: string }>(),
  );

  const reconcileZoneRemoval = (taskId: string, task: TaskResponse) => {
    const pending = pendingZoneRemovals.current.get(taskId);
    if (
      pending &&
      ["completed", "failed", "timed_out", "aborted"].includes(task.status)
    ) {
      pendingZoneRemovals.current.delete(taskId);
      if (task.status === "completed") {
        update((draft) => {
          const environment = findEnvironment(draft, pending.envId);
          if (!environment) return;
          const zone = environment.zones.find(
            (candidate) => candidate.id === pending.zoneId,
          );
          if (!zone) return;
          environment.zones = environment.zones.filter(
            (candidate) => candidate.id !== pending.zoneId,
          );
          environment.services.forEach((service) => {
            service.zones = service.zones.filter((name) => name !== zone.name);
          });
        });
      }
    }
  };
  const actions: NetworkActions = {
    addZone: async (envId, input) => {
      assertEnvironmentMutable(envId, "Zone mutation");
      const body: ZoneCreateRequest = {
        environment_id: envId,
        name: input.name,
        subnet: input.subnet,
        internal: input.internal,
      };
      const zone = zoneFromAPI(
        await controllerRequest<ZoneCreateResponse>("/zones", 201, {
          method: "POST",
          body,
        }),
      );
      update((draft) => {
        findEnvironment(draft, envId)?.zones.push(zone);
      });
      return zone;
    },
    getZone: async (zoneId) =>
      zoneFromAPI(
        await controllerRequest<ZoneShowResponse>(
          `/zones/${encodeURIComponent(zoneId)}`,
          200,
          { method: "GET" },
        ),
      ),
    getZoneRemovalImpact: (zoneId) =>
      controllerRequest<ZoneRemovalImpactResponse>(
        `/zones/${encodeURIComponent(zoneId)}/removal-impact`,
        200,
        { method: "GET" },
      ),
    removeZone: async (envId, zoneId, impactToken) => {
      assertEnvironmentMutable(envId, "Zone mutation");
      const accepted = await controllerRequest<ZoneRemoveResponse>(
        `/zones/${encodeURIComponent(zoneId)}${impactToken ? `?impact_token=${encodeURIComponent(impactToken)}` : ""}`,
        202,
        { method: "DELETE" },
      );
      if (!accepted.task_id)
        throw new Error("Controller response is missing task_id");
      pendingZoneRemovals.current.set(accepted.task_id, { envId, zoneId });
      return accepted.task_id;
    },
    addRoute: async (envId, route) => {
      assertEnvironmentMutable(envId, "Route mutation");
      const generation = nextEnvironmentGeneration(envId, "child");
      const body: RouteCreateRequest = {
        environment_id: envId,
        host: route.host || undefined,
        path: route.path,
        exposure: route.exposure,
        target_service_id: route.targetServiceId,
        target_port: route.targetPort,
      };
      const accepted = await controllerRequest<RouteCreateAccepted>(
        "/routes",
        202,
        {
          method: "POST",
          body,
        },
      );
      const created = routeFromAPI(accepted.route);
      update((d) => {
        // Public Routes never auto-enable ingress; Component lifecycle is not authored by C07.
        if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
          return;
        findEnvironment(d, envId)?.routes.push(created);
      });
      return created;
    },
    getRoute: async (routeId) =>
      routeFromAPI(
        await controllerRequest<RouteShowResponse>(
          `/routes/${encodeURIComponent(routeId)}`,
          200,
          { method: "GET" },
        ),
      ),
    updateRoute: async (envId, routeId, patch) => {
      assertEnvironmentMutable(envId, "Route mutation");
      const generation = nextEnvironmentGeneration(envId, "child");
      const body: RouteEditRequest = { exposure: patch.exposure };
      const accepted = await controllerRequest<RouteEditAccepted>(
        `/routes/${encodeURIComponent(routeId)}`,
        202,
        { method: "PATCH", body },
      );
      const edited = routeFromAPI(accepted.route);
      update((d) => {
        if ((environmentGenerations.current.get(envId) ?? 0) !== generation)
          return;
        const route = findEnvironment(d, envId)?.routes.find(
          (candidate) => candidate.id === routeId,
        );
        if (!route) return;
        Object.assign(route, edited);
      });
      return edited;
    },
    removeRoute: (envId, routeId) => (
      assertEnvironmentMutable(envId, "Route mutation"),
      dispatchResourceRemoval({
        kind: "route",
        environmentId: envId,
        resourceId: routeId,
      })
    ),
  };
  return { actions, reconcileZoneRemoval };
}
