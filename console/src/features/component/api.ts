import type { operations } from "@/lib/api.generated";
import type { EnvironmentComponent, HealthState } from "@/lib/types";
import { controllerRequest } from "@/lib/controller-json-request";

export type ComponentPageResponse =
  operations["component.list"]["responses"][200]["content"]["application/json"];
export type ComponentResponse = NonNullable<
  ComponentPageResponse["items"]
>[number];

export async function listAllComponents(
  environmentId: string,
  signal?: AbortSignal,
): Promise<EnvironmentComponent[]> {
  const page = await controllerRequest<ComponentPageResponse>(
    `/components?environment=${encodeURIComponent(environmentId)}&limit=200`,
    200,
    { signal },
  );
  return (page.items ?? []).map((item) =>
    environmentComponentFromAPI(item, environmentId),
  );
}

function environmentComponentFromAPI(
  item: ComponentResponse,
  environmentId: string,
): EnvironmentComponent {
  if (
    item.owner !== "environment" ||
    item.owner_id !== environmentId ||
    item.environment_id !== environmentId
  ) {
    throw new Error(
      "Controller returned a Component outside the Environment owner scope",
    );
  }
  const common = {
    id: item.id,
    owner: "environment" as const,
    ownerRef: environmentId,
    enabled: item.enabled,
    status: componentHealthState(item.status),
    dependencies: [] as string[],
    generatedServices: item.generated_services ?? [],
    software: item.software,
  };
  const config = item.config;
  if (item.kind === "envoy") {
    if (!config) {
      return {
        ...common,
        kind: "envoy",
        config: null,
        state: item.pinned_ipv4 ? { pinnedIPv4: item.pinned_ipv4 } : {},
      };
    }
    if (
      config.implementation !== "envoy" ||
      !Array.isArray(config.zone_ids) ||
      config.zone_ids.length === 0 ||
      config.zone_ids.some((id) => typeof id !== "string") ||
      new Set(config.zone_ids).size !== config.zone_ids.length ||
      (config.alias !== undefined && typeof config.alias !== "string")
    ) {
      throw new Error(
        "Controller returned invalid Envoy Component configuration",
      );
    }
    return {
      ...common,
      kind: "envoy",
      config: {
        zone_ids: [...config.zone_ids],
        ...(config.alias === undefined ? {} : { alias: config.alias }),
      },
      state: item.pinned_ipv4 ? { pinnedIPv4: item.pinned_ipv4 } : {},
    };
  }
  if (item.kind === "caddy") {
    if (!config) {
      return {
        ...common,
        kind: "caddy",
        config: null,
        state: item.pinned_ipv4 ? { pinnedIPv4: item.pinned_ipv4 } : {},
      };
    }
    if (config.implementation !== "caddy") {
      throw new Error("Controller returned a config that does not match Caddy");
    }
    const zoneIDs = "zone_ids" in config ? config.zone_ids : undefined;
    const caddyfileTemplate =
      "caddyfile_template" in config ? config.caddyfile_template : undefined;
    const alias = "alias" in config ? config.alias : undefined;
    if (
      !Array.isArray(zoneIDs) ||
      zoneIDs.length === 0 ||
      zoneIDs.some((id) => typeof id !== "string") ||
      new Set(zoneIDs).size !== zoneIDs.length ||
      (caddyfileTemplate !== undefined &&
        typeof caddyfileTemplate !== "string") ||
      (alias !== undefined && typeof alias !== "string")
    ) {
      throw new Error(
        "Controller returned invalid Caddy Component configuration",
      );
    }
    return {
      ...common,
      kind: "caddy",
      config: {
        zone_ids: [...zoneIDs],
        ...(caddyfileTemplate === undefined
          ? {}
          : { caddyfile_template: caddyfileTemplate }),
        ...(typeof alias === "string" ? { alias } : {}),
      },
      state: item.pinned_ipv4 ? { pinnedIPv4: item.pinned_ipv4 } : {},
    };
  }
  if (item.kind === "cloudflare-tunnel") {
    if (!config) {
      return { ...common, kind: "cloudflare-tunnel", config: null, state: {} };
    }
    if (config.implementation !== "cloudflare-tunnel") {
      throw new Error(
        "Controller returned a config that does not match Cloudflare Tunnel",
      );
    }
    const secretID = "secret_id" in config ? config.secret_id : undefined;
    const zoneIDs = "zone_ids" in config ? config.zone_ids : undefined;
    if (
      typeof secretID !== "string" ||
      !Array.isArray(zoneIDs) ||
      zoneIDs.length === 0 ||
      zoneIDs.some((id) => typeof id !== "string") ||
      new Set(zoneIDs).size !== zoneIDs.length
    ) {
      throw new Error(
        "Controller returned invalid Cloudflare Tunnel Component configuration",
      );
    }
    return {
      ...common,
      kind: "cloudflare-tunnel",
      config: { zone_ids: [...zoneIDs], secret_id: secretID },
      state: {},
    };
  }
  throw new Error(
    `Controller returned unknown Environment Component kind ${item.kind}`,
  );
}

function componentHealthState(status: string | undefined): HealthState {
  switch (status) {
    case "disabled":
      return "stopped";
    case "pending":
      return "pending";
    case "healthy":
      return "healthy";
    case "degraded":
      return "degraded";
    default:
      return "unknown";
  }
}

export async function listPlatformComponents(
  signal?: AbortSignal,
): Promise<ComponentResponse[]> {
  const page = await controllerRequest<ComponentPageResponse>(
    "/components?platform=true&limit=200",
    200,
    { signal },
  );
  return page.items ?? [];
}
