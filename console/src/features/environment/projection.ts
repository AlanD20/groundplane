import type { components } from "@/lib/api.generated";
import type { Environment } from "@/lib/types";

type EnvironmentDocument = components["schemas"]["Environment"] & {
  deletion_task_id?: string | null;
};

export type EnvironmentProjection = Environment & {
  deletionTaskId: string | null;
};

export function environmentFromAPI(
  environment: EnvironmentDocument,
): EnvironmentProjection {
  const provisioningState = environment.provisioning_state;
  if (
    provisioningState !== "provisioning" &&
    provisioningState !== "ready" &&
    provisioningState !== "failed"
  ) {
    throw new Error(
      `Controller returned unknown Environment provisioning state ${provisioningState}`,
    );
  }
  return {
    id: environment.id,
    projectId: environment.project_id,
    name: environment.name,
    networkPool: environment.network_pool,
    networkCapacity: {
      totalAddresses: environment.network_capacity.total_addresses,
      allocatedAddresses: environment.network_capacity.allocated_addresses,
      availableAddresses: environment.network_capacity.available_addresses,
      zoneCount: environment.network_capacity.zone_count,
    },
    status:
      provisioningState === "ready"
        ? "healthy"
        : provisioningState === "failed"
          ? "failed"
          : "pending",
    provisioningState,
    createTaskId: environment.create_task_id ?? null,
    deletionTaskId: environment.deletion_task_id ?? null,
    release: "none",
    deploys: [],
    releaseGroups: [],
    zones: [],
    services: [],
    attaches: [],
    routes: [],
    components: [],
    volumes: [],
    volumeDir: environment.volume_dir,
    entries: [],
    envVars: [],
    files: [],
    scripts: [],
    backup: {
      enabled: false,
      frequency: "*-*-* 03:15:00",
      keep: 7,
      encryption: "age",
      sources: [],
      nextRun: "never",
      lastRun: "never",
      lastStatus: "pending",
    },
    retention: { inactiveSlotDays: 7, keepImages: 3 },
    lastDeployAt: "never",
  };
}
