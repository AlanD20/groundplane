import type { Volume, VolumeDeletionImpactPage } from "./types";
import { controllerRequest } from "@/lib/controller-json-request";
import {
  findEnvironment,
  type EnvironmentRemovalDraft,
} from "@/features/environment/environment-removal-model";
import {
  createVolume,
  editVolume,
  getVolume,
  getVolumeDeletionImpact,
  removeVolume,
} from "./api";

export type VolumeActions = {
  addVolume: (
    envId: string,
    input: { slug: string; key?: string },
  ) => Promise<Volume>;
  getVolume: (volumeId: string) => Promise<Volume>;
  updateVolume: (
    envId: string,
    volumeId: string,
    patch: Pick<Volume, "slug">,
  ) => Promise<Volume>;
  getVolumeDeletionImpact: (
    volumeId: string,
    cursor?: string,
    limit?: number,
  ) => Promise<VolumeDeletionImpactPage>;
  removeVolume: (
    envId: string,
    volumeId: string,
    impactToken: string,
    confirmKey: string,
  ) => Promise<string>;
};
export function createVolumeActions(
  update: (change: (draft: EnvironmentRemovalDraft) => void) => void,
  assertEnvironmentMutable: (environmentId: string, operation: string) => void,
): VolumeActions {
  return {
    addVolume: async (envId, input) => {
      assertEnvironmentMutable(envId, "Volume mutation");
      const created = await createVolume(controllerRequest, envId, input);
      update((draft) => {
        findEnvironment(draft, envId)?.volumes.push(created);
      });
      return created;
    },
    getVolume: async (volumeId) => {
      const volume = await getVolume(controllerRequest, volumeId);
      update((draft) => {
        const current = findEnvironment(
          draft,
          volume.environmentId,
        )?.volumes.find((candidate) => candidate.id === volume.id);
        if (current) Object.assign(current, volume);
      });
      return volume;
    },
    updateVolume: async (envId, volumeId, patch) => {
      assertEnvironmentMutable(envId, "Volume mutation");
      const edited = await editVolume(controllerRequest, volumeId, patch.slug);
      update((draft) => {
        const volume = findEnvironment(draft, envId)?.volumes.find(
          (candidate) => candidate.id === volumeId,
        );
        if (volume) Object.assign(volume, edited);
      });
      return edited;
    },
    getVolumeDeletionImpact: (volumeId, cursor = "", limit = 40) =>
      getVolumeDeletionImpact(controllerRequest, volumeId, cursor, limit),
    removeVolume: (envId, volumeId, impactToken, confirmKey) => {
      assertEnvironmentMutable(envId, "Volume mutation");
      return removeVolume(controllerRequest, volumeId, impactToken, confirmKey);
    },
  };
}
