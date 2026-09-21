import type { useStore } from "@/lib/store";
import type { Environment } from "@/lib/types";
import type { BackupPolicySourceInput, BackupPolicyState } from "./types";
import { backupSourceLabel } from "./environment-backup-projection";
export function backupPolicySourceOptions(
  store: ReturnType<typeof useStore>,
  policyState: BackupPolicyState,
  sources: BackupPolicySourceInput[],
) {
  const backup = policyState.policy;
  // Sources: one per attach (never per service — a shared attach is never
  // backed up twice), any subset of volumes, and the environment's CONFIG
  // (env entries: vars, files, secrets — values included, age-encrypted).
  const attachOptions = policyState.attaches.filter((attach) => {
    const backing = store.getBackingProject(attach.backingProjectId);
    return (
      backing?.environments?.[0]?.services.find(
        (service) => service.id === attach.backingServiceId,
      )?.adapter === "postgres:16"
    );
  });
  const selectedRetainedAttachSources = backup.sources.filter(
    (source) =>
      source.kind === "attach" &&
      sources.some(
        (selected) =>
          selected.kind === "attach" && selected.targetId === source.targetId,
      ),
  );
  const unsupportedAttachSources = selectedRetainedAttachSources.filter(
    (source) =>
      policyState.attaches.some((attach) => attach.id === source.targetId) &&
      !attachOptions.some((attach) => attach.id === source.targetId),
  );
  const missingAttachSources = selectedRetainedAttachSources.filter(
    (source) =>
      !policyState.attaches.some((attach) => attach.id === source.targetId),
  );
  const missingVolumeSources = backup.sources.filter(
    (source) =>
      source.kind === "volume" &&
      sources.some(
        (selected) =>
          selected.kind === "volume" && selected.targetId === source.targetId,
      ) &&
      !policyState.volumes.some((volume) => volume.id === source.targetId),
  );
  return {
    attachOptions,
    unsupportedAttachSources,
    missingAttachSources,
    missingVolumeSources,
  };
}
export function backupSourceInputLabel(
  store: ReturnType<typeof useStore>,
  env: Environment,
  source: BackupPolicySourceInput,
) {
  return backupSourceLabel(store, env, { id: "", ...source });
}
