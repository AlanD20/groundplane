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
  // Only credential owners represent distinct databases; reusing connections
  // must not offer the same database as another source.
  const attachOptions = policyState.attaches.filter((attach) => {
    const backing = store.getBackingProject(attach.backingProjectId);
    const adapter = backing?.environments?.[0]?.services.find(
      (service) => service.id === attach.backingServiceId,
    )?.adapter;
    return (
      attach.credential.mode === "new" &&
      (adapter === "postgres" || adapter === "mysql")
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
