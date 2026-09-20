import type { ScriptExecution } from "./types";

export const maximumScriptVolumes = 32;
export const maximumScriptEntries = 64;

const reservedTargets = [
  "/groundplane-script-body",
  "/proc",
  "/sys",
  "/dev",
  "/run",
  "/var/run",
  "/bin",
  "/sbin",
  "/usr",
  "/lib",
  "/lib64",
  "/etc/hosts",
  "/etc/hostname",
  "/etc/resolv.conf",
];

function overlaps(left: string, right: string): boolean {
  return (
    left === right ||
    left.startsWith(`${right}/`) ||
    right.startsWith(`${left}/`)
  );
}

export function scriptExecutionError(
  execution: ScriptExecution,
): string | null {
  if (execution.mode === "inherited") return null;
  // The Controller remains authoritative for the complete OCI name grammar.
  const pinned = /^([^\s@]+)@sha256:[a-f0-9]{64}$/.exec(execution.image);
  const repository = pinned?.[1].split("/").at(-1);
  if (!repository || repository.includes(":"))
    return "Execution image must be a repository@sha256:digest reference without a tag.";
  if (
    !/^(0|[1-9][0-9]*):(0|[1-9][0-9]*)$/.test(execution.user) ||
    execution.user.split(":").some((part) => Number(part) > 4294967295)
  ) {
    return "Execution user must be numeric uid:gid (for example, 0:0).";
  }
  if (
    execution.volumes.length > maximumScriptVolumes ||
    execution.entryIds.length > maximumScriptEntries
  ) {
    return "Execution supports at most 32 Volume grants and 64 Entry grants.";
  }
  const seenVolumes = new Set<string>();
  for (const [index, grant] of execution.volumes.entries()) {
    if (
      !/^vol_[0-9A-HJKMNP-TV-Z]{26}$/.test(grant.volumeId) ||
      seenVolumes.has(grant.volumeId)
    ) {
      return "Execution requires a unique, stable Volume selection for each grant.";
    }
    seenVolumes.add(grant.volumeId);
    if (
      !grant.target.startsWith("/") ||
      grant.target === "/" ||
      /[\u0000-\u001f\u007f-\u009f]/u.test(grant.target) ||
      grant.target
        .slice(1)
        .split("/")
        .some((part) => part === "" || part === "." || part === "..")
    ) {
      return "Execution Volume targets must be canonical absolute paths, not root or traversal paths.";
    }
    if (
      reservedTargets.some((path) => overlaps(path, grant.target)) ||
      execution.volumes
        .slice(0, index)
        .some((previous) => overlaps(previous.target, grant.target))
    ) {
      return "Execution Volume targets must not overlap each other or reserved system paths.";
    }
  }
  if (
    new Set(execution.entryIds).size !== execution.entryIds.length ||
    execution.entryIds.some((id) => !/^ev_[0-9A-HJKMNP-TV-Z]{26}$/.test(id))
  ) {
    return "Execution Entry selections must be unique stable ids.";
  }
  return null;
}
