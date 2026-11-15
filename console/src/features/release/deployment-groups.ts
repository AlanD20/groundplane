import type { DeployRecord } from "@/lib/types";

export type DeploymentGroup = {
  taskId: string;
  when: string;
  releases: DeployRecord[];
};

// Group the complete history before filtering or pagination so a Task's
// Service releases always remain together, including partially failed runs.
export function groupDeployments(releases: DeployRecord[]): DeploymentGroup[] {
  const groups = new Map<string, DeploymentGroup>();
  for (const release of releases) {
    const existing = groups.get(release.taskId);
    if (existing) {
      existing.releases.push(release);
      if (release.when < existing.when) existing.when = release.when;
    } else {
      groups.set(release.taskId, {
        taskId: release.taskId,
        when: release.when,
        releases: [release],
      });
    }
  }
  for (const group of groups.values()) {
    group.releases.sort(
      (a, b) =>
        a.service.localeCompare(b.service) || a.when.localeCompare(b.when),
    );
  }
  return [...groups.values()];
}
