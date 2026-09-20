import type { Environment } from "@/lib/types";
export function refreshReleaseGroupTags(environment: Environment) {
  for (const group of environment.releaseGroups) {
    const activeTags = group.order.map(
      (service) =>
        environment.deploys.find(
          (record) => record.service === service && record.status === "active",
        )?.tag,
    );
    const distinct = new Set(activeTags);
    group.tag =
      activeTags.every(Boolean) && distinct.size === 1
        ? activeTags[0]
        : undefined;
  }
}
