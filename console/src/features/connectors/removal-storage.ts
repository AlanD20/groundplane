export type PendingConnectorRemoval = {
  connectorId: string;
  environmentId: string;
};
const pendingConnectorRemovalStorageKey =
  "groundplane-pending-connector-removals";
export function loadPendingConnectorRemovals(): Map<
  string,
  PendingConnectorRemoval
> {
  try {
    const stored = localStorage.getItem(pendingConnectorRemovalStorageKey);
    if (!stored) return new Map();
    const entries = JSON.parse(stored) as [string, PendingConnectorRemoval][];
    if (!Array.isArray(entries)) return new Map();
    return new Map(
      entries.filter(
        ([taskId, pending]) =>
          typeof taskId === "string" &&
          typeof pending?.connectorId === "string" &&
          typeof pending?.environmentId === "string",
      ),
    );
  } catch {
    return new Map();
  }
}
export function persistPendingConnectorRemovals(
  removals: Map<string, PendingConnectorRemoval>,
) {
  try {
    if (removals.size === 0)
      localStorage.removeItem(pendingConnectorRemovalStorageKey);
    else
      localStorage.setItem(
        pendingConnectorRemovalStorageKey,
        JSON.stringify([...removals]),
      );
  } catch {
    /* private mode */
  }
}
