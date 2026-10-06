import type { RecoveryPoint } from "./types";

export type RecoveryPointGroup = {
  id: string;
  capture?: NonNullable<RecoveryPoint["capture"]>;
  points: RecoveryPoint[];
  createdAt: string;
  sizeBytes: number;
  fullyLoaded: boolean;
};

// Group only by recorded provenance. A loaded page boundary cannot be presented
// as a complete run; retention may independently remove any source's Point.
export function groupRecoveryPoints(
  points: RecoveryPoint[],
  nextCursor: string | null,
): RecoveryPointGroup[] {
  const groups = new Map<string, RecoveryPointGroup>();
  for (const point of points) {
    const id = point.capture?.taskId ?? "earlier";
    let group = groups.get(id);
    if (!group) {
      group = {
        id,
        capture: point.capture,
        points: [],
        createdAt: point.capture?.createdAt ?? point.createdAt,
        sizeBytes: 0,
        fullyLoaded: true,
      };
      groups.set(id, group);
    }
    group.points.push(point);
    group.sizeBytes += point.sizeBytes;
    if (!group.capture && point.createdAt > group.createdAt)
      group.createdAt = point.createdAt;
  }
  if (nextCursor && points.length) {
    const last = points[points.length - 1];
    const boundary = groups.get(last.capture?.taskId ?? "earlier");
    if (boundary) boundary.fullyLoaded = false;
  }
  return [...groups.values()];
}
