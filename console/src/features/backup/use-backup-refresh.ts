import { useEffect } from "react";
import { useStore } from "@/lib/store";

// Observe only the visible Backup surface. Serialized reads also pick up
// scheduled runs, CLI operations and asynchronous retention after a Backup.
export function useBackupRefresh(environmentId: string, includePoints: boolean) {
  const { loadBackupPolicy, loadRecoveryPoints } = useStore();
  useEffect(() => {
    let stopped = false;
    let reading = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const refresh = async () => {
      if (stopped || reading || document.visibilityState === "hidden") return;
      if (timer) clearTimeout(timer);
      reading = true;
      await Promise.allSettled([
        loadBackupPolicy(environmentId),
        ...(includePoints ? [loadRecoveryPoints(environmentId)] : []),
      ]);
      reading = false;
      if (!stopped) timer = setTimeout(() => void refresh(), 5000);
    };
    const visibility = () => {
      if (timer) clearTimeout(timer);
      if (document.visibilityState === "visible") void refresh();
    };
    void refresh();
    document.addEventListener("visibilitychange", visibility);
    return () => {
      stopped = true;
      if (timer) clearTimeout(timer);
      document.removeEventListener("visibilitychange", visibility);
    };
  }, [environmentId, includePoints, loadBackupPolicy, loadRecoveryPoints]);
}
