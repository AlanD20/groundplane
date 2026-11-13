import { useEffect, useState } from "react";
import type { operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { ResourcePanel } from "@/components/common/resource-panel";
import { DetailRow } from "@/components/common/detail-row";
import { Button } from "@/components/ui/button";

type UpdateResult =
  operations["runner.update.show"]["responses"][200]["content"]["application/json"];
const stages = [
  ["download", "Download and verify the selected package"],
  ["idle", "Wait for the current job to finish"],
  ["activate", "Activate the verified Runner package"],
  ["reconnect", "Verify the installed version and GitHub connection"],
] as const;

export function RunnerUpdateEvidence({
  runnerId,
  taskId,
}: {
  runnerId: string;
  taskId: string;
}) {
  const [result, setResult] = useState<UpdateResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [refresh, setRefresh] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    setResult(null);
    setError(null);
    async function load() {
      try {
        const response = await controllerRequest<UpdateResult>(
          `/runners/${encodeURIComponent(runnerId)}/updates?task=${encodeURIComponent(taskId)}`,
          200,
          { signal: controller.signal },
        );
        if (controller.signal.aborted) return;
        setResult(response);
        if (response.phase !== "finished") timer = setTimeout(load, 2000);
      } catch (cause) {
        if (!controller.signal.aborted)
          setError(
            cause instanceof Error
              ? cause.message
              : "Unable to load Runner update details",
          );
      }
    }
    void load();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [runnerId, taskId, refresh]);
  return (
    <ResourcePanel title="GitHub Runner update">
      {result && (
        <>
          <DetailRow label="Requested version" value={result.version} />
          <DetailRow
            label="Update status"
            value={result.outcome || result.phase}
          />
          <ol className="space-y-2 text-sm">
            {stages.map(([key, label]) => {
              const stage = result.stages.find((item) => item.name === key);
              return (
                <li key={key} className="flex flex-wrap justify-between gap-2">
                  <span>{label}</span>
                  <span className="text-muted-foreground">
                    {stage?.outcome ||
                      (result.phase === "finished"
                        ? "Not confirmed"
                        : "Not confirmed yet")}
                  </span>
                </li>
              );
            })}
          </ol>
          {result.failure && (
            <p role="alert" className="text-sm text-destructive">
              {result.failure}
            </p>
          )}
        </>
      )}
      {!result && !error && (
        <p role="status" className="text-sm text-muted-foreground">
          Loading update details…
        </p>
      )}
      {error && (
        <>
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
          <Button
            variant="outline"
            size="sm"
            onClick={() => setRefresh((value) => value + 1)}
          >
            Retry
          </Button>
        </>
      )}
    </ResourcePanel>
  );
}
