import { useEffect, useRef, useState } from "react";
import type { operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import type { Runner } from "@/lib/types";
import { ResourcePanel } from "@/components/common/resource-panel";
import { DetailRow } from "@/components/common/detail-row";
import { TaskLink } from "@/components/common/task-link";
import { Button } from "@/components/ui/button";
import { Select } from "@/components/ui/select";
import { ControllerRequestError } from "@/lib/controller-request-errors";
import { Download, RefreshCw } from "lucide-react";

type Versions =
  operations["runner.versions"]["responses"][200]["content"]["application/json"];
type UpdateResult =
  operations["runner.update.show"]["responses"][200]["content"]["application/json"];
type UpdateRequest =
  operations["runner.update"]["requestBody"]["content"]["application/json"];
type RunnerRead =
  operations["runner.show"]["responses"][200]["content"]["application/json"];

export function RunnerSoftwarePanel({ runner }: { runner: Runner }) {
  const [versions, setVersions] = useState<Versions["items"]>([]);
  const [version, setVersion] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [task, setTask] = useState<string | null>(null);
  const [result, setResult] = useState<UpdateResult | null>(null);
  const pending = useRef<{ key: string; body: UpdateRequest } | null>(null);
  const path = `/runners/${encodeURIComponent(runner.id)}`;
  useEffect(() => {
    const controller = new AbortController();
    void controllerRequest<UpdateResult>(`${path}/updates`, 200, {
      signal: controller.signal,
    })
      .then((response) => {
        if (!controller.signal.aborted) {
          setTask(response.task_id);
          setResult(response);
        }
      })
      .catch((cause) => {
        if (
          !controller.signal.aborted &&
          !(cause instanceof ControllerRequestError && cause.status === 404)
        )
          setError(
            cause instanceof Error
              ? cause.message
              : "Unable to load Runner update history",
          );
      });
    return () => controller.abort();
  }, [path]);
  async function loadVersions() {
    setBusy(true);
    setError(null);
    try {
      const response = await controllerRequest<Versions>(
        `${path}/versions`,
        200,
      );
      setVersions(response.items);
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : "Unable to load upstream Runner releases",
      );
    } finally {
      setBusy(false);
    }
  }
  useEffect(() => {
    if (!task) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function refresh() {
      try {
        const response = await controllerRequest<UpdateResult>(
          `${path}/updates?task=${encodeURIComponent(task!)}`,
          200,
          { signal: controller.signal },
        );
        if (controller.signal.aborted) return;
        setResult(response);
        if (response.phase !== "finished") timer = setTimeout(refresh, 2000);
      } catch (cause) {
        if (!controller.signal.aborted)
          setError(
            cause instanceof Error
              ? cause.message
              : "Unable to refresh Runner update",
          );
      }
    }
    void refresh();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [task, path]);
  async function update() {
    if (runner.autoUpdate || !version || busy) return;
    setBusy(true);
    setError(null);
    try {
      if (!pending.current) {
        const current = await controllerRequest<RunnerRead>(path, 200);
        if (!current.revision)
          throw new Error("Refresh the Runner before updating its software");
        pending.current = {
          key: crypto.randomUUID(),
          body: { revision: current.revision, version },
        };
      }
      const accepted = await controllerRequest<{ task_id: string }>(
        `${path}/updates`,
        202,
        {
          method: "POST",
          body: pending.current.body,
          idempotencyKey: pending.current.key,
        },
      );
      setTask(accepted.task_id);
      setResult(null);
      pending.current = null;
    } catch (cause) {
      if (cause instanceof ControllerRequestError && cause.status < 500) {
        pending.current = null;
      }
      setError(
        cause instanceof Error
          ? cause.message
          : "Unable to accept Runner update",
      );
    } finally {
      setBusy(false);
    }
  }
  return (
    <ResourcePanel title="GitHub runner software">
      <DetailRow
        label="Installed upstream version"
        value={runner.githubVersion || "Not observed yet"}
      />
      <DetailRow
        label="Automatic updates"
        value={
          runner.autoUpdate
            ? "Enabled · checked hourly"
            : "Disabled · operator selects the version"
        }
      />
      <p className="text-xs text-muted-foreground">
        Updates preserve registration, workspace and build cache. GP
        image/tooling upgrades are separate.
      </p>
      {runner.autoUpdate ? (
        <p className="text-xs text-muted-foreground">
          Disable automatic updates in Runner settings to select an exact
          version.
        </p>
      ) : (
        <div className="flex flex-wrap items-end gap-2">
          <div className="min-w-48 flex-1">
            <Select
              aria-label="Upstream GitHub Runner version"
              searchable
              options={versions.map((item) => ({
                value: item.version,
                label: `${item.version} · ${item.architecture}`,
              }))}
              value={version}
              onValueChange={(next) => {
                if (!pending.current) setVersion(next);
              }}
              disabled={
                busy ||
                !!pending.current ||
                runner.lifecycle !== "ready" ||
                (!!task && result?.phase !== "finished")
              }
            />
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={loadVersions}
            disabled={busy}
          >
            <RefreshCw className="size-3.5" /> Releases
          </Button>
          <Button
            size="sm"
            onClick={update}
            disabled={
              busy ||
              !version ||
              runner.lifecycle !== "ready" ||
              (!!task && result?.phase !== "finished")
            }
          >
            <Download className="size-3.5" />
            {pending.current
              ? "Resolve update"
              : busy
                ? "Submitting…"
                : "Update when idle"}
          </Button>
        </div>
      )}
      <p className="text-xs text-muted-foreground">
        Available downloads are not a guarantee that GitHub still accepts an
        older version.
      </p>
      {task && <TaskLink taskId={task}>Open software update Task</TaskLink>}
      {result && (
        <p className="text-sm">
          {result.version}: {result.outcome || result.phase}
          {result.failure ? ` · ${result.failure}` : ""}
        </p>
      )}
      {error && (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      )}
    </ResourcePanel>
  );
}
