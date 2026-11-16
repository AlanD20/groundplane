import { SettingsDraft } from "@/components/common/settings-draft";
("use client");

import { useState } from "react";
import { useDraftField } from "@/lib/use-draft-field";
import { Save, Settings2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { useStore } from "@/lib/store";

export function AgentConfigCard({ agentId }: { agentId: string }) {
  const { agentConfig, agentConfigLoading, agentConfigError, setAgentConfig } =
    useStore();
  const [maxConcurrent, setMaxConcurrent] = useDraftField(
    agentConfig ? String(agentConfig.max_concurrent_tasks) : "",
    agentId,
  );
  const [maxParallel, setMaxParallel] = useDraftField(
    agentConfig ? String(agentConfig.max_parallel_operations_per_task) : "",
    agentId,
  );
  const [pullInterval, setPullInterval] = useDraftField(
    agentConfig ? String(agentConfig.pull_interval_seconds) : "",
    agentId,
  );
  const [labels, setLabels] = useDraftField(
    agentConfig ? formatLabels(agentConfig.labels) : "",
    agentId,
  );
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  const dirty =
    !!agentConfig &&
    (maxConcurrent !== String(agentConfig.max_concurrent_tasks) ||
      maxParallel !== String(agentConfig.max_parallel_operations_per_task) ||
      pullInterval !== String(agentConfig.pull_interval_seconds) ||
      labels !== formatLabels(agentConfig.labels));
  async function save() {
    setSaving(true);
    setSaveError(null);
    try {
      const tasks = boundedLimit(maxConcurrent, "Concurrent Tasks");
      const operations = boundedLimit(
        maxParallel,
        "Parallel operations per Task",
      );
      if (tasks * operations > 256)
        throw new Error(
          "Concurrent Tasks × parallel operations per Task must be at most 256",
        );
      const updated = await setAgentConfig(agentId, {
        max_concurrent_tasks: tasks,
        max_parallel_operations_per_task: operations,
        pull_interval_seconds: positiveInteger(pullInterval, "Pull interval"),
        labels: parseLabels(labels),
      });
      setMaxConcurrent(String(updated.max_concurrent_tasks));
      setMaxParallel(String(updated.max_parallel_operations_per_task));
      setPullInterval(String(updated.pull_interval_seconds));
      setLabels(formatLabels(updated.labels));
    } catch (cause) {
      setSaveError(
        cause instanceof Error
          ? cause.message
          : "Unable to save Agent configuration",
      );
    } finally {
      setSaving(false);
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Settings2 className="size-4 text-muted-foreground" /> Agent
          configuration
        </CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        {agentConfigLoading ? (
          <p className="text-sm text-muted-foreground">
            Loading Agent configuration…
          </p>
        ) : null}
        {!agentConfigLoading && agentConfigError ? (
          <p className="text-sm text-destructive">{agentConfigError}</p>
        ) : null}
        {!agentConfigLoading && agentConfig ? (
          <SettingsDraft
            dirty={dirty}
            busy={saving}
            onCancel={() => {
              setMaxConcurrent(String(agentConfig.max_concurrent_tasks));
              setMaxParallel(
                String(agentConfig.max_parallel_operations_per_task),
              );
              setPullInterval(String(agentConfig.pull_interval_seconds));
              setLabels(formatLabels(agentConfig.labels));
              setSaveError(null);
            }}
            actions={
              <Button
                size="sm"
                disabled={saving || !dirty}
                onClick={() => void save()}
              >
                <Save className="size-4" />{" "}
                {saving ? "Saving…" : "Save Agent config"}
              </Button>
            }
          >
            <div className="grid gap-3 sm:grid-cols-2">
              <div className="flex flex-col gap-1.5">
                <Label htmlFor={`agent-max-${agentId}`}>Concurrent Tasks</Label>
                <Input
                  id={`agent-max-${agentId}`}
                  type="number"
                  min={1}
                  max={256}
                  value={maxConcurrent}
                  onChange={(event) => setMaxConcurrent(event.target.value)}
                />
              </div>
              <div className="flex flex-col gap-1.5">
                <Label htmlFor={`agent-parallel-${agentId}`}>
                  Parallel operations per Task
                </Label>
                <Input
                  id={`agent-parallel-${agentId}`}
                  type="number"
                  min={1}
                  max={256}
                  value={maxParallel}
                  onChange={(event) => setMaxParallel(event.target.value)}
                />
              </div>
              <div className="flex flex-col gap-1.5">
                <Label htmlFor={`agent-pull-${agentId}`}>
                  Pull interval (seconds)
                </Label>
                <Input
                  id={`agent-pull-${agentId}`}
                  type="number"
                  min={1}
                  value={pullInterval}
                  onChange={(event) => setPullInterval(event.target.value)}
                />
              </div>
            </div>
            <p className="text-xs text-muted-foreground">
              Concurrent Tasks counts distinct in-flight Tasks. Each Task can
              run up to its parallel operation limit. Both limits are 1–256;
              their product must be at most 256.
              {Number.isInteger(Number(maxConcurrent)) &&
                Number.isInteger(Number(maxParallel)) &&
                ` Current total operation limit: ${Number(maxConcurrent) * Number(maxParallel)}.`}
            </p>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor={`agent-labels-${agentId}`}>Labels</Label>
              <Textarea
                id={`agent-labels-${agentId}`}
                value={labels}
                onChange={(event) => setLabels(event.target.value)}
                className="min-h-28 font-mono"
                placeholder={"region=local\nrole=workload"}
              />
              <p className="text-xs text-muted-foreground">
                One key=value label per line.
              </p>
            </div>
            {saveError ? (
              <p role="alert" className="text-xs text-destructive">
                {saveError}
              </p>
            ) : null}
          </SettingsDraft>
        ) : null}
      </CardContent>
    </Card>
  );
}

function positiveInteger(value: string, label: string): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1)
    throw new Error(`${label} must be a positive integer`);
  return parsed;
}

function formatLabels(labels: Record<string, string>): string {
  return Object.entries(labels)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${value}`)
    .join("\n");
}

function parseLabels(value: string): Record<string, string> {
  const labels: Record<string, string> = {};
  for (const rawLine of value.split("\n")) {
    const line = rawLine.trim();
    if (!line) continue;
    const separator = line.indexOf("=");
    if (separator < 1) throw new Error(`Invalid Agent label: ${line}`);
    const key = line.slice(0, separator).trim();
    const labelValue = line.slice(separator + 1).trim();
    if (!key || !labelValue) throw new Error(`Invalid Agent label: ${line}`);
    if (Object.hasOwn(labels, key))
      throw new Error(`Duplicate Agent label: ${key}`);
    labels[key] = labelValue;
  }
  return labels;
}

function boundedLimit(value: string, label: string): number {
  const parsed = positiveInteger(value, label);
  if (parsed > 256) throw new Error(`${label} must be between 1 and 256`);
  return parsed;
}
