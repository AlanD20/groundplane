import { ConnectorCheckPanel } from "@/features/connectors/connector-check-panel";
import { BackingCheckPanel } from "@/features/attach/backing-check-panel";
import { EdgeCheckPanel } from "@/features/environment/edge-check-panel";
import { DNSCheckPanel } from "@/features/platform-component/dns-check-panel";
import { RunnerCheckPanel } from "@/features/runners/runner-check-panel";
import type { TaskResponse, TaskWork } from "./api";

export function WorkCheckEvidence({
  task,
  work,
}: {
  task: TaskResponse["task"];
  work: TaskWork;
}) {
  const environmentId = task.scope.environment_id;
  switch (work.operation) {
    case "connector_check":
      return environmentId ? (
        <ConnectorCheckPanel
          environmentId={environmentId}
          identity={task.id}
          taskId={task.id}
          open
        />
      ) : null;
    case "backing_check":
      return environmentId ? (
        <BackingCheckPanel
          environmentId={environmentId}
          taskId={task.id}
          open
        />
      ) : null;
    case "edge_check":
      return environmentId ? (
        <EdgeCheckPanel environmentId={environmentId} taskId={task.id} open />
      ) : null;
    case "dns_check":
      return (
        <DNSCheckPanel componentId={work.target_id} taskId={task.id} open />
      );
    case "runner_check":
      return (
        <RunnerCheckPanel runnerId={work.target_id} taskId={task.id} open />
      );
    default:
      return null;
  }
}
