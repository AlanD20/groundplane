import type { operations } from "@/lib/api.generated";
import type { HealthState, PlatformInfra } from "@/lib/types";
import { controllerRequest } from "@/lib/controller-json-request";

export type AgentPageResponse =
  operations["agent.list"]["responses"][200]["content"]["application/json"];
export type AgentResponse =
  operations["agent.show"]["responses"][200]["content"]["application/json"];

export function agentFromAPI(
  agent: AgentResponse,
): PlatformInfra["agents"][number] {
  let status: HealthState;
  switch (agent.status) {
    case "pending":
    case "healthy":
    case "degraded":
    case "stopped":
      status = agent.status;
      break;
    default:
      throw new Error(
        `Controller returned unknown Agent status ${agent.status}`,
      );
  }
  return {
    id: agent.id,
    enrollmentTaskId: agent.enrollment_task_id,
    host: agent.host,
    status,
    version: agent.version ?? null,
    labels: { ...agent.labels },
    readyAt: agent.ready_at ?? null,
    lastReportAt: agent.last_report_at ?? null,
    inFlight: agent.in_flight,
  };
}

export async function listAllAgents(
  signal?: AbortSignal,
): Promise<PlatformInfra["agents"]> {
  const agents: PlatformInfra["agents"] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({ limit: "200" });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<AgentPageResponse>(
      `/agents?${query}`,
      200,
      { signal },
    );
    agents.push(...(page.items ?? []).map(agentFromAPI));
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return agents;
}
