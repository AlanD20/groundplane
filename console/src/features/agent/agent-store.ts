import { useCallback, useEffect } from "react";
import type { operations } from "@/lib/api.generated";
import type { PlatformInfra } from "@/lib/types";
import { controllerRequest } from "@/lib/controller-json-request";
import { listAllAgents } from "./api";
type AgentTaskAccepted =
  operations["agent.join"]["responses"][202]["content"]["application/json"];
type AgentConfigResponse =
  operations["agent.config.show"]["responses"][200]["content"]["application/json"];
type AgentConfigRequest =
  operations["agent.config.set"]["requestBody"]["content"]["application/json"];

export type AgentState = {
  agentsLoading: boolean;
  agentError: string | null;
  agentConfig: AgentConfigResponse | null;
  agentConfigLoading: boolean;
  agentConfigError: string | null;
};
type AgentWorkspace = AgentState & { platform: PlatformInfra };
type Update = (change: (draft: AgentWorkspace) => void) => void;
export type AgentActions = {
  refreshAgents: (signal?: AbortSignal) => Promise<PlatformInfra["agents"]>;
  setAgentConfig: (
    agentId: string,
    config: AgentConfigRequest,
  ) => Promise<AgentConfigResponse>;
  joinAgent: () => Promise<AgentTaskAccepted>;
  updateAgent: (agentId: string, image: string) => Promise<AgentTaskAccepted>;
  removeAgent: (agentId: string) => Promise<AgentTaskAccepted>;
  // selectors
};
export function useAgentRefresh(update: Update): AgentActions["refreshAgents"] {
  const refreshAgents = useCallback(
    async (signal?: AbortSignal) => {
      const agents = await listAllAgents(signal);
      update((draft) => {
        draft.platform.agents = agents;
        draft.agentsLoading = false;
        draft.agentError = null;
      });
      return agents;
    },
    [update],
  );

  return refreshAgents;
}
export function useAgentLoading(
  state: AgentWorkspace,
  update: Update,
  refreshAgents: AgentActions["refreshAgents"],
) {
  useEffect(() => {
    const controller = new AbortController();
    void refreshAgents(controller.signal).then(
      () => undefined,
      (error: unknown) => {
        if (controller.signal.aborted) return;
        update((draft) => {
          draft.agentsLoading = false;
          draft.agentError =
            error instanceof Error ? error.message : "Unable to load Agents";
        });
      },
    );
    return () => controller.abort();
  }, [refreshAgents, update]);

  const localAgentID = state.platform.agents[0]?.id ?? "";

  useEffect(() => {
    if (state.agentsLoading) return;
    if (!localAgentID) {
      update((draft) => {
        draft.agentConfig = null;
        draft.agentConfigLoading = false;
        draft.agentConfigError = null;
      });
      return;
    }
    const controller = new AbortController();
    const path = `/agents/${encodeURIComponent(localAgentID)}/config`;
    void controllerRequest<AgentConfigResponse>(path, 200, {
      signal: controller.signal,
    }).then(
      (config) =>
        update((draft) => {
          draft.agentConfig = config;
          draft.agentConfigLoading = false;
          draft.agentConfigError = null;
        }),
      (error: unknown) => {
        if (controller.signal.aborted) return;
        update((draft) => {
          draft.agentConfigLoading = false;
          draft.agentConfigError =
            error instanceof Error
              ? error.message
              : "Unable to load Agent config";
        });
      },
    );
    return () => controller.abort();
  }, [localAgentID, state.agentsLoading, update]);
}
export function createAgentActions(
  update: Update,
  refreshAgents: AgentActions["refreshAgents"],
): Omit<AgentActions, "refreshAgents"> {
  return {
    setAgentConfig: async (agentId, config) => {
      const path = `/agents/${encodeURIComponent(agentId)}/config`;
      const updated = await controllerRequest<AgentConfigResponse>(path, 200, {
        method: "PUT",
        body: config,
      });
      update((draft) => {
        draft.agentConfig = updated;
        draft.agentConfigError = null;
      });
      return updated;
    },
    joinAgent: async () => {
      const accepted = await controllerRequest<AgentTaskAccepted>(
        "/agents",
        202,
        { method: "POST" },
      );
      await refreshAgents();
      return accepted;
    },
    updateAgent: async (agentId, image) => {
      const accepted = await controllerRequest<AgentTaskAccepted>(
        `/agents/${encodeURIComponent(agentId)}/update`,
        202,
        { method: "POST", body: { image } },
      );
      await refreshAgents();
      return accepted;
    },
    removeAgent: async (agentId) => {
      const accepted = await controllerRequest<AgentTaskAccepted>(
        `/agents/${encodeURIComponent(agentId)}`,
        202,
        { method: "DELETE" },
      );
      await refreshAgents();
      return accepted;
    },
  };
}
