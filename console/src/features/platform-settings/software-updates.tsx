import { Download } from "lucide-react";
import { PageHeader } from "@/components/common/page-header";
import { SummaryStrip, SummaryItem } from "@/components/common/resource-panel";
import { ControllerUpdateCard } from "@/features/platform-controller/controller-update-card";
import { useStore } from "@/lib/store";
import { SoftwarePreparationPanel } from "./software-preparation-panel";
import { AgentImageUpdate } from "./agent-image-update";

export function SoftwareUpdates() {
  const { host, platform } = useStore();
  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Platform settings"
        title="Software updates"
        icon={<Download />}
      />
      <SummaryStrip>
        <SummaryItem label="Controller">
          {host?.controller.version ?? "Unavailable"}
        </SummaryItem>
        {platform.agents.map((agent) => (
          <SummaryItem key={agent.id} label={`Agent · ${agent.host}`}>
            {agent.version ?? "Unavailable"}
          </SummaryItem>
        ))}
      </SummaryStrip>
      <SoftwarePreparationPanel active />
      <ControllerUpdateCard />
      {platform.agents.map((agent) => (
        <AgentImageUpdate key={agent.id} agent={agent} />
      ))}
    </div>
  );
}
