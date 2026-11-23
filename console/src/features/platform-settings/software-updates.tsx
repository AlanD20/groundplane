import { Download } from "lucide-react";
import { useEffect, useState } from "react";
import { PageHeader } from "@/components/common/page-header";
import { SummaryStrip, SummaryItem } from "@/components/common/resource-panel";
import { ControllerUpdateCard } from "@/features/platform-controller/controller-update-card";
import { SoftwareIdentity } from "@/features/image-delivery/software-identity";
import { useStore } from "@/lib/store";
import { SoftwarePreparationPanel } from "./software-preparation-panel";
import { AgentImageUpdate } from "./agent-image-update";

export function SoftwareUpdates() {
  const { host, platform } = useStore();
  const [consoleVersion, setConsoleVersion] = useState<string | null>();

  useEffect(() => {
    const request = new AbortController();
    void fetch("/console-version.json", {
      cache: "no-store",
      signal: request.signal,
      headers: { Accept: "application/json" },
    })
      .then(async (response) => {
        if (!response.ok) throw new Error("Console version unavailable");
        const identity: { release?: unknown } | null = await response.json();
        const release = identity?.release;
        if (
          typeof release !== "string" ||
          !/^console\/v\d+\.\d+\.\d+$/.test(release)
        )
          throw new Error("Invalid Console version");
        if (!request.signal.aborted) setConsoleVersion(release);
      })
      .catch(() => {
        if (!request.signal.aborted) setConsoleVersion(null);
      });
    return () => request.abort();
  }, []);
  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Platform settings"
        title="Software updates"
        icon={<Download />}
      />
      <SummaryStrip>
        <SummaryItem label="Installed Controller">
          <SoftwareIdentity
            version={host?.controller.version}
            sourceBuild={host?.controller.source?.kind === "source_ref"}
            sourceCommit={host?.controller.source?.commit}
            digest={host?.controller.update.running_sha256}
            showReferences={false}
            unavailable="Unavailable"
          />
        </SummaryItem>
        {platform.agents.map((agent) => (
          <SummaryItem key={agent.id} label={`Installed Agent · ${agent.host}`}>
            <SoftwareIdentity
              artifactReference={agent.installedImage}
              version={agent.version}
              sourceBuild={agent.source?.kind === "source_ref"}
              sourceCommit={agent.source?.commit}
              showReferences={false}
              unavailable="Version not reported"
            />
          </SummaryItem>
        ))}
        <SummaryItem label="Installed Console">
          <SoftwareIdentity
            version={consoleVersion}
            showReferences={false}
            unavailable={
              consoleVersion === undefined ? "Loading…" : "Version unavailable"
            }
          />
        </SummaryItem>
      </SummaryStrip>
      <SoftwarePreparationPanel active />
      <ControllerUpdateCard />
      {platform.agents.map((agent) => (
        <AgentImageUpdate key={agent.id} agent={agent} />
      ))}
    </div>
  );
}
