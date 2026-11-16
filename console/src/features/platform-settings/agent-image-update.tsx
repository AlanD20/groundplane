import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ResourcePanel } from "@/components/common/resource-panel";
import { DetailRow } from "@/components/common/detail-row";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { SoftwareIdentity } from "@/features/image-delivery/software-identity";
import { useStore } from "@/lib/store";
import type { PlatformAgent } from "@/lib/types";

export function AgentImageUpdate({ agent }: { agent: PlatformAgent }) {
  const { agentsLoading, refreshAgents, updateAgent } = useStore();
  const [updating, setUpdating] = useState(false);
  const [image, setImage] = useState("");
  return (
    <ResourcePanel
      title={`Agent image · ${agent.host}`}
      actions={
        <Button
          size="sm"
          variant="outline"
          disabled={agentsLoading}
          title={
            agent.inFlight > 0
              ? "Choose prerequisite Tasks to wait for before updating a busy Agent"
              : undefined
          }
          onClick={() => setUpdating(true)}
        >
          Set desired image
        </Button>
      }
    >
      <DetailRow
        label="Installed Agent"
        value={
          <SoftwareIdentity
            artifactReference={agent.installedImage}
            sourceBuild={agent.source?.kind === "source_ref"}
            sourceCommit={agent.source?.commit}
            artifactLabel="Installed image"
            version={agent.version}
            showReferences={false}
            unavailable="Installed version not reported"
          />
        }
      />
      <DetailRow
        label="Installed image"
        value={
          <SoftwareIdentity
            artifactReference={agent.installedImage}
            sourceBuild={agent.source?.kind === "source_ref"}
            sourceCommit={agent.source?.commit}
            artifactLabel="Installed image"
            unavailable="Image identity not reported by the Controller"
          />
        }
      />
      {updating && (
        <TaskRunnerDialog
          variant="inline"
          open={updating}
          onOpenChange={(open) => {
            setUpdating(open);
            if (!open) void refreshAgents();
          }}
          title={`Update Agent · ${agent.host}`}
          description="Select the immutable image from an Agent release. Only the Agent is replaced; failed readiness restores its previous image."
          review={
            <div className="space-y-2">
              <Label htmlFor={`agent-image-${agent.id}`}>
                Desired Agent image digest
              </Label>
              <Input
                id={`agent-image-${agent.id}`}
                value={image}
                onChange={(event) => setImage(event.target.value)}
                placeholder="ghcr.io/aland20/groundplane-agent@sha256:…"
              />
            </div>
          }
          startDisabled={(after) =>
            !image.trim() ||
            agentsLoading ||
            (after.length === 0 && agent.inFlight > 0)
          }
          type="update"
          target={agent.id}
          workspace="platform"
          startLabel="Update Agent"
          runAfter
          onDispatch={async (after) =>
            (await updateAgent(agent.id, image.trim(), after)).task_id
          }
        />
      )}
    </ResourcePanel>
  );
}
