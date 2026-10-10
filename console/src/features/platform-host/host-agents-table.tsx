import { ResourceRow, ResourceTable } from "@/components/common/resource-table";
import { StatusDot } from "@/components/common/status-badge";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { Button } from "@/components/ui/button";
import { ResourcePanel } from "@/components/common/resource-panel";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { PlatformAgentActions } from "@/features/platform-agent/platform-agent-actions";
import { SoftwareIdentity } from "@/features/image-delivery/software-identity";
import { formatAgentLabels, formatLastReportAt } from "@/lib/agent-read-model";
import { useStore } from "@/lib/store";
import { Plus } from "lucide-react";
import { useState } from "react";
import { Link } from "react-router-dom";

export function HostAgentsTable() {
  const {
    platform,
    host,
    agentsLoading,
    agentError,
    refreshAgents,
    joinAgent,
  } = useStore();
  const [joinOpen, setJoinOpen] = useState(false);
  return (
    <>
      <ResourcePanel
        title="Agents"
        actions={
          <Button
            size="sm"
            disabled={
              agentsLoading || agentError !== null || platform.agents.length > 0
            }
            title={
              agentsLoading
                ? "Loading the local Agent"
                : agentError
                  ? "Agent state is unavailable"
                  : platform.agents.length > 0
                    ? "The MVP supports one local Agent"
                    : "Join the local Agent"
            }
            onClick={() => setJoinOpen(true)}
          >
            <Plus className="size-3.5" />{" "}
            {platform.agents.length > 0 ? "Agent joined" : "Join Agent"}
          </Button>
        }
      >
        <ResourceTable>
          <Table className="w-full text-sm">
            <TableHeader>
              <TableRow className="border-b border-border text-left text-xs font-semibold text-muted-foreground">
                <TableHead>Host</TableHead>
                <TableHead>Installed software</TableHead>
                <TableHead>Labels</TableHead>
                <TableHead>In-flight tasks</TableHead>
                <TableHead>Last report</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {platform.agents.map((a) => (
                <ResourceRow key={a.id} href={`/platform/host/agents/${a.id}`}>
                  <TableCell>
                    <Link
                      to={`/platform/host/agents/${a.id}`}
                      className="font-medium text-primary hover:underline"
                    >
                      {a.host}
                    </Link>
                  </TableCell>
                  <TableCell>
                    <SoftwareIdentity
                      version={a.version}
                      showReferences={false}
                      unavailable="Version not reported"
                    />
                  </TableCell>
                  <TableCell>
                    {formatAgentLabels(a.labels).join(", ")}
                  </TableCell>
                  <TableCell>{a.inFlight}</TableCell>
                  <TableCell>{formatLastReportAt(a.lastReportAt)}</TableCell>
                  <TableCell>
                    <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
                      <StatusDot status={a.status} /> {a.status}
                    </span>
                  </TableCell>
                  <TableCell className="py-2 text-right">
                    <PlatformAgentActions agent={a} compact allowRemove />
                  </TableCell>
                </ResourceRow>
              ))}
              {agentsLoading && (
                <TableRow>
                  <TableCell
                    colSpan={7}
                    className="py-6 text-center text-xs text-muted-foreground"
                  >
                    Loading the local Agent…
                  </TableCell>
                </TableRow>
              )}
              {!agentsLoading && agentError && (
                <TableRow>
                  <TableCell
                    colSpan={7}
                    className="py-6 text-center text-xs text-destructive"
                  >
                    {agentError}
                  </TableCell>
                </TableRow>
              )}
              {!agentsLoading &&
                !agentError &&
                platform.agents.length === 0 && (
                  <TableRow>
                    <TableCell
                      colSpan={7}
                      className="py-6 text-center text-xs text-muted-foreground"
                    >
                      No local Agent is joined. Join it to let the Controller
                      create and manage the Agent container.
                    </TableCell>
                  </TableRow>
                )}
            </TableBody>
          </Table>
        </ResourceTable>
      </ResourcePanel>

      <TaskRunnerDialog
        open={joinOpen}
        onOpenChange={(next) => {
          setJoinOpen(next);
          if (!next) void refreshAgents();
        }}
        title="Join local Agent"
        type="create"
        target={host?.hostname ?? "unavailable"}
        workspace="platform"
        startLabel="Join Agent"
        onDispatch={async () => (await joinAgent()).task_id}
      />
    </>
  );
}
