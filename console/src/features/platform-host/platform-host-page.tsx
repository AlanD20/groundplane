import { PageHeader } from "@/components/common/page-header";
import { DetailRow } from "@/components/common/detail-row";
import {
  ResourcePanel,
  SummaryItem,
  SummaryStrip,
} from "@/components/common/resource-panel";
import { ResourceRow, ResourceTable } from "@/components/common/resource-table";
import { StatusBadge } from "@/components/common/status-badge";
import { buttonVariants } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useStore } from "@/lib/store";
import { Boxes, Server } from "lucide-react";
import { Link } from "react-router-dom";
import { HostAgentsTable } from "./host-agents-table";

export default function PlatformHostPage() {
  const { host, hostLoading, hostError } = useStore();
  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title="Host"
        eyebrow="Platform"
        description={
          host
            ? `${host.hostname} · live inventory and control-plane health.`
            : "Live inventory and control-plane health."
        }
        icon={<Server />}
        actions={
          <Link to="/platform/host/images" className={buttonVariants()}>
            <Boxes aria-hidden />
            Manage images
          </Link>
        }
      />
      {hostError && (
        <p role="alert" className="text-sm text-warning">
          {host ? "Host health may be stale: " : ""}
          {hostError}
        </p>
      )}
      {!host ? (
        <ResourcePanel title="Host health">
          <p role="status" className="text-sm text-muted-foreground">
            {hostLoading
              ? "Loading Host health…"
              : "Host health is unavailable."}
          </p>
          <Link to="/platform/host/controller" className="text-xs text-primary">
            Controller settings
          </Link>
        </ResourcePanel>
      ) : (
        <>
          <SummaryStrip>
            <SummaryItem label="CPU load">{host.cpu.load}%</SummaryItem>
            <SummaryItem label="Memory">
              {host.memory.used} / {host.memory.total}
            </SummaryItem>
            <SummaryItem label="Disk">
              {host.disk.used} / {host.disk.total}
            </SummaryItem>
            <SummaryItem label="Uptime">{host.uptime}</SummaryItem>
          </SummaryStrip>
          <div className="grid items-start gap-6 lg:grid-cols-2">
            <ResourcePanel title="Host inventory">
              <DetailRow label="System" value={host.os} />
              <DetailRow label="Architecture" value={host.arch} />
              <DetailRow label="Docker / Compose" value={host.docker} />
              <DetailRow
                label="CPU"
                value={`${host.cpu.cores} cores · ${host.cpu.model}`}
              />
              <DetailRow
                label="Swap"
                value={`${host.swap.used} / ${host.swap.total}`}
              />
              <DetailRow
                label="Applications"
                value="Independent of Controller restarts"
              />
            </ResourcePanel>
            <ResourcePanel title="Control plane">
              <ResourceTable>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Component</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Version / size</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    <ResourceRow href="/platform/host/controller">
                      <TableCell>
                        <Link
                          to="/platform/host/controller"
                          className="font-medium hover:text-primary"
                        >
                          Controller
                        </Link>
                      </TableCell>
                      <TableCell>
                        <StatusBadge status={host.controller.status} />
                      </TableCell>
                      <TableCell>{host.controller.version}</TableCell>
                    </ResourceRow>
                    <ResourceRow href="/platform/host/etcd">
                      <TableCell>
                        <Link
                          to="/platform/host/etcd"
                          className="font-medium hover:text-primary"
                        >
                          etcd
                        </Link>
                      </TableCell>
                      <TableCell>
                        <StatusBadge status={host.etcd.status} />
                      </TableCell>
                      <TableCell>{host.etcd.dbSize}</TableCell>
                    </ResourceRow>
                  </TableBody>
                </Table>
              </ResourceTable>
            </ResourcePanel>
          </div>
        </>
      )}
      <HostAgentsTable />
    </div>
  );
}
