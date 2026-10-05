import { PageHeader } from "@/components/common/page-header";
import { DetailRow } from "@/components/common/detail-row";
import { ResourceMeter } from "@/components/common/resource-meter";
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
import {
  ArrowLeftRight,
  Boxes,
  Cpu,
  HardDrive,
  MemoryStick,
  Server,
} from "lucide-react";
import { Link } from "react-router-dom";
import { HostAgentsTable } from "./host-agents-table";

export default function PlatformHostPage() {
  const { host, hostLoading, hostError } = useStore();
  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title={host?.hostname ?? "Host"}
        eyebrow="Host"
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
            <SummaryItem label="Uptime">{host.uptime}</SummaryItem>
            <SummaryItem label="Disk">
              {host.disk.used} / {host.disk.total}
            </SummaryItem>
          </SummaryStrip>
          <ResourcePanel title="Resource usage">
            <div className="grid gap-6 sm:grid-cols-2">
              <ResourceMeter
                icon={<Cpu />}
                label="CPU load"
                pct={host.cpu.load}
                detail={`${host.cpu.cores} cores, ${host.cpu.model}`}
              />
              <ResourceMeter
                icon={<MemoryStick />}
                label="Memory"
                pct={host.memory.usedPct}
                detail={`${host.memory.used} / ${host.memory.total}`}
              />
              <ResourceMeter
                icon={<HardDrive />}
                label="Disk"
                pct={host.disk.usedPct}
                detail={`${host.disk.used} / ${host.disk.total}`}
              />
              <ResourceMeter
                icon={<ArrowLeftRight />}
                label="Swap"
                pct={host.swap.usedPct}
                detail={`${host.swap.used} / ${host.swap.total}`}
              />
            </div>
          </ResourcePanel>
          <ResourcePanel title="Host inventory">
            <div className="grid gap-x-8 sm:grid-cols-2">
              <DetailRow label="System" value={host.os} />
              <DetailRow label="Architecture" value={host.arch} />
              <DetailRow label="Docker / Compose" value={host.docker} />
              <DetailRow
                label="CPU"
                value={`${host.cpu.cores} cores · ${host.cpu.model}`}
              />
            </div>
          </ResourcePanel>
          <ResourceTable>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Infrastructure</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Version / size</TableHead>
                  <TableHead>Runtime</TableHead>
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
                  <TableCell>{host.controller.service}</TableCell>
                </ResourceRow>
                <ResourceRow href="/platform/host/etcd">
                  <TableCell className="font-medium">etcd</TableCell>
                  <TableCell>
                    <StatusBadge status={host.etcd.status} />
                  </TableCell>
                  <TableCell>{host.etcd.dbSize}</TableCell>
                  <TableCell>{host.etcd.node}</TableCell>
                </ResourceRow>
              </TableBody>
            </Table>
          </ResourceTable>
        </>
      )}
      <HostAgentsTable />
    </div>
  );
}
