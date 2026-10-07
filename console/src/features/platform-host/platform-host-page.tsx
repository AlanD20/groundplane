import { ResourceMeter } from "@/components/common/resource-meter";
import { PageHeader } from "@/components/common/page-header";
import { DetailRow } from "@/components/common/detail-row";
import { ResourcePanel } from "@/components/common/resource-panel";
import { StatusBadge } from "@/components/common/status-badge";
import { buttonVariants } from "@/components/ui/button";
import { useStore } from "@/lib/store";
import { Boxes, Server } from "lucide-react";
import { Link } from "react-router-dom";
import { HostAgentsTable } from "./host-agents-table";

export default function PlatformHostPage() {
  const { host, hostLoading, hostError, platform, platformComponentError } =
    useStore();
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
          <ResourcePanel title="Host capacity">
            <div className="grid gap-6 sm:grid-cols-2 xl:grid-cols-4">
              <ResourceMeter
                label="CPU load"
                pct={host.cpu.load}
                detail={`${host.cpu.load}%`}
              />
              <ResourceMeter
                label="Memory"
                pct={host.memory.usedPct}
                detail={`${host.memory.used} / ${host.memory.total}`}
              />
              <ResourceMeter
                label="Disk"
                pct={host.disk.usedPct}
                detail={`${host.disk.used} / ${host.disk.total}`}
              />
              <ResourceMeter
                label="Swap"
                pct={host.swap.usedPct}
                detail={`${host.swap.used} / ${host.swap.total}`}
              />
            </div>
          </ResourcePanel>
          <div className="grid items-start gap-6 lg:grid-cols-2">
            <ResourcePanel title="Host inventory">
              <DetailRow label="Uptime" value={host.uptime} />
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
              <HostDestination
                title="Controller"
                href="/platform/host/controller"
                status={<StatusBadge status={host.controller.status} />}
                description={`Version ${host.controller.version}`}
              />
              <HostDestination
                title="etcd"
                href="/platform/host/etcd"
                status={<StatusBadge status={host.etcd.status} />}
                description={`${host.etcd.dbSize} stored · Configuration and maintenance`}
              />
              {platform.components
                .filter((component) => component.kind === "coredns")
                .map((component) => (
                  <HostDestination
                    key={component.id}
                    title="CoreDNS"
                    href="/platform/components/coredns"
                    status={<StatusBadge status={component.status} />}
                    description={
                      platform.dns.enabled
                        ? "DNS records and upstream resolvers"
                        : "Resolver disabled"
                    }
                  />
                ))}
              {platformComponentError && (
                <p role="status" className="text-sm text-warning">
                  Component health unavailable: {platformComponentError}
                </p>
              )}
            </ResourcePanel>
          </div>
        </>
      )}
      <HostAgentsTable />
    </div>
  );
}

function HostDestination({
  title,
  href,
  status,
  description,
}: {
  title: string;
  href: string;
  status: React.ReactNode;
  description: string;
}) {
  return (
    <Link
      to={href}
      className="block rounded-lg border border-border bg-background/30 p-4 transition-colors hover:border-primary/40 hover:bg-accent/30"
    >
      <span className="flex flex-wrap items-center justify-between gap-2 text-sm font-medium">
        <span>{title} →</span>
        {status}
      </span>
      <span className="mt-2 block text-xs text-muted-foreground">
        {description}
      </span>
    </Link>
  );
}
