'use client'

import { ArrowLeftRight, Clock, Cpu, Database, HardDrive, MemoryStick, Server } from 'lucide-react'
import { Link } from 'react-router-dom'
import { useStore } from '@/lib/store'
import { PageHeader } from '@/components/common/page-header'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { StatusBadge } from '@/components/common/status-badge'
import { HostAgentsTable } from './host-agents-table'
import { MetaPill } from '@/components/common/meta-pill'

export default function PlatformHostPage() {
  const { host, hostLoading, hostError } = useStore()
  if (!host) {
    return (
      <div className="flex flex-col gap-6">
        <PageHeader
          title="Host"
          description="Machine health, Controller settings and local Agents."
          icon={<Server />}
        />
        <Card>
          <CardContent className="p-6 text-sm text-muted-foreground">
            {hostLoading ? 'Loading live Host health…' : (hostError ?? 'Host health is unavailable.')}
          </CardContent>
        </Card>
        <Link to="/platform/host/controller" className="text-sm text-primary">Controller settings</Link>
        <HostAgentsTable />
      </div>
    )
  }
  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title="Host"
        description="Machine health, Controller settings and local Agents."
        icon={<Server />}
        meta={
          <>
            <MetaPill icon={<Server />}>{host.hostname}</MetaPill>
            <MetaPill icon={<Cpu />}>
              {host.arch} · {host.os}
            </MetaPill>
            <MetaPill icon={<Clock />}>up {host.uptime}</MetaPill>
          </>
        }
      />

      {hostError ? <p role="status" className="text-sm text-warning">Host health may be stale: {hostError}</p> : null}

      <div className="grid gap-6 lg:grid-cols-3">
        <div className="flex flex-col gap-6 lg:col-span-2">
          <div className="grid gap-4">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center justify-between text-sm">
                  Controller <StatusBadge status={host.controller.status} />
                </CardTitle>
              </CardHeader>
              <CardContent className="flex flex-col gap-1.5 text-sm">
                <Row label="Runtime" value={host.controller.service} mono />
                <Row label="Version" value={host.controller.version} mono />
                <Row label="Storage" value={`etcd · ${host.etcd.node}`} mono />
                <Link to="/platform/host/controller" className="mt-2 text-xs font-medium text-primary hover:underline">
                  Open Controller settings
                </Link>
              </CardContent>
            </Card>

          </div>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Database className="size-4 text-muted-foreground" /> etcd
              </CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-1.5 text-sm">
              <Row label="Node" value={host.etcd.node} mono />
              <Row label="Status" value={<StatusBadge status={host.etcd.status} />} />
              <Row label="DB size" value={host.etcd.dbSize} mono />
              <p className="mt-1 text-xs text-muted-foreground">
                Desired state, the task queue, and the encrypted secret store live in etcd; watchers drive reconciliation.
              </p>
            </CardContent>
          </Card>
        </div>

        <div className="flex flex-col gap-6">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Cpu className="size-4 text-muted-foreground" /> Resources
              </CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-4">
              <Meter icon={<Cpu />} label="CPU load" pct={host.cpu.load} detail={`${host.cpu.cores} cores · ${host.cpu.model}`} />
              <Meter icon={<MemoryStick />} label="Memory" pct={host.memory.usedPct} detail={`${host.memory.used} / ${host.memory.total}`} />
              <Meter icon={<HardDrive />} label="Disk" pct={host.disk.usedPct} detail={`${host.disk.used} / ${host.disk.total}`} />
              <Meter icon={<ArrowLeftRight />} label="Swap" pct={host.swap.usedPct} detail={`${host.swap.used} / ${host.swap.total}`} />
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Docker</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground">Compose runtime</span>
                <span className="font-mono">{host.docker}</span>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
      <HostAgentsTable />
    </div>
  )
}

function Row({ label, value, mono }: { label: string; value: React.ReactNode; mono?: boolean }) {
  return (
    <div className="flex items-center justify-between gap-2">
      <span className="text-muted-foreground">{label}</span>
      <span className={mono ? 'font-mono text-xs' : 'text-xs'}>{value}</span>
    </div>
  )
}

function Meter({ icon, label, pct, detail }: { icon: React.ReactNode; label: string; pct: number; detail: string }) {
  const tone = pct >= 85 ? 'bg-destructive' : pct >= 70 ? 'bg-warning' : 'bg-primary'
  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-center justify-between text-xs">
        <span className="flex items-center gap-1.5 text-muted-foreground [&_svg]:size-3.5">
          {icon}
          {label}
        </span>
        <span className="font-mono text-muted-foreground">{detail}</span>
      </div>
      <div className="h-1.5 overflow-hidden rounded-full bg-secondary">
        <div className={`h-full rounded-full ${tone}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  )
}
