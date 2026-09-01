'use client'

import { ArrowLeftRight, Clock, Cpu, Database, HardDrive, MemoryStick, Server } from 'lucide-react'
import { useStore } from '@/lib/store'
import { PageHeader } from '@/components/common/page-header'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { StatusBadge, StatusDot } from '@/components/common/status-badge'
import { MetaPill } from '@/components/common/meta-pill'
import { AgentConfigCard } from '@/features/platform-agent/agent-config-card'
import { PlatformAgentActions } from '@/features/platform-agent/platform-agent-actions'

export default function PlatformHostPage() {
  const { host, hostLoading, hostError, tenantProjects, backingProjects, platform } = useStore()
  if (!host) {
    return (
      <div className="flex flex-col gap-6">
        <PageHeader
          title="Host"
          description="The Controller and Agent on this machine — one host, one control plane, one execution plane."
          icon={<Server />}
        />
        <Card>
          <CardContent className="p-6 text-sm text-muted-foreground">
            {hostLoading ? 'Loading live Host health…' : (hostError ?? 'Host health is unavailable.')}
          </CardContent>
        </Card>
      </div>
    )
  }
  const agent = platform.agents[0]
  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title="Host"
        description="The Controller and Agent on this machine — one host, one control plane, one execution plane."
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
        actions={agent ? <PlatformAgentActions agent={agent} /> : undefined}
      />

      <div className="grid gap-6 lg:grid-cols-3">
        <div className="flex flex-col gap-6 lg:col-span-2">
          <div className="grid grid-cols-2 gap-4">
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
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center justify-between text-sm">
                  Agent <StatusBadge status={host.agent.status} />
                </CardTitle>
              </CardHeader>
              <CardContent className="flex flex-col gap-1.5 text-sm">
                <Row label="Runtime" value="container · ECS-agent pattern" />
                <Row label="Lifecycle owner" value="Controller daemon" />
                <Row label="Task model" value="pulls tasks · acks on completion" />
                <Row label="Max concurrent tasks" value={`${host.agent.maxConcurrent} · controller-config`} mono />
                <Row label="Pull interval" value={`${host.agent.pullInterval} · heartbeat`} mono />
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

      {agent ? <AgentConfigCard agentId={agent.id} /> : null}

      {/* Tenant projects */}
      <Card>
        <CardHeader>
          <CardTitle>Tenant projects</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left text-xs font-semibold text-muted-foreground">
                  <th className="py-2 pr-4">Tenant</th>
                  <th className="py-2 pr-4">Project</th>
                  <th className="py-2 pr-4">Environment</th>
                  <th className="py-2 pr-4">Services</th>
                  <th className="py-2">Attached shared</th>
                </tr>
              </thead>
              <tbody>
                {tenantProjects.flatMap((p) =>
                  (p.environments ?? []).map((e) => (
                    <tr key={e.id} className="border-b border-border last:border-0">
                      <td className="py-2 pr-4 font-mono text-xs text-muted-foreground">{p.tenantId}</td>
                      <td className="py-2 pr-4 font-mono text-xs">{p.slug}</td>
                      <td className="py-2 pr-4 font-mono text-xs">{e.name}</td>
                      <td className="py-2 pr-4 text-xs">{e.services.length}</td>
                      <td className="py-2 font-mono text-xs text-muted-foreground">
                        {e.attaches.map((a) => a.projectId).join(', ') || '—'}
                      </td>
                    </tr>
                  )),
                )}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>

      {/* Backing services */}
      <Card>
        <CardHeader>
          <CardTitle>Backing services</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left text-xs font-semibold text-muted-foreground">
                  <th className="py-2 pr-4">Service</th>
                  <th className="py-2 pr-4">Engine</th>
                  <th className="py-2 pr-4">Consumers</th>
                  <th className="py-2">Status</th>
                </tr>
              </thead>
              <tbody>
                {backingProjects.map((g) => {
                  const env = g.environments?.[0]
                  const svc = env?.services[0]
                  return (
                    <tr key={g.id} className="border-b border-border last:border-0">
                      <td className="py-2 pr-4 font-mono text-xs">{svc?.serviceName ?? g.slug}</td>
                      <td className="py-2 pr-4 text-xs">
                        {svc?.adapter} · {svc?.image}
                      </td>
                      <td className="py-2 pr-4 text-xs">{g.consumers?.length ?? 0}</td>
                      <td className="py-2">
                        {g.status && (
                          <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
                            <StatusDot status={g.status} /> {g.status}
                          </span>
                        )}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>

      {/* How they talk */}
      <Card>
        <CardHeader>
          <CardTitle>How they talk</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="max-w-3xl text-sm text-muted-foreground">
            The Controller is the control plane: it holds desired state, sequences actions (deploy, rollback, backup, run),
            and tells the Agent what and how to do it. The Agent applies those procedures through Docker Compose and streams
            observed state back. The Console is only a client of the Controller. Public routes need the ingress components
            (Cloudflare Tunnel + Caddy), enabled per environment on the Router tab — never auto-deployed.
          </p>
        </CardContent>
      </Card>

      {/* Encryption at rest + DR */}
      <Card>
        <CardHeader>
          <CardTitle>Encryption at rest + disaster recovery</CardTitle>
        </CardHeader>
        <CardContent className="flex max-w-3xl flex-col gap-2 text-sm text-muted-foreground">
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
            <span className="font-mono text-xs">/etc/groundplane/controller.age</span>
            <span className="font-mono text-xs">0600 · root-only</span>
          </div>
          <p>
            The Controller runs as root; this key wraps every secret value before it lands in etcd — no other user or
            service on the host can read the secret store. Only a root compromise exposes it. Each environment
            additionally gets its own age keypair for backup encryption (Backups tab): backups encrypt with the public
            recipient, and the private identity is stored under this same wrap, revealed once and exported for disaster
            recovery.
          </p>
          <div className="rounded-lg border border-border bg-surface px-3 py-2">
            <p className="text-xs">
              <span className="font-medium text-foreground">Everything is etcd.</span> Every source of truth lives in
              etcd; Corefiles, Caddyfiles, env files, resolv.conf and compose projects are all re-rendered from it by
              the reconcile loop. <span className="font-medium text-foreground">DR export = etcd snapshot +
              controller.age</span> — importing both on a fresh host restores the entire host&apos;s state, secrets
              included. The snapshot alone restores configs but none of the secret values.
            </p>
          </div>
        </CardContent>
      </Card>
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
