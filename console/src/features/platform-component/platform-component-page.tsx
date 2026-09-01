'use client'

import { useRequiredParams } from '@/lib/router'
import { Link } from 'react-router-dom'
import {
  ArrowLeft,
  Network,
  Plus,
  RefreshCw,
  Save,
} from 'lucide-react'
import { useState } from 'react'
import { useStore } from '@/lib/store'
import { environmentPlatformIngress } from '@/lib/environment-platform-ingress'
import { PageHeader } from '@/components/common/page-header'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { StatusBadge } from '@/components/common/status-badge'
import { MetaPill } from '@/components/common/meta-pill'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'

const kindIcon: Record<string, React.ReactNode> = {
  coredns: <Network className="size-4 text-muted-foreground" />,
}

export default function PlatformComponentPage() {
  const params = useRequiredParams('component')
  const { platform, tenantProjects, platformComponentsLoading, platformComponentError, setComponentEnabled, updateComponentConfig, refreshPlatformComponents } = useStore()
  const component = platform.components.find((c) => c.kind === params.component)

  const { unavailable: environmentsWithoutComponentProjection } = environmentPlatformIngress(tenantProjects)
  const dnsConfig = platform.dns.upstream !== undefined && platform.dns.upstreamAuto !== undefined && platform.dns.tailnetDelegation !== undefined && platform.dns.forwarders !== undefined
    ? {
        upstream: platform.dns.upstream,
        upstreamAuto: platform.dns.upstreamAuto,
        tailnetDelegation: platform.dns.tailnetDelegation,
        forwarders: platform.dns.forwarders,
      }
    : undefined
  const editableDNSConfig = dnsConfig ?? {
    upstream: '',
    upstreamAuto: false,
    tailnetDelegation: false,
    forwarders: [],
  }

  if (platformComponentsLoading) {
    return <div className="py-10 text-sm text-muted-foreground">Loading platform components…</div>
  }
  if (platformComponentError) {
    return (
      <div className="flex flex-col items-start gap-4 py-10">
        <p className="text-sm text-destructive">{platformComponentError}</p>
        <Button variant="outline" onClick={() => void refreshPlatformComponents()}><RefreshCw className="size-4" /> Retry</Button>
      </div>
    )
  }
  if (!component || component.kind !== 'coredns') {
    return (
      <div className="flex flex-col items-start gap-4 py-10">
        <p className="text-sm text-muted-foreground">Unknown platform component.</p>
        <Link
          to="/platform/components"
          className="inline-flex h-8 items-center gap-1.5 rounded-lg border border-border bg-background px-2.5 text-sm font-medium hover:bg-muted hover:text-foreground dark:border-input dark:bg-input/30 dark:hover:bg-input/50"
        >
          <ArrowLeft className="size-4" /> Back to Components
        </Link>
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-6">
      <div className="flex items-center justify-between">
        <Link
          to="/platform/components"
          className="inline-flex items-center gap-1.5 text-xs font-medium text-muted-foreground transition-colors hover:text-foreground"
        >
          <ArrowLeft className="size-3.5" /> Components
        </Link>
      </div>
      <PageHeader
        title={component.name}
        description={component.runtime}
        icon={kindIcon[component.kind]}
        meta={
          <>
            <MetaPill icon={kindIcon[component.kind]}>{component.name}</MetaPill>
            <MetaPill icon={<RefreshCw />}>
              {component.image}:{component.version}
            </MetaPill>
            <StatusBadge status={component.status} />
          </>
        }
      />

      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center justify-between text-sm">
              <span className="flex items-center gap-2">
                {kindIcon[component.kind]}
                {component.name}
              </span>
              <StatusBadge status={component.status} />
            </CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-1.5 text-sm">
            <Row label="Image" value={`${component.image}:${component.version}`} mono />
            <Row label="Runtime" value={component.runtime} />
            {component.hostNetwork ? <Row label="Network" value="host network" mono /> : null}
            <div className="mt-1">
              <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground/70">Mounts</span>
              {component.mounts.map((m) => (
                <div key={m} className="font-mono text-xs text-muted-foreground">
                  {m}
                </div>
              ))}
            </div>
            <div className="mt-1 flex flex-col gap-0.5">
              {component.notes.map((n) => (
                <span key={n} className="text-xs text-muted-foreground">
                  · {n}
                </span>
              ))}
            </div>
          </CardContent>
        </Card>

        <CoreDnsSettings
          upstream={editableDNSConfig.upstream}
          upstreamAuto={editableDNSConfig.upstreamAuto}
          tailnetDelegation={editableDNSConfig.tailnetDelegation}
          forwarders={editableDNSConfig.forwarders}
          enabled={platform.dns.enabled}
          configured={dnsConfig !== undefined}
          onEnabled={async (enabled) => {
            await setComponentEnabled(component.id, enabled)
            await refreshPlatformComponents()
          }}
          onTailnet={(tailnetDelegation) => replaceCoreDNSConfig(
            updateComponentConfig,
            refreshPlatformComponents,
            component.id,
            editableDNSConfig,
            { tailnetDelegation },
          )}
          onAddForwarder={(domain, upstream) => replaceCoreDNSConfig(
            updateComponentConfig,
            refreshPlatformComponents,
            component.id,
            editableDNSConfig,
            { forwarders: [...editableDNSConfig.forwarders, { domain, upstream }] },
          )}
          onRemoveForwarder={(index) => replaceCoreDNSConfig(
            updateComponentConfig,
            refreshPlatformComponents,
            component.id,
            editableDNSConfig,
            { forwarders: editableDNSConfig.forwarders.filter((_, candidateIndex) => candidateIndex !== index) },
          )}
          onSave={(upstream, upstreamAuto) => replaceCoreDNSConfig(
            updateComponentConfig,
            refreshPlatformComponents,
            component.id,
            editableDNSConfig,
            { upstream, upstreamAuto },
          )}
        />
      </div>

      {environmentsWithoutComponentProjection > 0 && (
        <p className="rounded-lg border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning" role="status">
          Ingress for {environmentsWithoutComponentProjection} Environment{environmentsWithoutComponentProjection === 1 ? '' : 's'} is unavailable because the Controller did not publish an authoritative Component projection.
        </p>
      )}

    </div>
  )
}

type CoreDNSConfigUpdate = {
  upstream?: string
  upstreamAuto?: boolean
  tailnetDelegation?: boolean
  forwarders?: { domain: string; upstream: string }[]
}

async function replaceCoreDNSConfig(
  updateComponentConfig: ReturnType<typeof useStore>['updateComponentConfig'],
  refreshPlatformComponents: ReturnType<typeof useStore>['refreshPlatformComponents'],
  componentId: string,
  current: {
    upstream: string
    upstreamAuto: boolean
    tailnetDelegation: boolean
    forwarders: { domain: string; upstream: string }[]
  },
  update: CoreDNSConfigUpdate,
) {
  const next = { ...current, ...update }
  await updateComponentConfig(componentId, {
    upstream_auto: next.upstreamAuto,
    upstream_resolvers: next.upstreamAuto ? [] : resolverList(next.upstream),
    forwarders: next.forwarders.map((forwarder) => ({
      domain: forwarder.domain,
      resolvers: resolverList(forwarder.upstream),
    })),
    tailnet_delegation: next.tailnetDelegation,
  })
  await refreshPlatformComponents()
}

function resolverList(value: string): string[] {
  return value.trim().split(/[\s,]+/).filter(Boolean)
}

function CoreDnsSettings({
  upstream,
  upstreamAuto,
  tailnetDelegation,
  forwarders,
  enabled,
  configured,
  onEnabled,
  onTailnet,
  onAddForwarder,
  onRemoveForwarder,
  onSave,
}: {
  upstream: string
  upstreamAuto: boolean
  tailnetDelegation: boolean
  forwarders: { domain: string; upstream: string }[]
  enabled: boolean
  configured: boolean
  onEnabled: (v: boolean) => Promise<void>
  onTailnet: (v: boolean) => Promise<void>
  onAddForwarder: (domain: string, upstream: string) => Promise<void>
  onRemoveForwarder: (index: number) => Promise<void>
  onSave: (upstream: string, upstreamAuto: boolean) => Promise<void>
}) {
  const [u, setU] = useState(upstream)
  const [auto, setAuto] = useState(upstreamAuto)
  const [fwdDomain, setFwdDomain] = useState('')
  const [fwdUpstream, setFwdUpstream] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function mutate(action: () => Promise<void>) {
    setSaving(true)
    setError(null)
    try {
      await action()
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Unable to update CoreDNS')
    } finally {
      setSaving(false)
    }
  }
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between text-sm">
          <span>Settings</span>
          <StatusBadge status={enabled ? 'healthy' : 'stopped'} />
        </CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        {!configured ? (
          <p className="rounded-lg border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning" role="status">
            Save a complete resolver configuration before enabling CoreDNS.
          </p>
        ) : null}
        <div className="flex items-center justify-between border-b border-border pb-3">
          <div className="flex flex-col">
            <span className="text-sm font-medium">Local resolver</span>
            <span className="text-xs text-muted-foreground">
              deployed by the Agent in groundplane-infra; the Controller renders the Corefile — reloads are graceful,
              zero-downtime, and a bad edit is rejected while the old instance keeps serving.
            </span>
          </div>
          <Switch checked={enabled} disabled={saving || !configured} onCheckedChange={(value) => void mutate(() => onEnabled(value))} />
        </div>
        <div className="grid grid-cols-2 gap-3">
          <div className="flex flex-col gap-1">
            <Label htmlFor="dns-upstream">Catch-all upstream</Label>
            <Input
              id="dns-upstream"
              value={u}
              onChange={(e) => setU(e.target.value)}
              className="font-mono"
              disabled={auto}
              placeholder="1.1.1.1 8.8.8.8"
            />
          </div>
        </div>
        <div className="flex items-center justify-between border-b border-border pb-3">
          <div className="flex flex-col">
            <span className="text-sm font-medium">Auto upstream</span>
            <span className="text-xs text-muted-foreground">
              read the resolvers from the host&apos;s /etc/resolv.conf at render time — the input above is just a
              fallback preview. Off = pinned to the values you type.
            </span>
          </div>
          <Switch checked={auto} disabled={saving} onCheckedChange={setAuto} />
        </div>
        <div className="flex items-center justify-between border-b border-border py-3">
          <div className="flex flex-col">
            <span className="text-sm font-medium">Tailnet delegation</span>
            <span className="text-xs text-muted-foreground">
              forwards the tailnet domain (ts.net) to 100.100.100.100 so MagicDNS names resolve through the local
              resolver when Tailscale runs on the host
            </span>
          </div>
          <Switch checked={tailnetDelegation} disabled={saving} onCheckedChange={(value) => void mutate(() => onTailnet(value))} />
        </div>
        <div className="flex flex-col gap-2">
          <span className="text-sm font-medium">Domain forwarders</span>
          <span className="text-xs text-muted-foreground">
            per-zone routing: each domain is answered by its own resolvers (rendered as{' '}
            <span className="font-mono">forward &lt;domain&gt; &lt;resolvers&gt;</span> in the Corefile) before the
            catch-all. The tailnet delegation above is one of these, managed automatically.
          </span>
          <div className="flex flex-col gap-1.5">
            {forwarders.map((f, index) => (
              <div key={f.domain} className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
                <div className="flex items-center gap-2 font-mono text-xs">
                  <span className="text-primary">{f.domain}</span>
                  <span className="text-muted-foreground">→</span>
                  <span className="text-muted-foreground">{f.upstream}</span>
                </div>
                <Button variant="ghost" size="sm" disabled={saving} onClick={() => void mutate(() => onRemoveForwarder(index))}>
                  Remove
                </Button>
              </div>
            ))}
            {forwarders.length === 0 && (
              <div className="text-xs text-muted-foreground">no domain forwarders — everything goes to the catch-all</div>
            )}
          </div>
          <div className="grid grid-cols-[1fr_1fr_auto] items-end gap-2">
            <div className="flex flex-col gap-1">
              <Label htmlFor="fwd-domain">Domain</Label>
              <Input
                id="fwd-domain"
                value={fwdDomain}
                onChange={(e) => setFwdDomain(e.target.value)}
                className="font-mono"
                placeholder="home.arpa"
              />
            </div>
            <div className="flex flex-col gap-1">
              <Label htmlFor="fwd-upstream">Resolvers</Label>
              <Input
                id="fwd-upstream"
                value={fwdUpstream}
                onChange={(e) => setFwdUpstream(e.target.value)}
                className="font-mono"
                placeholder="192.168.1.1 10.0.0.53"
              />
            </div>
            <Button
              size="sm"
              disabled={saving || !fwdDomain.trim() || !fwdUpstream.trim()}
              onClick={() => void mutate(async () => {
                await onAddForwarder(fwdDomain.trim(), fwdUpstream.trim())
                setFwdDomain('')
                setFwdUpstream('')
              })}
            >
              <Plus className="size-4" /> Add
            </Button>
          </div>
        </div>
        {error ? <p className="text-xs text-destructive" role="alert">{error}</p> : null}
        <Button size="sm" disabled={saving} onClick={() => void mutate(() => onSave(u, auto))}>
          <Save className="size-4" /> Save
        </Button>
      </CardContent>
    </Card>
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
