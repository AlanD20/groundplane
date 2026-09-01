'use client'

import { useEffect, useState } from 'react'
import { Save, Settings2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'
import { useStore } from '@/lib/store'

export function AgentConfigCard({ agentId }: { agentId: string }) {
  const { agentConfig, agentConfigLoading, agentConfigError, setAgentConfig } = useStore()
  const [maxConcurrent, setMaxConcurrent] = useState('')
  const [pullInterval, setPullInterval] = useState('')
  const [labels, setLabels] = useState('')
  const [saving, setSaving] = useState(false)
  const [saveError, setSaveError] = useState<string | null>(null)

  useEffect(() => {
    if (!agentConfig) return
    setMaxConcurrent(String(agentConfig.max_concurrent_tasks))
    setPullInterval(String(agentConfig.pull_interval_seconds))
    setLabels(formatLabels(agentConfig.labels))
  }, [agentConfig])

  async function save() {
    setSaving(true)
    setSaveError(null)
    try {
      await setAgentConfig(agentId, {
        max_concurrent_tasks: positiveInteger(maxConcurrent, 'Max concurrent tasks'),
        pull_interval_seconds: positiveInteger(pullInterval, 'Pull interval'),
        labels: parseLabels(labels),
      })
    } catch (cause) {
      setSaveError(cause instanceof Error ? cause.message : 'Unable to save Agent configuration')
    } finally {
      setSaving(false)
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Settings2 className="size-4 text-muted-foreground" /> Agent configuration
        </CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        {agentConfigLoading ? <p className="text-sm text-muted-foreground">Loading Agent configuration…</p> : null}
        {!agentConfigLoading && agentConfigError ? <p className="text-sm text-destructive">{agentConfigError}</p> : null}
        {!agentConfigLoading && agentConfig ? (
          <>
            <div className="grid gap-3 sm:grid-cols-2">
              <div className="flex flex-col gap-1.5">
                <Label htmlFor={`agent-max-${agentId}`}>Max concurrent tasks</Label>
                <Input id={`agent-max-${agentId}`} type="number" min={1} value={maxConcurrent} onChange={(event) => setMaxConcurrent(event.target.value)} />
              </div>
              <div className="flex flex-col gap-1.5">
                <Label htmlFor={`agent-pull-${agentId}`}>Pull interval (seconds)</Label>
                <Input id={`agent-pull-${agentId}`} type="number" min={1} value={pullInterval} onChange={(event) => setPullInterval(event.target.value)} />
              </div>
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor={`agent-labels-${agentId}`}>Labels</Label>
              <Textarea
                id={`agent-labels-${agentId}`}
                value={labels}
                onChange={(event) => setLabels(event.target.value)}
                className="min-h-28 font-mono"
                placeholder={'region=local\nrole=workload'}
              />
              <p className="text-xs text-muted-foreground">One key=value label per line.</p>
            </div>
            {saveError ? <p role="alert" className="text-xs text-destructive">{saveError}</p> : null}
            <div>
              <Button size="sm" disabled={saving} onClick={() => void save()}>
                <Save className="size-4" /> {saving ? 'Saving…' : 'Save Agent config'}
              </Button>
            </div>
          </>
        ) : null}
      </CardContent>
    </Card>
  )
}

function positiveInteger(value: string, label: string): number {
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed) || parsed < 1) throw new Error(`${label} must be a positive integer`)
  return parsed
}

function formatLabels(labels: Record<string, string>): string {
  return Object.entries(labels)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${value}`)
    .join('\n')
}

function parseLabels(value: string): Record<string, string> {
  const labels: Record<string, string> = {}
  for (const rawLine of value.split('\n')) {
    const line = rawLine.trim()
    if (!line) continue
    const separator = line.indexOf('=')
    if (separator < 1) throw new Error(`Invalid Agent label: ${line}`)
    const key = line.slice(0, separator).trim()
    const labelValue = line.slice(separator + 1).trim()
    if (!key || !labelValue) throw new Error(`Invalid Agent label: ${line}`)
    if (Object.hasOwn(labels, key)) throw new Error(`Duplicate Agent label: ${key}`)
    labels[key] = labelValue
  }
  return labels
}
