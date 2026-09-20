'use client'

import { useEffect, useState } from 'react'
import { FileCode2, RefreshCw, Save, ServerCog } from 'lucide-react'
import { PageHeader } from '@/components/common/page-header'
import { MetaPill } from '@/components/common/meta-pill'
import { StatusBadge } from '@/components/common/status-badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Label } from '@/components/ui/label'
import { CodeEditor } from '@/components/ui/code-editor'
import { useStore } from '@/lib/store'
import { ControllerUpdateCard } from './controller-update-card'

export default function PlatformControllerPage() {
  const {
    host,
    controllerConfig,
    controllerConfigLoading,
    controllerConfigError,
    refreshControllerConfig,
    setControllerConfig,
  } = useStore()
  const [content, setContent] = useState('')
  const [saving, setSaving] = useState(false)
  const [saveError, setSaveError] = useState<string | null>(null)

  useEffect(() => {
    const controller = new AbortController()
    void refreshControllerConfig(controller.signal).catch(() => undefined)
    return () => controller.abort()
  }, [refreshControllerConfig])

  useEffect(() => {
    if (controllerConfig) setContent(controllerConfig.content)
  }, [controllerConfig?.revision])

  const dirty = controllerConfig !== null && content !== controllerConfig.content

  async function save() {
    if (!controllerConfig) return
    setSaving(true)
    setSaveError(null)
    try {
      await setControllerConfig({content, expected_revision: controllerConfig.revision})
    } catch (cause) {
      setSaveError(cause instanceof Error ? cause.message : 'Unable to save Controller configuration')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title="Controller"
        description="Native control-plane runtime and its restart-bound startup configuration."
        icon={<ServerCog />}
        meta={
          <>
            {host ? <StatusBadge status={host.controller.status} /> : null}
            {host ? <MetaPill icon={<ServerCog />}>{host.controller.version}</MetaPill> : null}
          </>
        }
      />

      <ControllerUpdateCard />

      <Card>
        <CardHeader>
          <CardTitle>
            <h2 className="flex items-center gap-2"><FileCode2 className="size-4 text-muted-foreground" /> Startup configuration</h2>
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          {controllerConfigLoading && !controllerConfig ? (
            <p className="text-sm text-muted-foreground">Loading Controller configuration…</p>
          ) : null}
          {!controllerConfigLoading && controllerConfigError && !controllerConfig ? (
            <p role="alert" className="text-sm text-destructive">{controllerConfigError}</p>
          ) : null}
          {controllerConfig ? (
            <>
              <div className="grid gap-3 rounded-lg border border-border bg-surface p-3 text-xs sm:grid-cols-2">
                <div>
                  <p className="text-muted-foreground">File</p>
                  <p className="break-all font-mono">{controllerConfig.path}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Revision</p>
                  <p className="break-all font-mono">{controllerConfig.revision}</p>
                </div>
              </div>
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="controller-config-document">Controller YAML</Label>
                <CodeEditor
                  id="controller-config-document"
                  label="Controller YAML"
                  language="yaml"
                  value={content}
                  onValueChange={setContent}
                  aria-describedby="controller-config-help"
                />
                <p id="controller-config-help" className="text-xs text-muted-foreground">
                  Saving validates the startup schema and atomically replaces the exact file. Comments and formatting
                  are preserved.
                </p>
              </div>
              {controllerConfig.restart_required ? (
                <p role="status" className="rounded-lg border border-warning/40 bg-warning/10 px-3 py-2 text-xs text-foreground">
                  The file differs from the running startup snapshot. Restart the Controller to apply it.
                </p>
              ) : null}
              {saveError ? <p role="alert" className="text-xs text-destructive">{saveError}</p> : null}
              <div className="flex flex-wrap gap-2">
                <Button size="sm" disabled={!dirty || saving} onClick={() => void save()}>
                  <Save className="size-4" /> {saving ? 'Saving…' : 'Save Controller config'}
                </Button>
                <Button
                  size="sm"
                  variant="outline"
                  disabled={controllerConfigLoading || saving}
                  onClick={() => void refreshControllerConfig().catch(() => undefined)}
                >
                  <RefreshCw className="size-4" /> Reload file
                </Button>
              </div>
            </>
          ) : null}
        </CardContent>
      </Card>
    </div>
  )
}
