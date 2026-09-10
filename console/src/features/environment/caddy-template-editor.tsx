import { useEffect, useRef, useState } from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { useStore } from '@/lib/store'
import type { ManagedConfigFile } from '@/lib/types'
import { caddyTemplateError, readCaddyTemplate } from './caddy-template'

export function CaddyTemplateEditor({ componentId, enabled, value, onChange, onBlockedChange }: {
  componentId: string
  enabled: boolean
  value: string
  onChange: (value: string) => void
  onBlockedChange: (blocked: boolean) => void
}) {
  const [reading, setReading] = useState(false)
  const [readError, setReadError] = useState<string | null>(null)
  const generation = useRef(0)
  const error = readError ?? caddyTemplateError(value)

  useEffect(() => () => { generation.current++ }, [])
  useEffect(() => { onBlockedChange(reading || error !== null) }, [reading, error, onBlockedChange])

  async function importFile(file: File) {
    const selected = ++generation.current
    setReading(true)
    setReadError(null)
    try {
      const template = await readCaddyTemplate(file)
      if (selected === generation.current) onChange(template)
    } catch (cause) {
      if (selected === generation.current) {
        setReadError(cause instanceof Error ? cause.message : 'Unable to read Caddyfile template.')
      }
    } finally {
      if (selected === generation.current) setReading(false)
    }
  }

  return <div className="flex min-w-0 flex-col gap-2">
    <Label htmlFor="caddy-template">Complete Caddyfile template</Label>
    <textarea
      id="caddy-template"
      className="min-h-40 w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-xs"
      value={value}
      disabled={reading}
      spellCheck={false}
      aria-invalid={error !== null}
      aria-describedby="caddy-template-help caddy-template-error"
      onChange={(event) => { setReadError(null); onChange(event.target.value) }}
    />
    <p id="caddy-template-help" className="text-xs text-muted-foreground">
      Empty uses <code>{'{gp.routes}'}</code>. Full files can reference a declared Route with{' '}
      <code className="break-all">{'{gp.route:HOST:PATH:FIELD}'}</code>, where FIELD is host, path or upstream.
      Native Caddy placeholders stay unchanged. The Controller resolves references; Caddy validates before reload.
    </p>
    <Label htmlFor="caddy-template-file">Import complete file (UTF-8, up to 32 KiB)</Label>
    <Input id="caddy-template-file" type="file" accept=".caddy,Caddyfile,text/plain" disabled={reading}
      onChange={(event) => {
        const file = event.target.files?.[0]
        event.target.value = ''
        if (file) void importFile(file)
      }}
    />
    {reading && <p role="status" className="text-xs text-muted-foreground">Reading Caddyfile template…</p>}
    <p id="caddy-template-error" role={error ? 'alert' : undefined} className="text-xs text-warning">{error}</p>
    <SavedCaddyPreview componentId={componentId} enabled={enabled} />
  </div>
}

function SavedCaddyPreview({ componentId, enabled }: { componentId: string; enabled: boolean }) {
  const { refreshComponentConfig } = useStore()
  const [files, setFiles] = useState<ManagedConfigFile[]>([])
  const [loading, setLoading] = useState(enabled)
  const [error, setError] = useState<string | null>(null)
  const [refresh, setRefresh] = useState(0)

  useEffect(() => {
    if (!enabled) return
    const controller = new AbortController()
    setLoading(true)
    setError(null)
    setFiles([])
    void refreshComponentConfig(componentId, controller.signal)
      .then((next) => { if (!controller.signal.aborted) setFiles(next) })
      .catch((cause: unknown) => {
        if (!controller.signal.aborted) setError(cause instanceof Error ? cause.message : 'Unable to load saved preview.')
      })
      .finally(() => { if (!controller.signal.aborted) setLoading(false) })
    return () => controller.abort()
  }, [componentId, enabled, refresh, refreshComponentConfig])

  return <div className="mt-2 flex min-w-0 flex-col gap-2 border-t pt-3">
    <div className="flex items-center justify-between gap-2">
      <span className="text-sm font-medium">Controller-rendered Caddyfile</span>
      <Button type="button" variant="outline" size="sm" disabled={!enabled || loading} onClick={() => setRefresh((n) => n + 1)}>
        Refresh preview
      </Button>
    </div>
    <p className="text-xs text-muted-foreground">
      Current saved configuration only, excluding unsaved edits. This preview is not proof of live application.
    </p>
    {loading && <p role="status" className="text-xs text-muted-foreground">Loading saved preview…</p>}
    {error && <p role="alert" className="text-xs text-warning">{error}</p>}
    {!enabled && <p className="text-xs text-muted-foreground">Enable the Component to read its saved rendered file.</p>}
    {enabled && !loading && !error && files.length === 0 &&
      <p className="text-xs text-muted-foreground">No rendered file is available from the Controller.</p>}
    {!loading && !error && files.map((file, index) => <div key={file.path} className="flex min-w-0 flex-col gap-1">
      <Label htmlFor={`rendered-caddyfile-${index}`} className="break-all">{file.path}</Label>
      <textarea id={`rendered-caddyfile-${index}`} readOnly value={file.rendered} spellCheck={false}
        className="min-h-40 w-full rounded-md border border-input bg-muted px-3 py-2 font-mono text-xs" />
    </div>)}
  </div>
}
