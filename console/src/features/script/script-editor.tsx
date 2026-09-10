import { useState } from 'react'
import type { Environment } from '@/lib/types'
import type { Script, ScriptExecution, ScriptHook, ScriptInput } from '@/lib/script-types'
import { scriptExecutionError } from '@/lib/script-execution'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select } from '@/components/ui/select'
import { Textarea } from '@/components/ui/textarea'
import { DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { ScriptExecutionFields } from './script-execution-fields'
import { parseScriptOrder } from './script-order'

const hooks: ScriptHook[] = ['manual', 'pre-deploy', 'post-deploy', 'pre-rollback', 'post-rollback', 'on-failure']

export function ScriptEditor({ env, script, onSave, onClose }: {
  env: Environment
  script: Script | null
  onSave: (input: ScriptInput) => Promise<void>
  onClose: () => void
}) {
  const [slug, setSlug] = useState(script?.slug ?? '')
  const [service, setService] = useState(script?.service ?? env.services[0]?.name ?? '')
  const [body, setBody] = useState(script?.body ?? '')
  const [when, setWhen] = useState<ScriptHook>(script?.when ?? 'manual')
  const [order, setOrder] = useState(String(script?.order ?? 0))
  const [execution, setExecution] = useState<ScriptExecution>(script?.execution ?? { mode: 'inherited' })
  const [submitting, setSubmitting] = useState(false)
  const [submitError, setSubmitError] = useState<string | null>(null)
  const parsedOrder = parseScriptOrder(order)
  const contextError = scriptExecutionError(execution)
  const valid = !!slug.trim() && !!service && !!body.trim() && parsedOrder !== undefined && !contextError

  return (
    <form className="flex min-w-0 flex-col gap-4" onSubmit={async (event) => {
      event.preventDefault()
      if (!valid || submitting || parsedOrder === undefined) return
      setSubmitting(true)
      setSubmitError(null)
      try {
        await onSave({ slug: slug.trim(), service, body, when, order: parsedOrder, execution })
        onClose()
      } catch (error) {
        setSubmitError(error instanceof Error ? error.message : 'Script mutation failed')
      } finally {
        setSubmitting(false)
      }
    }}>
      <DialogHeader>
        <DialogTitle>{script ? `Edit script · ${script.slug}` : `Add script · ${env.name}`}</DialogTitle>
      </DialogHeader>
      <fieldset disabled={submitting} className="flex min-w-0 flex-col gap-4">
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="script-name">Name</Label>
          <Input id="script-name" value={slug} onChange={(event) => setSlug(event.target.value)} placeholder="prepare-tls" />
        </div>
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="script-service">Service</Label>
          {script ? <Input id="script-service" value={service} disabled /> :
            <Select id="script-service" value={service} onValueChange={setService}
              options={env.services.map((candidate) => ({ value: candidate.name, label: candidate.name }))} />}
        </div>
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="script-body">Script (one line or many)</Label>
          <Textarea id="script-body" value={body} onChange={(event) => setBody(event.target.value)} rows={6}
            spellCheck={false} className="font-mono text-xs" placeholder={'set -eu\n# Prepare or migrate selected data'} />
        </div>
        <div className="grid gap-4 sm:grid-cols-2">
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="script-when">When</Label>
            <Select id="script-when" value={when} options={hooks.map((hook) => ({ value: hook, label: hook }))}
              onValueChange={(value) => { const hook = hooks.find((hook) => hook === value); if (hook) setWhen(hook) }} />
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="script-order">Hook order</Label>
            <Input id="script-order" type="number" min={0} max={65535} step={1} value={order}
              onChange={(event) => setOrder(event.target.value)} aria-invalid={parsedOrder === undefined}
              aria-describedby="script-order-help" />
          </div>
        </div>
        <p id="script-order-help" className="text-xs text-muted-foreground">
          0–65535. Lower numbers run first within this Service and phase; ties use the Script slug.
          Service dependencies and Release Group order are unchanged. Manual runs ignore this field.
        </p>
        {parsedOrder === undefined && <p role="alert" className="text-xs text-destructive">Enter a whole number from 0 through 65535.</p>}
        <ScriptExecutionFields env={env} service={service} execution={execution} onChange={setExecution} />
        {contextError && <p role="alert" className="text-xs text-destructive">{contextError}</p>}
        <p className="text-xs text-muted-foreground">
          Hooks run automatically for selected deploys and rollbacks. Saving replaces the complete execution choice;
          published runs keep their captured context. Resource availability is checked before a run is published.
        </p>
      </fieldset>
      {submitError && <p role="alert" className="text-xs text-destructive">{submitError}</p>}
      <DialogFooter>
        <Button type="button" variant="outline" disabled={submitting} onClick={onClose}>Cancel</Button>
        <Button type="submit" disabled={submitting || !valid}>{submitting ? 'Saving…' : script ? 'Save script' : 'Add script'}</Button>
      </DialogFooter>
    </form>
  )
}
