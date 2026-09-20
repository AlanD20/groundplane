import { useEffect, useId, useState } from 'react'
import type { BackingHooks } from './api'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { CodeEditor } from '@/components/ui/code-editor'
import { FormSection } from '@/components/ui/form-section'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select } from '@/components/ui/select'

const events = [
  { key: 'attach', label: 'On attach', description: 'Provision one consumer and write its declared facts to $GP_RESULT_FILE.' },
  { key: 'detach', label: 'On detach', description: 'Remove that consumer’s provisioned resources. Saved facts are available as GP_FACT_*.' },
  { key: 'before_stop', label: 'Before stop', description: 'Run before GP stops this shared service. A failure prevents the operation.' },
  { key: 'after_start', label: 'After start', description: 'Run after GP starts this shared service. There is no individual consumer.' },
] as const

export function BackingHookFields({ value, onChange, onError }: {
  value: BackingHooks
  onChange: (value: BackingHooks) => void
  onError: (message: string | null) => void
}) {
  const id = useId()
  const [errors, setErrors] = useState<Record<string, string>>({})
  useEffect(() => {
    const invalidTimeout = events.some(({ key }) => {
      const hook = value[key]
      return hook && (!Number.isInteger(hook.timeout_seconds) || hook.timeout_seconds < 1 || hook.timeout_seconds > 900)
    })
    onError(Object.values(errors)[0] ?? (invalidTimeout ? 'Every enabled hook needs a timeout from 1 to 900 seconds.' : null))
  }, [errors, value, onError])
  const setError = (key: string, message: string | null) => {
    const next = { ...errors }
    if (message) next[key] = message
    else delete next[key]
    setErrors(next)
  }
  const facts = value.facts ?? []
  const inputs = value.inputs ?? []

  return (
    <div className="flex min-w-0 flex-col gap-5">
      <FormSection title="Provisioning hooks (optional)" description="Without hooks, attaching only connects the network. Commands run inside the backing container; they never run on the host.">
        <p className="text-xs text-muted-foreground sm:col-span-2">
          The image needs /bin/sh, timeout with kill-after support, and basic file utilities.
          Your commands must handle retries safely. Do not put secrets in command text or print them.
        </p>
        {events.map((event) => {
          const hook = value[event.key]
          return (
            <div key={event.key} className="grid min-w-0 gap-3 rounded-lg border border-border p-3 sm:col-span-2">
              <label className="flex items-center gap-2 text-sm font-medium">
                <Checkbox checked={!!hook} onChange={(e) => {
                  const next = { ...value }
                  if (e.target.checked) {
                    next[event.key] = { command: [], timeout_seconds: 0 }
                    setError(event.key, 'Enter the hook command.')
                  }
                  else { delete next[event.key]; setError(event.key, null) }
                  onChange(next)
                }} />
                {event.label}
              </label>
              <p className="text-xs text-muted-foreground">{event.description}</p>
              {hook && <>
                <HookCommand key={`${id}-${event.key}`} id={`${id}-${event.key}`} command={hook.command ?? []}
                  onChange={(command) => onChange({ ...value, [event.key]: { ...hook, command } })}
                  onError={(message) => setError(event.key, message)} />
                <div className="grid gap-1.5">
                  <Label htmlFor={`${id}-${event.key}-timeout`}>Timeout (seconds)</Label>
                  <Input id={`${id}-${event.key}-timeout`} type="number" min={1} max={900}
                    value={hook.timeout_seconds || ''}
                    onChange={(e) => onChange({ ...value, [event.key]: { ...hook, timeout_seconds: Number(e.target.value) } })} />
                  {(!Number.isInteger(hook.timeout_seconds) || hook.timeout_seconds < 1 || hook.timeout_seconds > 900) &&
                    <p className="text-xs text-destructive">Enter a whole number from 1 to 900. There is no default timeout.</p>}
                </div>
              </>}
            </div>
          )
        })}
      </FormSection>

      <FormSection title="Hook inputs" description="GP supplies stable IDs and GP_INPUT_HOST. Add only the other values your commands need. Generated passwords belong to an Attach, not lifecycle hooks.">
        {inputs.map((input, index) => {
          const source = input.generate ? 'password' : 'secret_ref' in input ? 'secret' : 'literal'
          const replace = (next: typeof input) => onChange({ ...value, inputs: inputs.map((item, i) => i === index ? next : item) })
          return <div key={index} className="grid gap-3 rounded-lg border border-border p-3 sm:col-span-2 sm:grid-cols-2">
            <div className="grid gap-1.5"><Label htmlFor={`${id}-input-${index}`}>Input key</Label>
              <Input id={`${id}-input-${index}`} value={input.key} placeholder="PORT"
                onChange={(e) => replace({ ...input, key: e.target.value })} />
              <p className="text-xs text-muted-foreground">Available as GP_INPUT_{input.key || 'KEY'}</p>
            </div>
            <div className="grid gap-1.5"><Label htmlFor={`${id}-source-${index}`}>Value source</Label>
              <Select id={`${id}-source-${index}`} value={source} options={[
                { value: 'literal', label: 'Plain value' }, { value: 'secret', label: 'Secret reference' },
                { value: 'password', label: 'Generate an Attach password' },
              ]} onValueChange={(next) => replace(next === 'password' ? { key: input.key, generate: 'password' }
                : next === 'secret' ? { key: input.key, secret_ref: '' } : { key: input.key, value: '' })} />
            </div>
            {source !== 'password' && <div className="grid gap-1.5 sm:col-span-2">
              <Label htmlFor={`${id}-value-${index}`}>{source === 'secret' ? 'Secret ID' : 'Plain value'}</Label>
              <Input id={`${id}-value-${index}`} value={source === 'secret' ? input.secret_ref ?? '' : input.value ?? ''}
                onChange={(e) => replace(source === 'secret' ? { key: input.key, secret_ref: e.target.value } : { key: input.key, value: e.target.value })} />
            </div>}
            <Button type="button" variant="ghost" onClick={() => onChange({ ...value, inputs: inputs.filter((_, i) => i !== index) })}>Remove input</Button>
          </div>
        })}
        <Button type="button" variant="outline" onClick={() => onChange({ ...value, inputs: [...inputs, { key: '', value: '' }] })}>Add input</Button>
      </FormSection>

      <FormSection title="Output facts" description="Declare each key the attach hook must write as KEY=value. Mark credentials and connection URLs containing credentials as sensitive.">
        <p className="text-xs text-muted-foreground sm:col-span-2">Complete the Attach before applying a Blueprint that uses these facts. Producing and consuming new hook facts in the same apply is not supported.</p>
        {facts.map((fact, index) => <div key={index} className="flex flex-wrap items-end gap-3 sm:col-span-2">
          <div className="grid min-w-0 flex-1 gap-1.5"><Label htmlFor={`${id}-fact-${index}`}>Fact key</Label>
            <Input id={`${id}-fact-${index}`} value={fact.key} placeholder="PASSWORD"
              onChange={(e) => onChange({ ...value, facts: facts.map((item, i) => i === index ? { ...item, key: e.target.value } : item) })} />
          </div>
          <label className="flex h-9 items-center gap-2 text-sm"><Checkbox checked={fact.secret}
            onChange={(e) => onChange({ ...value, facts: facts.map((item, i) => i === index ? { ...item, secret: e.target.checked } : item) })} /> Sensitive</label>
          <Button type="button" variant="ghost" onClick={() => onChange({ ...value, facts: facts.filter((_, i) => i !== index) })}>Remove fact</Button>
        </div>)}
        <Button type="button" variant="outline" onClick={() => onChange({ ...value, facts: [...facts, { key: '', secret: false }] })}>Add fact</Button>
      </FormSection>
    </div>
  )
}

function HookCommand({ id, command, onChange, onError }: {
  id: string
  command: string[]
  onChange: (command: string[]) => void
  onError: (message: string | null) => void
}) {
  const [text, setText] = useState(JSON.stringify(command, null, 2))
  return <div className="grid min-w-0 gap-1.5">
    <Label htmlFor={id}>Command and arguments (JSON array)</Label>
    <CodeEditor id={id} label="Hook command and arguments" language="json" value={text} onValueChange={(next) => {
      setText(next)
      try {
        const parsed: unknown = JSON.parse(next)
        if (!Array.isArray(parsed) || !parsed.length || !parsed.every((item): item is string => typeof item === 'string') || !parsed[0].trim()) {
          onError('Enter a JSON array with an executable followed by its arguments.')
          return
        }
        onError(null)
        onChange(parsed)
      } catch {
        onError('The hook command must be a valid JSON array.')
      }
    }} />
    <p className="text-xs text-muted-foreground">For a shell script, use ["/bin/sh", "-c", "your script"]. Arguments are passed literally.</p>
  </div>
}
