import { useState } from 'react'
import { Pencil, Plus, Terminal, Trash2 } from 'lucide-react'
import { useStore } from '@/lib/store'
import { useRequiredParams } from '@/lib/router'
import type { Environment } from '@/lib/types'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Select } from '@/components/ui/select'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'
import { parseScriptOrder } from './script-order'

// ---- Scripts ----

export function ScriptsCard({ env }: { env: Environment }) {
  const store = useStore()
  const params = useRequiredParams('tenant')
  const [addOpen, setAddOpen] = useState(false)
  const [editing, setEditing] = useState<Environment['scripts'][number] | null>(null)
  const [removing, setRemoving] = useState<Environment['scripts'][number] | null>(null)
  const [sName, setSName] = useState('')
  const [sService, setSService] = useState(env.services[0]?.name ?? '')
  const [sBody, setSBody] = useState('')
  const [sWhen, setSWhen] = useState('manual')
  const [sOrder, setSOrder] = useState('0')
  const parsedOrder = parseScriptOrder(sOrder)
  const [submitting, setSubmitting] = useState(false)
  const [submitError, setSubmitError] = useState<string | null>(null)

  return (
    <>
      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <Terminal className="size-4 text-muted-foreground" /> Scripts
          </CardTitle>
          <Button variant="outline" size="sm" onClick={() => {
            setEditing(null)
            setSName('')
            setSService(env.services[0]?.name ?? '')
            setSBody('')
            setSWhen('manual')
            setSOrder('0')
            setAddOpen(true)
          }}>
            <Plus className="size-3.5" /> Script
          </Button>
        </CardHeader>
        <CardContent className="flex flex-col gap-1.5">
          {env.scripts.map((s) => (
            <div key={s.id} className="flex items-center justify-between gap-3 rounded-lg border border-border bg-surface px-3 py-2">
              <div className="flex min-w-0 flex-col">
                <div className="flex items-center gap-2">
				  <span className="font-mono text-sm">{s.slug}</span>
                  <Badge variant={s.when === 'manual' ? 'default' : 'primary'}>{s.when}</Badge>
                  <span className="text-xs text-muted-foreground">order {s.order}</span>
                </div>
                <span className="truncate font-mono text-xs text-muted-foreground">
                  {s.body.split('\n')[0]}
                  {s.body.split('\n').length > 1 ? ` · +${s.body.split('\n').length - 1} lines` : ''}
                </span>
              </div>
              <div className="flex items-center gap-2">
                <span className="font-mono text-xs text-muted-foreground">→ {s.service}</span>
				<Button variant="outline" size="sm" onClick={() => void store.runScript(s.id)}>
                  <Terminal className="size-3.5" /> Run
                </Button>
                <Button
                  variant="ghost"
                  size="icon-xs"
                  title="Edit script"
                  onClick={() => {
					setSName(s.slug)
                    setSService(s.service)
                    setSBody(s.body)
                    setSWhen(s.when)
                    setSOrder(String(s.order))
                    setEditing(s)
                  }}
                >
                  <Pencil className="size-3.5" />
                </Button>
                <Button
                  variant="ghost"
                  size="icon-xs"
                  className="text-muted-foreground hover:text-destructive"
                  title="Remove script"
                  onClick={() => setRemoving(s)}
                >
                  <Trash2 className="size-3.5" />
                </Button>
              </div>
            </div>
          ))}
          {env.scripts.length === 0 && (
            <div className="text-xs text-muted-foreground">no scripts — hooks run automatically on deploy/rollback</div>
          )}
        </CardContent>
      </Card>

      <Drawer open={addOpen || !!editing} onOpenChange={(next) => {
        setAddOpen(next)
        if (!next) {
          setEditing(null)
          setSubmitError(null)
        }
      }}>
        <DrawerContent>
          <DialogHeader>
			<DialogTitle>{editing ? `Edit script · ${editing.slug}` : `Add script · ${env.name}`}</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="s-name">Name</Label>
			  <Input id="s-name" value={sName} onChange={(e) => setSName(e.target.value)} placeholder="migrate" />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="s-service">Service</Label>
              {editing ? (
                <Input id="s-service" value={sService} disabled />
              ) : (
                <Select
                  id="s-service"
                  value={sService}
                  onValueChange={setSService}
                  options={env.services.map((s) => ({ value: s.name, label: s.name }))}
                />
              )}
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="s-body">Script (one line or many)</Label>
              <Textarea id="s-body" value={sBody} onChange={(e) => setSBody(e.target.value)} rows={6} spellCheck={false} className="font-mono text-xs" placeholder={'php artisan migrate --force\nphp artisan db:seed --force'} />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="s-when">When</Label>
              <Select
                id="s-when"
                value={sWhen}
                onValueChange={setSWhen}
                options={['manual', 'pre-deploy', 'post-deploy', 'pre-rollback', 'post-rollback', 'on-failure'].map((w) => ({ value: w, label: w }))}
              />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="s-order">Hook order</Label>
              <Input id="s-order" type="number" min={0} max={65535} step={1} value={sOrder}
                onChange={(event) => setSOrder(event.target.value)}
                aria-invalid={parsedOrder === undefined} aria-describedby="s-order-help" />
              <p id="s-order-help" className="text-xs text-muted-foreground">
                0–65535. Within this Service and phase, lower numbers run first; ties use the Script slug.
                Service dependencies and Release Group order are unchanged. Manual runs ignore this field.
              </p>
              {parsedOrder === undefined && <p role="alert" className="text-xs text-destructive">
                Enter a whole number from 0 through 65535.
              </p>}
            </div>
            <p className="text-xs text-muted-foreground">
              A full script body that runs against a service. Hooks run automatically on deploy/rollback.
            </p>
            {submitError && <p className="text-xs text-destructive">{submitError}</p>}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => {
              setAddOpen(false)
              setEditing(null)
            }}>
              Cancel
            </Button>
            <Button
              disabled={submitting || !sName.trim() || !sService || !sBody.trim() || parsedOrder === undefined}
              onClick={async () => {
                if (parsedOrder === undefined) return
                setSubmitting(true)
                setSubmitError(null)
                try {
                  if (editing) {
                    await store.updateScript(env.id, editing.id, {
					  slug: sName.trim(),
                      when: sWhen as Environment['scripts'][number]['when'],
                      body: sBody,
                      order: parsedOrder,
                    })
                  } else {
                    await store.addScript(env.id, {
					  slug: sName.trim(),
                      service: sService,
                      when: sWhen as Environment['scripts'][number]['when'],
                      body: sBody,
                      order: parsedOrder,
                    })
                  }
                  setAddOpen(false)
                  setEditing(null)
                  setSName('')
                  setSBody('')
                } catch (error) {
                  setSubmitError(error instanceof Error ? error.message : 'Script mutation failed')
                } finally {
                  setSubmitting(false)
                }
              }}
            >
              {submitting ? 'Saving…' : editing ? 'Save script' : 'Add script'}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>
      <TaskRunnerDialog
        open={!!removing}
        onOpenChange={(next) => !next && setRemoving(null)}
		title={`Remove script · ${removing?.slug ?? ''}`}
        description="Removes this script and its deploy or rollback hook from the environment."
        type="destroy"
        target={removing?.id ?? env.id}
        workspace={params.tenant}
        destructive
		confirmText={removing?.slug ?? ''}
        startLabel="Remove script"
        steps={[
          { label: 'Validate the script record', state: 'pending' },
          { label: 'Remove the automatic hook registration', state: 'pending' },
          { label: 'Remove the script', state: 'pending' },
        ]}
        onDispatch={async () => {
          if (!removing) throw new Error('No Script selected for removal')
          return store.removeScript(env.id, removing.id)
        }}
      />
    </>
  )
}
