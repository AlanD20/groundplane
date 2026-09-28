import { useRef, useState } from 'react'
import { Download } from 'lucide-react'
import { Link, useSearchParams } from 'react-router-dom'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { CopyButton } from '@/components/common/copy-button'
import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'
import type { operations } from '@/lib/api.generated'
import { controllerRequest } from '@/lib/controller-json-request'
import { newULID } from '@/lib/utils'

type Accepted = operations['image.fetch']['responses'][202]['content']['application/json']

export function ImageFetchCard({ onSettled }: { onSettled?: () => Promise<void> }) {
  const [search] = useSearchParams()
  const [open, setOpen] = useState(false)
  const [image, setImage] = useState(search.get('fetch') ?? '')
  const [accepted, setAccepted] = useState<Accepted | null>(null)
  const [completed, setCompleted] = useState(false)
  const intent = useRef<{ image: string; key: string; accepted: Accepted | null } | null>(null)

  function openFetch() {
    // A known acceptance can be followed in Activity. A new explicit fetch may
    // select a moved tag; unresolved acceptance must keep its original key.
    if (intent.current?.accepted) intent.current = null
    setOpen(true)
  }

  async function dispatch() {
    const requested = image.trim()
    if (!intent.current || intent.current.image !== requested) {
      intent.current = { image: requested, key: newULID(), accepted: null }
    }
    const current = intent.current
    if (current.accepted) return current.accepted.task_id
    setCompleted(false)
    setAccepted(null)
    const response = await controllerRequest<Accepted>('/images/fetch', 202, {
      method: 'POST', body: { image: current.image }, idempotencyKey: current.key,
    })
    current.accepted = response
    if (intent.current === current) setAccepted(response)
    return response.task_id
  }

  return (
    <>
      <Card id="images">
        <CardHeader><CardTitle><h2 className="flex items-center gap-2"><Download className="size-4" /> Host images</h2></CardTitle></CardHeader>
        <CardContent className="flex min-w-0 flex-col gap-3">
          <p className="text-sm text-muted-foreground">
            Fetch from Docker Hub, public GHCR or another public registry, or GP's private registry. External private-registry credentials are not supported. This does not edit a Service or deploy a Release.
          </p>
          <Button className="self-start" variant="outline" onClick={openFetch}><Download className="size-4" /> Fetch image</Button>
          {accepted ? (
            <div className="flex min-w-0 flex-col gap-2 rounded-lg border border-border p-3 text-xs">
              <p role="status">{completed ? 'Fetch completed. Use this immutable reference in Deploy.' : 'Fetch accepted. Wait for the Task to complete before deploying.'}</p>
              <div className="flex min-w-0 items-start gap-2">
                <code className="min-w-0 flex-1 break-all">{accepted.image}</code>
                <CopyButton value={accepted.image} label="Copy image" />
              </div>
              <code className="break-all">Task: {accepted.task_id}</code>
              <Link to="/platform/activity" className="text-primary hover:underline">Open Activity to inspect, retry or abort the Task</Link>
            </div>
          ) : null}
        </CardContent>
      </Card>
      <TaskRunnerDialog
        open={open} onOpenChange={setOpen} variant="drawer" title="Fetch image" type="fetch"
        target={image.trim() || 'Registry image'} workspace="platform" startLabel="Fetch image"
        startDisabled={!image.trim()} onDispatch={dispatch} onCommit={() => setCompleted(true)}
        onSettled={onSettled}
        description="Select an explicit tag or digest. GP pins its content before acceptance; retries keep that selection."
        executionCopy="The Controller fetches and verifies the selected content; your running Services are unchanged."
        steps={[]}
        review={<div className="space-y-2">
          <Label htmlFor="fetch-image">Registry image</Label>
          <Input id="fetch-image" value={image} onChange={event => setImage(event.target.value)}
            placeholder="nginx:latest, ghcr.io/org/app:tag or registry.groundplane.internal:5000/project/api:release" />
          <p className="text-xs text-muted-foreground">After an uncertain request, keep the same image and retry to resolve its original acceptance.</p>
        </div>}
      />
    </>
  )
}
