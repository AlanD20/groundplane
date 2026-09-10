import { useState } from 'react'
import { ArrowUpCircle, RefreshCw } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { useStore } from '@/lib/store'
import { ControllerReleaseDetails, type ControllerRelease } from './controller-release-details'
import { ControllerUpdateProgress } from './controller-update-progress'

export function ControllerUpdateCard() {
  const {
    host, hostLoading, hostError, refreshHost, updateController, resolveControllerUpdate,
    controllerUpdateActive, controllerUpdatePublishing, controllerUpdateError, pendingControllerUpdate,
  } = useStore()
  const [review, setReview] = useState<ControllerRelease | null>(null)
  const update = host?.controller.update
  const candidate = update?.candidate
  const disabled = !update?.available || !candidate || !!hostError || hostLoading || controllerUpdateActive || controllerUpdatePublishing
  const unresolved = pendingControllerUpdate && !pendingControllerUpdate.taskId

  function confirm() {
    if (!review || disabled) return
    const release = review.release
    setReview(null)
    void updateController(release).catch(() => undefined)
  }

  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle>
            <h2 className="flex items-center gap-2"><ArrowUpCircle className="size-4 text-muted-foreground" /> Controller update</h2>
          </CardTitle>
        </CardHeader>
        <CardContent className="flex min-w-0 flex-col gap-4">
          <p className="text-sm text-muted-foreground">
            Activate an immutable release staged on this host. The Controller drains active work, restarts,
            and updates the Agent. Application workloads remain running.
          </p>
          {update?.running_sha256 ? (
            <div className="min-w-0 text-xs">
              <p className="text-muted-foreground">Running Controller binary</p>
              <code className="mt-1 block break-all">{update.running_sha256}</code>
            </div>
          ) : null}
          {!host && hostLoading ? <p role="status" className="text-sm text-muted-foreground">Loading release metadata…</p> : null}
          {hostError ? <p role="status" className="text-sm text-warning">Release metadata may be stale: {hostError}</p> : null}
          {update && !update.available ? (
            <p role="status" className="text-sm text-muted-foreground">
              {update.error || 'Native updates are unavailable. Guarded bootstrap and release staging are required on this host.'}
            </p>
          ) : null}
          {candidate ? <ControllerReleaseDetails release={candidate} /> : update ? (
            <p className="text-sm text-muted-foreground">No staged candidate. Stage a verified release using deployment tooling first.</p>
          ) : null}
          <ControllerUpdateProgress />
          {controllerUpdateError ? <p role="alert" className="text-sm text-destructive">{controllerUpdateError}</p> : null}
          {unresolved ? (
            <div className="flex flex-col gap-2 rounded-lg border border-warning/40 p-3 text-xs">
              <p>Acceptance is unresolved. Resolve the original request before starting another update.</p>
              <code className="break-all">{pendingControllerUpdate.release}</code>
              <Button size="sm" variant="outline" className="self-start" disabled={controllerUpdatePublishing}
                onClick={() => void resolveControllerUpdate().catch(() => undefined)}>
                {controllerUpdatePublishing ? 'Resolving…' : 'Resolve update request'}
              </Button>
            </div>
          ) : null}
          <div className="flex flex-wrap gap-2">
            <Button size="sm" disabled={disabled} onClick={() => { if (candidate) setReview(candidate) }}>
              <ArrowUpCircle className="size-4" /> {controllerUpdatePublishing ? 'Publishing…' : 'Update Controller'}
            </Button>
            <Button size="sm" variant="outline" disabled={hostLoading}
              onClick={() => void refreshHost().catch(() => undefined)}>
              <RefreshCw className="size-4" /> Refresh release
            </Button>
          </div>
        </CardContent>
      </Card>
      <Dialog open={review !== null} onOpenChange={(open) => { if (!open) setReview(null) }}>
        <DialogContent className="max-h-full overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Update native Controller?</DialogTitle>
            <DialogDescription>
              The API briefly disconnects. This Console follows the same Task after restart.
              A failed candidate restores the previous Controller and Agent; the update still fails.
            </DialogDescription>
          </DialogHeader>
          {review ? <ControllerReleaseDetails release={review} /> : null}
          <p className="text-xs text-muted-foreground">
            Up to 120 seconds to drain active work; 600 seconds for the update and recovery.
            Abort is unavailable once activation commits.
          </p>
          <DialogFooter>
            <Button variant="outline" onClick={() => setReview(null)}>Cancel</Button>
            <Button disabled={disabled} onClick={confirm}>Update Controller</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}
