'use client'

import { useState } from 'react'
import { Trash2 } from 'lucide-react'
import { EmptyState } from '@/components/common/empty-state'
import { RevealValue } from '@/components/common/reveal-value'
import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import type { ReusableSecret } from '@/lib/types'

export function ReusableSecretOwnerList({
  secrets,
  loading,
  emptyTitle,
  emptyDescription,
  scopeLabel,
  onRemove,
  onRefresh,
  onReveal,
}: {
  secrets: ReusableSecret[]
  loading: boolean
  emptyTitle: string
  emptyDescription?: string
  scopeLabel: string
  onRemove?: (secret: ReusableSecret) => Promise<{ task_id: string }>
  onRefresh?: () => Promise<void>
  onReveal: (secret: ReusableSecret) => Promise<string>
}) {
  const [confirmation, setConfirmation] = useState<ReusableSecret | null>(null)

  return (
    <>
      {loading && <p role="status" className="border-t border-border py-3 text-xs text-muted-foreground">Loading Secrets…</p>}
      {!loading && secrets.length === 0 && (
        <EmptyState title={emptyTitle} description={emptyDescription} className="rounded-none border-x-0 border-b-0" />
      )}
      {secrets.length > 0 && (
        <div className="flex flex-col" role="list">
          {secrets.map((secret) => (
            <div key={secret.id} className="flex items-center justify-between gap-3 border-t border-border py-2.5 text-sm" role="listitem">
              <div className="flex min-w-0 items-center gap-2">
                <span className="truncate font-mono">{secret.key}</span>
                {secret.kind === 'file' && <Badge variant="muted">file</Badge>}
              </div>
              <div className="hidden min-w-0 flex-1 flex-col px-3 lg:flex">
                <span className="truncate font-mono text-xs text-muted-foreground">{secret.ref}</span>
              </div>
              <span className="hidden text-xs text-muted-foreground md:block">{scopeLabel}</span>
              <div className="flex shrink-0 items-center gap-2">
                <RevealValue loadValue={() => onReveal(secret)} label={secret.key} />
                {onRemove && (
                  <Button
                    variant="ghost"
                    size="icon-sm"
                    className="text-muted-foreground hover:text-destructive"
                    onClick={() => setConfirmation(secret)}
                    aria-label={'Delete ' + secret.key}
                  >
                    <Trash2 className="size-3.5" />
                  </Button>
                )}
              </div>
            </div>
          ))}
        </div>
      )}

      {confirmation && onRemove && (
        <TaskRunnerDialog
          open
          onOpenChange={(open) => { if (!open) setConfirmation(null) }}
          title={'Delete ' + confirmation.key + '?'}
          description="The Controller publishes an authoritative removal Task. This Secret remains visible until that Task completes and the Controller list refreshes."
          type="remove"
          target={confirmation.id}
          workspace={confirmation.scope === 'platform' ? 'Platform' : 'Project'}
          steps={[]}
          startLabel="Delete Secret"
          destructive
          onDispatch={async () => (await onRemove(confirmation)).task_id}
          onCommit={() => { void onRefresh?.().catch(() => undefined) }}
        />
      )}
    </>
  )
}
