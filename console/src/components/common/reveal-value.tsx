'use client'

import { useState } from 'react'
import { Eye, EyeOff } from 'lucide-react'
import { CopyButton } from './copy-button'
import { useStore } from '@/lib/store'
import { cn } from '@/lib/utils'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'

// Masked value with an optional typed-confirmation reveal (never cached,
// never logged). The typed confirmation is a PLATFORM-wide preference — off by
// default (Settings → Preferences → "Require typed confirmation to reveal
// secrets"), so revealing is one click unless the operator opted in.
export function RevealValue({
  loadValue,
  label,
  className,
  confirmWord = 'reveal',
  sensitive = true,
}: {
  loadValue: () => Promise<string>
  label?: string
  className?: string
  confirmWord?: string
  sensitive?: boolean
}) {
  const { requireRevealConfirm } = useStore()
  const [open, setOpen] = useState(false)
  const [revealed, setRevealed] = useState(false)
  const [loadedValue, setLoadedValue] = useState<string>()
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [typed, setTyped] = useState('')

  const displayedValue = loadedValue

  async function reveal(): Promise<boolean> {
    setError(null)
    setLoading(true)
    try {
      setLoadedValue(await loadValue())
      setRevealed(true)
      return true
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Unable to reveal value')
      return false
    } finally {
      setLoading(false)
    }
  }

  function hide() {
    setRevealed(false)
    setLoadedValue(undefined)
    setError(null)
  }

  return (
    <div className={cn('flex items-center gap-1', className)}>
      <code className="truncate rounded-md bg-muted px-2 py-1 font-mono text-xs">
        {revealed ? displayedValue : sensitive ? '•'.repeat(12) : 'not loaded'}
      </code>
      {revealed ? (
        <>
          <CopyButton value={displayedValue ?? ''} />
          <button
            type="button"
            onClick={hide}
            className="rounded-md p-1.5 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
            aria-label="Hide value"
          >
            <EyeOff className="size-3.5" />
          </button>
        </>
      ) : (
        <button
          type="button"
          onClick={() => {
            if (sensitive && requireRevealConfirm) {
              setTyped('')
              setOpen(true)
            } else {
              void reveal()
            }
          }}
          className="rounded-md p-1.5 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
          aria-label={sensitive ? 'Reveal value' : 'Show value'}
          disabled={loading}
        >
          <Eye className="size-3.5" />
        </button>
      )}
      {error && <span className="text-xs text-destructive">{error}</span>}

      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Reveal {label ?? 'secret'}</DialogTitle>
            <DialogDescription>
              This value is sensitive and never cached or logged. Type{' '}
              <span className="font-mono text-foreground">{confirmWord}</span> to reveal it.
            </DialogDescription>
          </DialogHeader>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="reveal-confirm">Confirmation</Label>
            <Input
              id="reveal-confirm"
              autoFocus
              value={typed}
              onChange={(e) => setTyped(e.target.value)}
              placeholder={confirmWord}
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button
              disabled={typed !== confirmWord || loading}
              onClick={() => {
                void reveal().then((success) => {
                  if (success) setOpen(false)
                })
              }}
            >
              {loading ? 'Revealing…' : 'Reveal value'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
