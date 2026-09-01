'use client'

import { useEffect, useRef, useState } from 'react'
import { ScrollText } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { useStore } from '@/lib/store'
import type { LogTarget, TransientLogEvent } from '@/lib/transient-logs'

export function LogViewer({ target, label = 'Logs' }: { target: LogTarget; label?: string }) {
  const store = useStore()
  const [open, setOpen] = useState(false)
  const [tail, setTail] = useState(200)
  const [follow, setFollow] = useState(false)
  const [events, setEvents] = useState<TransientLogEvent[]>([])
  const [error, setError] = useState<string | null>(null)
  const [streaming, setStreaming] = useState(false)
  const abortRef = useRef<AbortController | null>(null)

  const stop = () => {
    abortRef.current?.abort()
    abortRef.current = null
    setStreaming(false)
  }

  const start = () => {
    stop()
    const controller = new AbortController()
    abortRef.current = controller
    setEvents([])
    setError(null)
    setStreaming(true)
    void store.watchLogs(target, { tail, follow, signal: controller.signal }, (event) => {
      setEvents((current) => [...current.slice(-999), event])
    }).catch((reason: unknown) => {
      if (!controller.signal.aborted) {
        setError(reason instanceof Error ? reason.message : 'Log stream failed')
      }
    }).finally(() => {
      if (abortRef.current === controller) {
        abortRef.current = null
        setStreaming(false)
      }
    })
  }

  useEffect(() => stop, [])

  return (
    <>
      <Button variant="outline" onClick={() => setOpen(true)}>
        <ScrollText className="size-4" /> {label}
      </Button>
      <Dialog open={open} onOpenChange={(next) => { setOpen(next); if (!next) stop() }}>
        <DialogContent className="max-w-5xl">
          <DialogHeader>
            <DialogTitle>{label}</DialogTitle>
            <DialogDescription>Transient workload output. Reconnects create a new fixed-source stream.</DialogDescription>
          </DialogHeader>
          <div className="flex flex-wrap items-end gap-3 border-y border-border py-3">
            <label className="grid gap-1 text-xs font-medium">
              Tail per container
              <Input className="w-32" type="number" min={0} max={1000} value={tail} onChange={(event) => setTail(Number(event.target.value))} />
            </label>
            <label className="flex h-9 items-center gap-2 text-sm">
              <input type="checkbox" checked={follow} onChange={(event) => setFollow(event.target.checked)} /> Follow
            </label>
            <Button onClick={start} disabled={streaming || !Number.isSafeInteger(tail) || tail < 0 || tail > 1000}>
              {streaming ? 'Streaming' : 'Open stream'}
            </Button>
            {streaming ? <Button variant="outline" onClick={stop}>Stop</Button> : null}
          </div>
          {error ? <p role="alert" className="text-sm text-destructive">{error}</p> : null}
          <div className="h-[55vh] overflow-auto rounded-lg bg-[#101510] p-4 font-mono text-xs text-[#d8e3c9]">
            {events.length === 0 ? <p className="text-[#83907a]">No log lines received.</p> : events.map((event) => (
              <div key={event.sequence} className="grid grid-cols-[9rem_8rem_5rem_1fr] gap-3 border-b border-white/5 py-1">
                <span className="text-[#849a7c]">{new Date(event.timestamp).toLocaleTimeString()}</span>
                <span className="truncate text-[#b8cc87]">{event.service_name}</span>
                <span className={event.stream === 'stderr' ? 'text-[#ff9d7a]' : 'text-[#8bc6b6]'}>{event.stream}</span>
                <span className="whitespace-pre-wrap break-all">{event.line}{event.truncated ? ' [truncated]' : ''}</span>
              </div>
            ))}
          </div>
        </DialogContent>
      </Dialog>
    </>
  )
}
