import { cn } from '@/lib/utils'
import type { HealthState } from '@/lib/types'

const MAP: Record<string, { label: string; dot: string; text: string }> = {
  healthy: { label: 'Healthy', dot: 'bg-success', text: 'text-success' },
  online: { label: 'Online', dot: 'bg-success', text: 'text-success' },
  completed: { label: 'Completed', dot: 'bg-success', text: 'text-success' },
  running: { label: 'Running', dot: 'bg-info animate-pulse', text: 'text-info' },
  degraded: { label: 'Degraded', dot: 'bg-warning', text: 'text-warning' },
  pending: { label: 'Pending', dot: 'bg-warning', text: 'text-warning' },
  failed: { label: 'Failed', dot: 'bg-destructive', text: 'text-destructive' },
  timed_out: { label: 'Timed out', dot: 'bg-destructive', text: 'text-destructive' },
  stopped: { label: 'Stopped', dot: 'bg-muted-foreground', text: 'text-muted-foreground' },
  offline: { label: 'Offline', dot: 'bg-muted-foreground', text: 'text-muted-foreground' },
}

export function StatusDot({ status, className }: { status: HealthState | string; className?: string }) {
  const m = MAP[status] ?? MAP.stopped
  return <span className={cn('inline-block size-2 shrink-0 rounded-full', m.dot, className)} aria-hidden />
}

export function StatusBadge({
  status,
  label,
  className,
}: {
  status: HealthState | string
  label?: string
  className?: string
}) {
  const m = MAP[status] ?? MAP.stopped
  return (
    <span
      className={cn(
        'inline-flex items-center gap-1.5 rounded-md border border-border bg-surface px-1.5 py-0.5 text-xs font-medium',
        m.text,
        className,
      )}
    >
      <span className={cn('size-1.5 rounded-full', m.dot)} aria-hidden />
      {label ?? m.label}
    </span>
  )
}
