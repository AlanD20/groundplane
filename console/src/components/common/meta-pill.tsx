import { cn } from '@/lib/utils'

// Compact icon + text pill for PageHeader meta rows and stat lines.
export function MetaPill({
  icon,
  children,
  tone = 'default',
  className,
}: {
  icon?: React.ReactNode
  children: React.ReactNode
  tone?: 'default' | 'success' | 'warning' | 'danger'
  className?: string
}) {
  return (
    <span
      className={cn(
        'inline-flex items-center gap-1.5 whitespace-nowrap rounded-full border px-2.5 py-0.5 text-xs font-medium',
        '[&_svg]:size-3.5 [&_svg]:shrink-0',
        tone === 'success'
          ? 'border-success/25 bg-success/10 text-success'
          : tone === 'warning'
            ? 'border-warning/25 bg-warning/10 text-warning'
            : tone === 'danger'
              ? 'border-destructive/25 bg-destructive/10 text-destructive'
              : 'border-border bg-surface text-muted-foreground',
        className,
      )}
    >
      {icon}
      <span>{children}</span>
    </span>
  )
}
