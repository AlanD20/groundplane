import { cn } from '@/lib/utils'

export function EmptyState({
  icon,
  title,
  description,
  action,
  className,
}: {
  icon?: React.ReactNode
  title: string
  description?: string
  action?: React.ReactNode
  className?: string
}) {
  return (
    <div
      className={cn(
        'flex flex-col items-center justify-center gap-3 rounded-xl border border-dashed border-border bg-surface/40 px-6 py-12 text-center',
        className,
      )}
    >
      {icon && (
        <div className="flex size-10 items-center justify-center rounded-lg border border-border bg-surface text-muted-foreground [&_svg]:size-5">
          {icon}
        </div>
      )}
      <div className="flex flex-col gap-1">
        <p className="text-sm font-medium">{title}</p>
        {description && <p className="max-w-sm text-sm text-muted-foreground">{description}</p>}
      </div>
      {action}
    </div>
  )
}
