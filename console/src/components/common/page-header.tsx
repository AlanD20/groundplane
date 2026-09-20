import { cn } from '@/lib/utils'

export function PageHeader({
  title,
  description,
  icon,
  actions,
  meta,
  eyebrow,
  className,
}: {
  title: React.ReactNode
  description?: React.ReactNode
  icon?: React.ReactNode
  actions?: React.ReactNode
  meta?: React.ReactNode
  eyebrow?: React.ReactNode
  className?: string
}) {
  return (
    <div className={cn('flex flex-col gap-3 pb-2', className)}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="flex items-start gap-3">
          {icon && (
            <div className="flex size-9 shrink-0 items-center justify-center rounded-lg border border-border bg-surface text-primary [&_svg]:size-4.5">
              {icon}
            </div>
          )}
          <div className="flex flex-col gap-1">
            {eyebrow && <span className="text-[11px] font-semibold uppercase tracking-wider text-primary">{eyebrow}</span>}
            <h1 className="text-pretty text-[30px] font-medium tracking-tight leading-tight">{title}</h1>
            {description && <p className="max-w-2xl text-sm text-muted-foreground">{description}</p>}
          </div>
        </div>
        {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
      </div>
      {meta && <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">{meta}</div>}
    </div>
  )
}
