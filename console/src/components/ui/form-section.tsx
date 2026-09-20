import type { ReactNode } from 'react'
import { cn } from '@/lib/utils'

export function FormSection({
  title,
  description,
  children,
  className,
}: {
  title: string
  description?: string
  children: ReactNode
  className?: string
}) {
  return (
    <fieldset className={cn('min-w-0 space-y-4 rounded-xl border border-border p-4', className)}>
      <legend className="px-2 text-sm font-semibold">{title}</legend>
      {description && <p className="text-xs text-muted-foreground">{description}</p>}
      <div className="grid min-w-0 gap-4 sm:grid-cols-2">{children}</div>
    </fieldset>
  )
}
